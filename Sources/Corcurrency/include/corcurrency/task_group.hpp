// C++ Corcurrency — structured concurrency: task groups and `async let`.
//
//     int sum = co_await withTaskGroup<int>([](TaskGroup<int>& group) -> async<int> {
//         for (int i = 0; i < 10; ++i)
//             group.addTask([i]() -> async<int> { co_return i * i; });
//         int total = 0;
//         for_await (int value, group) total += value;
//         co_return total;
//     });
//
//     async_let user = fetchUser(id);         // async let user = fetchUser(id)
//     async_let image = fetchImage(id);
//     auto profile = Profile(co_await user, co_await image);
#pragma once

#include <corcurrency/task.hpp>

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

namespace corcurrency {

namespace detail {

inline Executor* childExecutor(TaskBase* parent) {
    return parent && parent->executorPreference ? parent->executorPreference : &globalConcurrentExecutor();
}

inline TaskSpawnOptions childOptions(TaskBase* parent, std::optional<TaskPriority> priority) {
    TaskSpawnOptions o;
    if (parent) {
        o.priority = parent->priority;
        o.locals = parent->locals;
    }
    if (priority) o.priority = *priority;
    o.executor = childExecutor(parent);
    return o;
}

/// Shared state of a task group. `T` is the child result type.
///
/// Like `dispatch_group_t`, the counters live in one atomic word, so a child
/// finishing and the parent suspending in `next()` agree without a lock and
/// no wake-up is lost (the parent sets `waiting` only if nothing is ready,
/// in the same compare-and-swap that reads the counts):
///
///     63          32 31          2      1         0
///     ┌─────────────┬─────────────┬───────────┬─────────┐
///     │   pending   │    ready    │ cancelled │ waiting │
///     └─────────────┴─────────────┴───────────┴─────────┘
///
/// Results go on a lock-free stack that only the parent pops. The running
/// children (needed by `cancelAll`) form an intrusive list under a mutex.
/// Everything is O(1) per child except `cancelAll`, which is O(running).
template <class T>
class GroupState {
public:
    GroupState(TaskBase* parent, bool discardResults, bool throwing)
        : parent_(parent), discardResults_(discardResults), throwing_(throwing) {}

    ~GroupState() {
        freeNodes(stack_.load(std::memory_order_acquire));
        freeNodes(local_);
    }

    TaskBase* parent() const noexcept { return parent_; }

    /// Parent cancellation reaches the children through a single cancellation
    /// handler on the parent, instead of registering every child with it.
    static void attachToParent(const std::shared_ptr<GroupState>& self) {
        if (!self->parent_) return;
        self->handlerId_ = TaskBase::makeHandlerId();
        std::weak_ptr<GroupState> weak = self;
        if (!self->parent_->addCancellationHandler(self->handlerId_, [weak] {
                if (auto state = weak.lock()) state->cancelAll();
            }))
            self->cancelAll();
    }

    void detachFromParent() {
        if (parent_) parent_->removeCancellationHandler(handlerId_);
    }

    bool isCancelled() {
        return (status_.load(std::memory_order_acquire) & cancelledBit) || (parent_ && parent_->isCancelled());
    }

    bool isEmpty() {
        auto s = status_.load(std::memory_order_acquire);
        return pending(s) == 0 && ready(s) == 0;
    }

    /// Registers a new child (returns false if `onlyUnlessCancelled` and the group is cancelled).
    bool beginChild(bool onlyUnlessCancelled) {
        if (!onlyUnlessCancelled) {
            status_.fetch_add(pendingOne, std::memory_order_relaxed);
            return true;
        }
        if (parent_ && parent_->isCancelled()) return false;
        auto s = status_.load(std::memory_order_relaxed);
        do {
            if (s & cancelledBit) return false;
        } while (!status_.compare_exchange_weak(s, s + pendingOne, std::memory_order_relaxed));
        return true;
    }

    /// Called before the child is started, so it cannot finish unregistered.
    void attachChild(TaskBase* child) {
        child->retain();
        {
            std::lock_guard lock(mutex_);
            child->groupPrev = nullptr;
            child->groupNext = head_;
            if (head_) head_->groupPrev = child;
            head_ = child;
        }
        if (status_.load(std::memory_order_acquire) & cancelledBit) child->cancel();
    }

    void childFinished(TaskBase* child, Result<T> result) {
        detachChild(child);
        if (!discardResults_) {
            push(new Node{nullptr, std::move(result)});
        } else if (!result.isSuccess() && throwing_ && recordFirstError(result.error())) {
            cancelAll();
        }
        auto s = status_.load(std::memory_order_relaxed);
        std::uint64_t n;
        do {
            n = s - pendingOne + (discardResults_ ? 0 : readyOne);
            if ((n & waitingBit) && canReturnNext(n)) n &= ~waitingBit;
        } while (!status_.compare_exchange_weak(s, n, std::memory_order_acq_rel, std::memory_order_relaxed));
        // We cleared `waiting`: the suspended parent is ours to resume.
        if ((s & waitingBit) && !(n & waitingBit)) std::exchange(waiter_, {}).resume();
    }

    void cancelAll() {
        status_.fetch_or(cancelledBit, std::memory_order_acq_rel);
        std::vector<TaskRef> running;
        {
            std::lock_guard lock(mutex_);
            for (auto* child = head_; child; child = child->groupNext) running.emplace_back(child);
        }
        for (auto& c : running) c->cancel();
    }

    /// Awaiter for the next completed child (`nullopt` when the group is empty).
    /// Only the group's parent awaits it.
    struct NextAwaiter {
        std::shared_ptr<GroupState> state;
        bool await_ready() const noexcept {
            return canReturnNext(state->status_.load(std::memory_order_acquire));
        }
        bool await_suspend(std::coroutine_handle<> h) {
            auto& st = *state;
            st.waiter_ = Waiter::current(h);
            auto s = st.status_.load(std::memory_order_acquire);
            do {
                if (canReturnNext(s)) {
                    st.waiter_ = {};
                    return false;
                }
            } while (!st.status_.compare_exchange_weak(s, s | waitingBit, std::memory_order_acq_rel,
                                                       std::memory_order_acquire));
            // A child may already be resuming us on another thread: don't touch `this`.
            return true;
        }
        std::optional<Result<T>> await_resume() {
            if (ready(state->status_.load(std::memory_order_acquire)) == 0) return std::nullopt;
            return state->pop();
        }
    };

    std::exception_ptr firstError() {
        std::lock_guard lock(mutex_);
        return firstError_;
    }

private:
    static constexpr std::uint64_t waitingBit = 1;
    static constexpr std::uint64_t cancelledBit = 2;
    static constexpr std::uint64_t readyOne = 4;
    static constexpr std::uint64_t readyMask = 0xFFFF'FFFC;
    static constexpr std::uint64_t pendingOne = std::uint64_t(1) << 32;

    static std::uint64_t pending(std::uint64_t s) noexcept { return s >> 32; }
    static std::uint64_t ready(std::uint64_t s) noexcept { return (s & readyMask) >> 2; }
    /// `next()` can return without suspending.
    static bool canReturnNext(std::uint64_t s) noexcept { return ready(s) != 0 || pending(s) == 0; }

    struct Node {
        Node* next;
        Result<T> result;
    };

    static void freeNodes(Node* node) {
        while (node) delete std::exchange(node, node->next);
    }

    void push(Node* node) {
        node->next = stack_.load(std::memory_order_relaxed);
        while (!stack_.compare_exchange_weak(node->next, node, std::memory_order_release,
                                             std::memory_order_relaxed)) {
        }
    }

    /// Parent only, when `ready > 0`: the oldest finished result.
    Result<T> pop() {
        if (!local_) {
            // Take everything pushed so far and reverse it into completion order.
            for (auto* node = stack_.exchange(nullptr, std::memory_order_acquire); node;) {
                auto* next = node->next;
                node->next = local_;
                local_ = node;
                node = next;
            }
        }
        auto* node = std::exchange(local_, local_->next);
        Result<T> result = std::move(node->result);
        delete node;
        status_.fetch_sub(readyOne, std::memory_order_relaxed);
        return result;
    }

    void detachChild(TaskBase* child) {
        {
            std::lock_guard lock(mutex_);
            if (child->groupPrev) child->groupPrev->groupNext = child->groupNext;
            else head_ = child->groupNext;
            if (child->groupNext) child->groupNext->groupPrev = child->groupPrev;
            child->groupPrev = child->groupNext = nullptr;
        }
        child->release();
    }

    bool recordFirstError(std::exception_ptr e) {
        std::lock_guard lock(mutex_);
        if (firstError_) return false;
        firstError_ = std::move(e);
        return true;
    }

    TaskBase* parent_;
    bool discardResults_;
    bool throwing_;
    std::uint64_t handlerId_ = 0;
    std::atomic<std::uint64_t> status_{0};
    std::atomic<Node*> stack_{nullptr}; // pushed by children, newest first
    Node* local_ = nullptr;             // taken by the parent, oldest first
    Waiter waiter_{};                   // valid while `waiting` is set
    std::mutex mutex_;                  // guards the list below and `firstError_`
    TaskBase* head_ = nullptr;          // running children (retained)
    std::exception_ptr firstError_;
};

template <class T, class F>
async<void> runGroupChild(std::shared_ptr<GroupState<T>> group, F operation) {
    auto* self = currentContext().task;
    try {
        if constexpr (std::is_void_v<T>) {
            co_await invokeAsync(std::move(operation));
            group->childFinished(self, Result<T>::success());
        } else {
            group->childFinished(self, Result<T>::success(co_await invokeAsync(std::move(operation))));
        }
    } catch (...) {
        group->childFinished(self, Result<T>::failure(std::current_exception()));
    }
}

template <class T>
async<std::optional<T>> nextValue(std::shared_ptr<GroupState<T>> state) {
    auto r = co_await typename GroupState<T>::NextAwaiter{state};
    if (!r) co_return std::nullopt;
    if constexpr (std::is_void_v<T>) {
        r->get();
        co_return std::optional<T>{};
    } else {
        co_return std::move(*r).get();
    }
}

} // namespace detail

/// Swift's `TaskGroup<ChildTaskResult>` / `ThrowingTaskGroup<ChildTaskResult, Error>`.
/// Only usable inside `withTaskGroup` / `withThrowingTaskGroup`.
/// A child's exception is rethrown by `next()` (Swift's throwing group semantics).
template <class ChildTaskResult>
class TaskGroup {
    static_assert(!std::is_void_v<ChildTaskResult>, "use DiscardingTaskGroup for Void children");

public:
    using Element = ChildTaskResult;

    TaskGroup(const TaskGroup&) = delete;
    TaskGroup& operator=(const TaskGroup&) = delete;

    /// `group.addTask { ... }` — runs concurrently as a child task.
    template <detail::TaskOperation F>
    void addTask(F&& operation) {
        spawn(std::nullopt, std::forward<F>(operation), false);
    }
    /// `group.addTask(priority: p) { ... }`
    template <detail::TaskOperation F>
    void addTask(TaskPriority priority, F&& operation) {
        spawn(priority, std::forward<F>(operation), false);
    }
    /// `group.addTaskUnlessCancelled { ... }`
    template <detail::TaskOperation F>
    bool addTaskUnlessCancelled(F&& operation) {
        return spawn(std::nullopt, std::forward<F>(operation), true);
    }
    template <detail::TaskOperation F>
    bool addTaskUnlessCancelled(TaskPriority priority, F&& operation) {
        return spawn(priority, std::forward<F>(operation), true);
    }

    /// `await group.next()` — the next child result in completion order, or
    /// `nullopt` once all children finished. Rethrows a child's error.
    async<std::optional<ChildTaskResult>> next() { return detail::nextValue(state_); }

    /// `await group.nextResult()`
    auto nextResult() -> async<std::optional<Result<ChildTaskResult>>> {
        co_return co_await typename detail::GroupState<ChildTaskResult>::NextAwaiter{state_};
    }

    /// `try await group.waitForAll()` — rethrows the first child error.
    auto waitForAll() -> async<void> {
        auto state = state_;
        while (auto r = co_await typename detail::GroupState<ChildTaskResult>::NextAwaiter{state}) r->get();
    }

    void cancelAll() { state_->cancelAll(); }
    bool isCancelled() { return state_->isCancelled(); }
    bool isEmpty() { return state_->isEmpty(); }

    /// `for await value in group` (see `for_await`).
    class AsyncIterator {
    public:
        explicit AsyncIterator(std::shared_ptr<detail::GroupState<ChildTaskResult>> s) : state_(std::move(s)) {}
        async<std::optional<ChildTaskResult>> next() { return detail::nextValue(state_); }

    private:
        std::shared_ptr<detail::GroupState<ChildTaskResult>> state_;
    };
    AsyncIterator makeAsyncIterator() { return AsyncIterator(state_); }

    // Internal: created by `withTaskGroup` / `withThrowingTaskGroup`.
    explicit TaskGroup(detail::TaskBase* parent)
        : state_(std::make_shared<detail::GroupState<ChildTaskResult>>(parent, false, true)) {
        detail::GroupState<ChildTaskResult>::attachToParent(state_);
    }
    ~TaskGroup() { state_->detachFromParent(); }

    // Internal: awaits (and discards) all remaining children.
    auto drain() -> async<void> {
        auto state = state_;
        while (co_await typename detail::GroupState<ChildTaskResult>::NextAwaiter{state}) {
        }
    }

private:
    template <class F>
    bool spawn(std::optional<TaskPriority> priority, F&& operation, bool unlessCancelled) {
        if (!state_->beginChild(unlessCancelled)) return false;
        detail::spawnTask<void>(
            [state = state_, op = std::decay_t<F>(std::forward<F>(operation))]() mutable {
                return detail::runGroupChild<ChildTaskResult>(state, std::move(op));
            },
            detail::childOptions(state_->parent(), priority),
            [this](const detail::TaskRef& child) { state_->attachChild(child.get()); });
        return true;
    }

    std::shared_ptr<detail::GroupState<ChildTaskResult>> state_;
};

/// Swift's `ThrowingTaskGroup` — the same type here, since C++ errors are exceptions.
template <class ChildTaskResult>
using ThrowingTaskGroup = TaskGroup<ChildTaskResult>;

/// Swift's `DiscardingTaskGroup` / `ThrowingDiscardingTaskGroup`: children's
/// results are dropped as soon as they finish. In the throwing flavour the first
/// child error cancels the group and is rethrown from `with...DiscardingTaskGroup`.
class DiscardingTaskGroup {
public:
    DiscardingTaskGroup(const DiscardingTaskGroup&) = delete;
    DiscardingTaskGroup& operator=(const DiscardingTaskGroup&) = delete;

    template <detail::TaskOperation F>
    void addTask(F&& operation) {
        spawn(std::nullopt, std::forward<F>(operation), false);
    }
    template <detail::TaskOperation F>
    void addTask(TaskPriority priority, F&& operation) {
        spawn(priority, std::forward<F>(operation), false);
    }
    template <detail::TaskOperation F>
    bool addTaskUnlessCancelled(F&& operation) {
        return spawn(std::nullopt, std::forward<F>(operation), true);
    }

    void cancelAll() { state_->cancelAll(); }
    bool isCancelled() { return state_->isCancelled(); }
    bool isEmpty() { return state_->isEmpty(); }

    // Internal: created by `with[Throwing]DiscardingTaskGroup`.
    DiscardingTaskGroup(detail::TaskBase* parent, bool throwing)
        : state_(std::make_shared<detail::GroupState<void>>(parent, true, throwing)) {
        detail::GroupState<void>::attachToParent(state_);
    }
    ~DiscardingTaskGroup() { state_->detachFromParent(); }

    // Internal.
    auto drain() -> async<void> {
        auto state = state_;
        while (co_await detail::GroupState<void>::NextAwaiter{state}) {
        }
    }
    std::exception_ptr firstError() { return state_->firstError(); }

private:
    template <class F>
    bool spawn(std::optional<TaskPriority> priority, F&& operation, bool unlessCancelled) {
        if (!state_->beginChild(unlessCancelled)) return false;
        detail::spawnTask<void>(
            [state = state_, op = std::decay_t<F>(std::forward<F>(operation))]() mutable {
                return detail::runGroupChild<void>(state, std::move(op));
            },
            detail::childOptions(state_->parent(), priority),
            [this](const detail::TaskRef& child) { state_->attachChild(child.get()); });
        return true;
    }

    std::shared_ptr<detail::GroupState<void>> state_;
};

namespace detail {

/// Runs `body(group)`; then (cancelling first if it threw) awaits every child.
template <class Group, class Body>
auto runGroupBody(Group& group, Body& body) -> async<async_result_t<Body&, Group&>> {
    using R = async_result_t<Body&, Group&>;
    std::exception_ptr error;
    std::optional<std::conditional_t<std::is_void_v<R>, std::monostate, R>> result;
    try {
        if constexpr (std::is_void_v<R>) {
            co_await callAsync(body, group);
            result.emplace();
        } else {
            result.emplace(co_await callAsync(body, group));
        }
    } catch (...) {
        error = std::current_exception();
    }
    if (error) group.cancelAll();
    co_await group.drain();
    if (error) std::rethrow_exception(error);
    if constexpr (!std::is_void_v<R>) co_return std::move(*result);
}

} // namespace detail

/// `await withTaskGroup(of: T.self) { group in ... }`
template <class ChildTaskResult, class Body>
auto withTaskGroup(Body body) -> async<detail::async_result_t<Body&, TaskGroup<ChildTaskResult>&>> {
    TaskGroup<ChildTaskResult> group(detail::currentContext().task);
    co_return co_await detail::runGroupBody(group, body);
}

/// `try await withThrowingTaskGroup(of: T.self) { group in ... }`
template <class ChildTaskResult, class Body>
auto withThrowingTaskGroup(Body body) -> async<detail::async_result_t<Body&, TaskGroup<ChildTaskResult>&>> {
    TaskGroup<ChildTaskResult> group(detail::currentContext().task);
    co_return co_await detail::runGroupBody(group, body);
}

/// `await withDiscardingTaskGroup { group in ... }`
template <class Body>
auto withDiscardingTaskGroup(Body body) -> async<detail::async_result_t<Body&, DiscardingTaskGroup&>> {
    DiscardingTaskGroup group(detail::currentContext().task, false);
    co_return co_await detail::runGroupBody(group, body);
}

/// `try await withThrowingDiscardingTaskGroup { group in ... }`
template <class Body>
auto withThrowingDiscardingTaskGroup(Body body)
    -> async<detail::async_result_t<Body&, DiscardingTaskGroup&>> {
    DiscardingTaskGroup group(detail::currentContext().task, true);
    using R = detail::async_result_t<Body&, DiscardingTaskGroup&>;
    if constexpr (std::is_void_v<R>) {
        co_await detail::runGroupBody(group, body);
        if (auto e = group.firstError()) std::rethrow_exception(e);
    } else {
        auto r = co_await detail::runGroupBody(group, body);
        if (auto e = group.firstError()) std::rethrow_exception(e);
        co_return r;
    }
}

// MARK: - async let

/// Swift's `async let`: starts `operation` immediately as a child task.
/// Await it (possibly several times) with `co_await`. If it goes out of scope
/// without being awaited it is cancelled (Swift also implicitly awaits; C++
/// destructors cannot suspend, so always await an `async_let` you start).
template <class T>
class async_let {
public:
    /// `async_let x = someAsyncFunction(args);`
    async_let(async<T>&& operation) // NOLINT(google-explicit-constructor)
        : async_let(0, [op = std::move(operation)]() mutable -> async<T> { return std::move(op); }) {}

    /// `async_let x = [&] { ... };` — from a closure (sync or async).
    template <detail::TaskOperation F>
        requires(!detail::is_async_v<F>)
    async_let(F&& operation) // NOLINT(google-explicit-constructor)
        : async_let(0, std::forward<F>(operation)) {}

    async_let(const async_let&) = delete;
    async_let& operator=(const async_let&) = delete;

    ~async_let() {
        if (!awaited_) child_->cancel();
        if (parent_) parent_->removeChild(child_.get());
    }

    /// `await x`
    detail::ValueAwaiter<T> operator co_await() {
        awaited_ = true;
        return {{child_, false}};
    }
    detail::ValueAwaiter<T> get() { return operator co_await(); }

private:
    template <class F>
    async_let(int, F&& operation) : parent_(detail::currentContext().task) {
        child_ = detail::spawnTask<T>(std::forward<F>(operation), detail::childOptions(parent_, std::nullopt));
        if (parent_) parent_->addChild(child_.get());
    }

    detail::TaskBase* parent_;
    detail::TaskRef child_;
    bool awaited_ = false;
};

template <class T>
async_let(async<T>) -> async_let<T>;
template <class F>
    requires(!detail::is_async_v<F>)
async_let(F) -> async_let<detail::async_result_t<F&>>;

// MARK: - Task executor preference

/// `await withTaskExecutorPreference(executor) { ... }` — runs `operation` on
/// `executor`; child tasks created inside (groups, `async let`) prefer it too.
template <class F>
auto withTaskExecutorPreference(TaskExecutor& executor, F operation) -> async<detail::async_result_t<F&>> {
    auto* task = detail::currentContext().task;
    Executor* saved = task ? task->executorPreference : nullptr;
    if (task) task->executorPreference = &executor;
    struct Restore {
        detail::TaskBase* task;
        Executor* saved;
        ~Restore() {
            if (task) task->executorPreference = saved;
        }
    } restore{task, saved};
    co_await hopTo(executor);
    co_return co_await detail::callAsync(operation);
}

} // namespace corcurrency
