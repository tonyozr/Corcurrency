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

#include <deque>
#include <memory>
#include <mutex>
#include <optional>

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
template <class T>
class GroupState {
public:
    using Stored = std::conditional_t<std::is_void_v<T>, std::monostate, T>;

    GroupState(TaskBase* parent, bool discardResults, bool throwing)
        : parent_(parent), discardResults_(discardResults), throwing_(throwing) {}

    TaskBase* parent() const noexcept { return parent_; }

    bool isCancelled() {
        std::lock_guard lock(mutex_);
        return cancelled_ || (parent_ && parent_->isCancelled());
    }

    bool isEmpty() {
        std::lock_guard lock(mutex_);
        return pending_ == 0 && ready_.empty();
    }

    /// Registers a new child (returns false if `onlyUnlessCancelled` and the group is cancelled).
    bool beginChild(bool onlyUnlessCancelled) {
        std::lock_guard lock(mutex_);
        if (onlyUnlessCancelled && (cancelled_ || (parent_ && parent_->isCancelled()))) return false;
        ++pending_;
        return true;
    }

    void attachChild(TaskRef child) {
        bool cancelNow;
        {
            std::lock_guard lock(mutex_);
            cancelNow = cancelled_;
            running_.push_back(child);
        }
        if (parent_) parent_->addChild(child.get());
        if (cancelNow) child->cancel();
    }

    void childFinished(TaskBase* child, Result<T> result) {
        std::optional<Waiter> waiter;
        std::vector<TaskRef> toCancel;
        {
            std::lock_guard lock(mutex_);
            --pending_;
            std::erase_if(running_, [child](auto& c) { return c.get() == child; });
            if (discardResults_) {
                if (!result.isSuccess() && throwing_ && !firstError_) {
                    firstError_ = result.error();
                    cancelled_ = true;
                    toCancel = running_;
                }
            } else {
                ready_.push_back(std::move(result));
            }
            if (waiter_ && (!ready_.empty() || pending_ == 0)) {
                waiter = std::move(waiter_);
                waiter_.reset();
            }
        }
        if (parent_) parent_->removeChild(child);
        for (auto& c : toCancel) c->cancel();
        if (waiter) waiter->resume();
    }

    void cancelAll() {
        std::vector<TaskRef> running;
        {
            std::lock_guard lock(mutex_);
            cancelled_ = true;
            running = running_;
        }
        for (auto& c : running) c->cancel();
    }

    /// Awaiter for the next completed child (`nullopt` when the group is empty).
    struct NextAwaiter {
        std::shared_ptr<GroupState> state;
        bool await_ready() {
            std::lock_guard lock(state->mutex_);
            return !state->ready_.empty() || state->pending_ == 0;
        }
        bool await_suspend(std::coroutine_handle<> h) {
            std::lock_guard lock(state->mutex_);
            if (!state->ready_.empty() || state->pending_ == 0) return false;
            state->waiter_ = Waiter::current(h);
            return true;
        }
        std::optional<Result<T>> await_resume() {
            std::lock_guard lock(state->mutex_);
            if (state->ready_.empty()) return std::nullopt;
            auto r = std::move(state->ready_.front());
            state->ready_.pop_front();
            return r;
        }
    };

    std::exception_ptr firstError() {
        std::lock_guard lock(mutex_);
        return firstError_;
    }

private:
    std::mutex mutex_;
    TaskBase* parent_;
    bool discardResults_;
    bool throwing_;
    bool cancelled_ = false;
    std::size_t pending_ = 0;
    std::deque<Result<T>> ready_;
    std::vector<TaskRef> running_;
    std::optional<Waiter> waiter_;
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
        : state_(std::make_shared<detail::GroupState<ChildTaskResult>>(parent, false, true)) {}

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
        auto options = detail::childOptions(state_->parent(), priority);
        auto child = detail::spawnTask<void>(
            [state = state_, op = std::decay_t<F>(std::forward<F>(operation))]() mutable {
                return detail::runGroupChild<ChildTaskResult>(state, std::move(op));
            },
            std::move(options));
        state_->attachChild(std::move(child));
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
        : state_(std::make_shared<detail::GroupState<void>>(parent, true, throwing)) {}

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
        auto child = detail::spawnTask<void>(
            [state = state_, op = std::decay_t<F>(std::forward<F>(operation))]() mutable {
                return detail::runGroupChild<void>(state, std::move(op));
            },
            detail::childOptions(state_->parent(), priority));
        state_->attachChild(std::move(child));
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
