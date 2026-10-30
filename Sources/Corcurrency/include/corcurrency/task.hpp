// C++ Corcurrency — `Task`, `Task<>::sleep`, cancellation.
//
//     Task task([]() -> async<int> { co_return 42; });          // Task { ... }
//     auto d = Task<int>::detached(TaskPriority::high, fn);       // Task.detached(priority: .high) { ... }
//     int v = co_await task.value();                              // await task.value
//     task.cancel();
//     co_await Task<>::sleep(100ms);                              // try await Task.sleep(for: .milliseconds(100))
//     if (Task<>::isCancelled()) ...                              // Task.isCancelled
#pragma once

#include <corcurrency/async.hpp>

#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <variant>
#include <vector>

namespace corcurrency {

/// Marker for `Task<Never>` (`Task<>`), which only hosts Swift's static task APIs.
struct Never {
    Never() = delete;
};

template <class Success = Never>
class Task;

// MARK: - Result

/// Swift's `Result<Success, any Error>` where the error is a C++ exception.
template <class T>
class Result {
public:
    static Result success(T value) { return Result(std::in_place_index<0>, std::move(value)); }
    static Result failure(std::exception_ptr e) { return Result(std::in_place_index<1>, std::move(e)); }

    bool isSuccess() const noexcept { return storage_.index() == 0; }
    /// Returns the value or rethrows the error (Swift's `get()`).
    T get() const& {
        if (!isSuccess()) std::rethrow_exception(std::get<1>(storage_));
        return std::get<0>(storage_);
    }
    T get() && {
        if (!isSuccess()) std::rethrow_exception(std::get<1>(storage_));
        return std::move(std::get<0>(storage_));
    }
    std::exception_ptr error() const noexcept { return isSuccess() ? nullptr : std::get<1>(storage_); }

private:
    template <std::size_t I, class U>
    Result(std::in_place_index_t<I> i, U&& v) : storage_(i, std::forward<U>(v)) {}
    std::variant<T, std::exception_ptr> storage_;
};

template <>
class Result<void> {
public:
    static Result success() { return Result(nullptr); }
    static Result failure(std::exception_ptr e) { return Result(std::move(e)); }
    bool isSuccess() const noexcept { return error_ == nullptr; }
    void get() const {
        if (error_) std::rethrow_exception(error_);
    }
    std::exception_ptr error() const noexcept { return error_; }

private:
    explicit Result(std::exception_ptr e) : error_(std::move(e)) {}
    std::exception_ptr error_;
};

namespace detail {

/// A coroutine that starts suspended and destroys itself when done.
struct DetachedCoroutine {
    struct promise_type {
        DetachedCoroutine get_return_object() noexcept {
            return {std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_always initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() noexcept {}
        void unhandled_exception() noexcept { std::terminate(); }
    };
    std::coroutine_handle<promise_type> handle;
};

/// Someone suspended until a task (or group child) finishes.
struct Waiter {
    std::coroutine_handle<> handle;
    Executor* executor;
    TaskRef task;

    static Waiter current(std::coroutine_handle<> h) {
        auto& ctx = currentContext();
        return {h, ctx.executor, TaskRef(ctx.task)};
    }
    void resume() const { schedule(executor, handle, task.get()); }
};

/// Task record + result storage for `Task<T>`.
template <class T>
class TaskState final : public TaskBase {
public:
    using Stored = std::conditional_t<std::is_void_v<T>, std::monostate, T>;

    void succeed(Stored value) { finish(Result<T>::success(std::move(value))); }
    void fail(std::exception_ptr e) { finish(Result<T>::failure(std::move(e))); }

    /// Returns false if the task already finished (do not suspend).
    bool addWaiter(Waiter w) {
        std::lock_guard lock(mutex_);
        if (result_) return false;
        waiters_.push_back(std::move(w));
        return true;
    }

    const Result<T>& result() const noexcept { return *result_; }
    bool isFinished() {
        std::lock_guard lock(mutex_);
        return result_.has_value();
    }

private:
    void finish(Result<T> r) {
        std::vector<Waiter> waiters;
        {
            std::lock_guard lock(mutex_);
            result_.emplace(std::move(r));
            waiters.swap(waiters_);
        }
        for (auto& w : waiters) w.resume();
    }

    std::mutex mutex_;
    std::optional<Result<T>> result_;
    std::vector<Waiter> waiters_;
};

template <>
inline void TaskState<void>::succeed(std::monostate) { finish(Result<void>::success()); }

/// The top-level coroutine of every task: runs the operation, stores the result.
template <class T, class F>
DetachedCoroutine runTaskBody(TaskRef ref, F f) {
    auto* state = static_cast<TaskState<T>*>(ref.get());
    try {
        if constexpr (std::is_void_v<T>) {
            co_await invokeAsync(std::move(f));
            state->succeed({});
        } else {
            state->succeed(co_await invokeAsync(std::move(f)));
        }
    } catch (...) {
        state->fail(std::current_exception());
    }
}

/// Inheritable attributes of the creating context (Swift `Task {}` semantics).
struct TaskSpawnOptions {
    TaskPriority priority = TaskPriority::medium;
    Executor* executor = nullptr;
    std::shared_ptr<const TaskLocalNode> locals;
    std::string name;
    bool immediate = false;
};

inline TaskSpawnOptions inheritedOptions() {
    TaskSpawnOptions o;
    auto& ctx = currentContext();
    if (ctx.task) {
        o.priority = ctx.task->priority;
        o.locals = ctx.task->locals;
    }
    o.executor = ctx.executor;
    return o;
}

template <class T, class F>
TaskRef spawnTask(F&& f, TaskSpawnOptions options) {
    auto* state = new TaskState<T>();
    TaskRef ref(state, /*retain=*/false);
    state->priority = options.priority;
    state->locals = std::move(options.locals);
    state->name = std::move(options.name);
    auto co = runTaskBody<T>(ref, std::decay_t<F>(std::forward<F>(f)));
    if (options.immediate) {
        runInline(co.handle, state);
    } else {
        schedule(options.executor, co.handle, state);
    }
    return ref;
}

template <class T>
struct TaskValueAwaiter {
    TaskRef ref;
    bool wantResult;

    TaskState<T>* state() const noexcept { return static_cast<TaskState<T>*>(ref.get()); }
    bool await_ready() const { return state()->isFinished(); }
    bool await_suspend(std::coroutine_handle<> h) { return state()->addWaiter(Waiter::current(h)); }
};

template <class T>
struct ValueAwaiter : TaskValueAwaiter<T> {
    T await_resume() const { return this->state()->result().get(); }
};

template <class T>
struct ResultAwaiter : TaskValueAwaiter<T> {
    Result<T> await_resume() const { return this->state()->result(); }
};

/// `co_await Task<>::sleep(...)`.
class SleepAwaiter {
public:
    explicit SleepAwaiter(std::uint64_t nanoseconds) : nanoseconds_(nanoseconds) {}

    bool await_ready() const noexcept {
        auto* task = currentContext().task;
        return task && task->isCancelled();
    }

    bool await_suspend(std::coroutine_handle<> h) {
        // Read members before registering the cancellation handler: it may resume
        // the coroutine on another thread and destroy this awaiter at once.
        const auto nanoseconds = nanoseconds_;
        auto& ctx = currentContext();
        auto st = std::make_shared<State>();
        st->handle = h;
        st->executor = ctx.executor;
        st->task = TaskRef(ctx.task);
        state_ = st;
        if (ctx.task) {
            st->handlerId = TaskBase::makeHandlerId();
            const bool added = ctx.task->addCancellationHandler(st->handlerId, [st] {
                int expected = kPending;
                if (st->phase.compare_exchange_strong(expected, kCancelled)) st->resumeNow();
            });
            if (!added) {
                st->handlerId = 0;
                st->phase = kCancelled;
                return false;
            }
        }
        // From here on `this` may already be resumed and destroyed.
        scheduleAfter(nanoseconds, &fire, new std::shared_ptr<State>(std::move(st)));
        return true;
    }

    void await_resume() {
        auto& ctx = currentContext();
        if (state_ && state_->handlerId && ctx.task) ctx.task->removeCancellationHandler(state_->handlerId);
        if (!state_ || state_->phase.load() == kCancelled) throw CancellationError();
    }

private:
    static constexpr int kPending = 0, kFired = 1, kCancelled = 2;

    struct State {
        std::atomic<int> phase{kPending};
        std::coroutine_handle<> handle;
        Executor* executor = nullptr;
        TaskRef task;
        std::uint64_t handlerId = 0;
        void resumeNow() { schedule(executor, handle, task.get()); }
    };

    static void fire(void* ctx) {
        auto* holder = static_cast<std::shared_ptr<State>*>(ctx);
        auto& st = *holder;
        int expected = kPending;
        if (st->phase.compare_exchange_strong(expected, kFired)) st->resumeNow();
        delete holder;
    }

    std::uint64_t nanoseconds_;
    std::shared_ptr<State> state_;
};

/// `co_await Task<>::yield()` — reschedule on the current executor.
struct YieldAwaiter {
    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) const {
        auto& ctx = currentContext();
        schedule(ctx.executor, h, ctx.task);
    }
    void await_resume() const noexcept {}
};

template <class F>
concept TaskOperation = std::invocable<F&>;

} // namespace detail

// MARK: - Task

/// Swift's `Task<Success, any Error>`. A handle to an unstructured task; copies
/// refer to the same task. Await `value()` / `result()`; errors are exceptions.
template <class Success>
class Task {
public:
    /// `Task { ... }` — inherits priority, task-locals and the current actor
    /// isolation (executor) of the creating context.
    template <detail::TaskOperation F>
    explicit Task(F&& operation)
        : ref_(detail::spawnTask<Success>(std::forward<F>(operation), detail::inheritedOptions())) {}

    /// `Task(priority: p) { ... }`
    template <detail::TaskOperation F>
    Task(TaskPriority priority, F&& operation)
        : ref_(detail::spawnTask<Success>(std::forward<F>(operation), withPriority(priority))) {}

    /// `Task(name: "...", priority: p) { ... }` (SE-0469)
    template <detail::TaskOperation F>
    Task(std::string name, TaskPriority priority, F&& operation)
        : ref_(detail::spawnTask<Success>(std::forward<F>(operation), withName(std::move(name), priority))) {}

    /// `Task.detached { ... }` — no inherited isolation, priority or task-locals.
    template <detail::TaskOperation F>
    static Task detached(F&& operation) {
        return detached(TaskPriority::medium, std::forward<F>(operation));
    }
    template <detail::TaskOperation F>
    static Task detached(TaskPriority priority, F&& operation) {
        detail::TaskSpawnOptions o;
        o.priority = priority;
        o.executor = &globalConcurrentExecutor();
        return Task(detail::spawnTask<Success>(std::forward<F>(operation), std::move(o)));
    }

    /// `Task.immediate { ... }` (SE-0472) — starts running synchronously on the
    /// calling thread until its first suspension point.
    template <detail::TaskOperation F>
    static Task immediate(F&& operation) {
        auto o = detail::inheritedOptions();
        o.immediate = true;
        return Task(detail::spawnTask<Success>(std::forward<F>(operation), std::move(o)));
    }

    /// `await task.value` — rethrows the task's error.
    detail::ValueAwaiter<Success> value() const { return {{ref_, false}}; }
    /// `await task.result`
    detail::ResultAwaiter<Success> result() const { return {{ref_, true}}; }
    /// `co_await task` is shorthand for `co_await task.value()`.
    detail::ValueAwaiter<Success> operator co_await() const { return value(); }

    void cancel() const { ref_->cancel(); }
    bool isCancelled() const noexcept { return ref_->isCancelled(); }
    TaskPriority priority() const noexcept { return ref_->priority; }
    const std::string& name() const noexcept { return ref_->name; }

    friend bool operator==(const Task& a, const Task& b) noexcept { return a.ref_.get() == b.ref_.get(); }

private:
    explicit Task(detail::TaskRef ref) : ref_(std::move(ref)) {}

    static detail::TaskSpawnOptions withPriority(TaskPriority p) {
        auto o = detail::inheritedOptions();
        o.priority = p;
        return o;
    }
    static detail::TaskSpawnOptions withName(std::string name, TaskPriority p) {
        auto o = withPriority(p);
        o.name = std::move(name);
        return o;
    }

    detail::TaskRef ref_;
};

template <class F>
Task(F) -> Task<detail::async_result_t<F&>>;
template <class F>
Task(TaskPriority, F) -> Task<detail::async_result_t<F&>>;
template <class F>
Task(std::string, TaskPriority, F) -> Task<detail::async_result_t<F&>>;

/// `Task<>` (Swift: `Task where Success == Never`) — static APIs about the current task.
template <>
class Task<Never> {
public:
    Task() = delete;

    /// `Task.isCancelled`
    static bool isCancelled() noexcept {
        auto* task = detail::currentContext().task;
        return task && task->isCancelled();
    }

    /// `try Task.checkCancellation()`
    static void checkCancellation() {
        if (isCancelled()) throw CancellationError();
    }

    /// `Task.currentPriority`
    static TaskPriority currentPriority() noexcept {
        auto* task = detail::currentContext().task;
        return task ? task->priority : TaskPriority::medium;
    }

    /// `Task.name` of the current task, if any (SE-0469).
    static std::optional<std::string> name() {
        auto* task = detail::currentContext().task;
        if (!task || task->name.empty()) return std::nullopt;
        return task->name;
    }

    /// `try await Task.sleep(for: duration)` — throws `CancellationError` if cancelled.
    template <class Rep, class Period>
    static detail::SleepAwaiter sleep(std::chrono::duration<Rep, Period> duration) {
        const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
        return detail::SleepAwaiter(ns > 0 ? static_cast<std::uint64_t>(ns) : 0);
    }

    /// `try await Task.sleep(nanoseconds:)`
    static detail::SleepAwaiter sleep(std::uint64_t nanoseconds) { return detail::SleepAwaiter(nanoseconds); }

    /// `try await Task.sleep(until: deadline)`
    template <class Clock, class Duration>
    static detail::SleepAwaiter sleep(std::chrono::time_point<Clock, Duration> deadline) {
        return sleep(deadline - Clock::now());
    }

    /// `await Task.yield()`
    static detail::YieldAwaiter yield() noexcept { return {}; }
};

// MARK: - Cancellation handler

/// `withTaskCancellationHandler(operation:onCancel:)` — `onCancel` runs
/// immediately (on the cancelling thread) when the current task is cancelled
/// while `operation` runs, or right away if it is already cancelled.
template <class Operation, class OnCancel>
auto withTaskCancellationHandler(Operation operation, OnCancel onCancel)
    -> async<detail::async_result_t<Operation&>> {
    auto* task = detail::currentContext().task;
    std::uint64_t id = 0;
    if (task) {
        id = detail::TaskBase::makeHandlerId();
        if (!task->addCancellationHandler(id, std::function<void()>(onCancel))) {
            id = 0;
            onCancel();
        }
    }
    struct Unregister {
        detail::TaskBase* task;
        std::uint64_t id;
        ~Unregister() {
            if (task && id) task->removeCancellationHandler(id);
        }
    } guard{task, id};
    co_return co_await detail::invokeAsync(std::move(operation));
}

} // namespace corcurrency
