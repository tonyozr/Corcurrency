// C++ Corcurrency — runtime core: priorities, executors, jobs and task records.
#pragma once

#include <atomic>
#include <coroutine>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace corcurrency {

// MARK: - TaskPriority

/// Mirrors Swift's `TaskPriority`. Raw values match Swift (and Dispatch QoS).
enum class TaskPriority : std::uint8_t {
    background = 9,
    low = 17,
    utility = 17,
    medium = 21,
    high = 25,
    userInitiated = 25,
};

// MARK: - CancellationError

/// Thrown by `Task<>::checkCancellation()`, `Task<>::sleep(...)` etc.
struct CancellationError : std::exception {
    const char* what() const noexcept override { return "CancellationError"; }
};

class Executor;
class ExecutorJob;

namespace detail {

class TaskBase;

/// Intrusive reference to a task record.
class TaskRef {
public:
    TaskRef() noexcept = default;
    explicit TaskRef(TaskBase* t, bool retain = true) noexcept;
    TaskRef(const TaskRef& o) noexcept;
    TaskRef(TaskRef&& o) noexcept : ptr_(std::exchange(o.ptr_, nullptr)) {}
    TaskRef& operator=(TaskRef o) noexcept { std::swap(ptr_, o.ptr_); return *this; }
    ~TaskRef();

    TaskBase* get() const noexcept { return ptr_; }
    TaskBase* operator->() const noexcept { return ptr_; }
    explicit operator bool() const noexcept { return ptr_ != nullptr; }

private:
    TaskBase* ptr_ = nullptr;
};

/// Immutable linked list of `TaskLocal` bindings, shared between parent and child tasks.
struct TaskLocalNode {
    const void* key;
    std::shared_ptr<void> value;
    std::shared_ptr<const TaskLocalNode> next;
};

/// The runtime record of a task (Swift's `AsyncTask`).
class TaskBase {
public:
    TaskBase() = default;
    TaskBase(const TaskBase&) = delete;
    TaskBase& operator=(const TaskBase&) = delete;
    virtual ~TaskBase() = default;

    void retain() noexcept { refs_.fetch_add(1, std::memory_order_relaxed); }
    void release() noexcept {
        if (refs_.fetch_sub(1, std::memory_order_acq_rel) == 1) delete this;
    }

    bool isCancelled() const noexcept { return cancelled_.load(std::memory_order_acquire); }

    /// Cancels this task, runs its cancellation handlers and cancels its child tasks.
    void cancel();

    /// Unique id for `addCancellationHandler`.
    static std::uint64_t makeHandlerId() noexcept;

    /// Returns false (and does not register) if the task is already cancelled.
    bool addCancellationHandler(std::uint64_t id, std::function<void()> handler);
    void removeCancellationHandler(std::uint64_t id);

    /// Structured children (task group / async let) are cancelled with the parent.
    void addChild(TaskBase* child);
    void removeChild(TaskBase* child);

    TaskPriority priority = TaskPriority::medium;
    std::string name;
    std::shared_ptr<const TaskLocalNode> locals;
    /// Swift's task executor preference: where non-isolated child work runs.
    Executor* executorPreference = nullptr;

private:
    std::atomic<int> refs_{1};
    std::atomic<bool> cancelled_{false};
    std::mutex mutex_;
    std::vector<std::pair<std::uint64_t, std::function<void()>>> handlers_;
    std::vector<TaskRef> children_;
};

inline TaskRef::TaskRef(TaskBase* t, bool retain) noexcept : ptr_(t) {
    if (ptr_ && retain) ptr_->retain();
}
inline TaskRef::TaskRef(const TaskRef& o) noexcept : ptr_(o.ptr_) {
    if (ptr_) ptr_->retain();
}
inline TaskRef::~TaskRef() {
    if (ptr_) ptr_->release();
}

/// What is running on the current thread right now.
struct ExecutionContext {
    TaskBase* task = nullptr;
    Executor* executor = nullptr;
};

ExecutionContext& currentContext() noexcept;

/// Enqueues `h` to resume on `executor` (the global concurrent executor if null),
/// running as part of `task`.
void schedule(Executor* executor, std::coroutine_handle<> h, TaskBase* task);

/// Calls `fn(ctx)` on a global queue after `nanoseconds`.
void scheduleAfter(std::uint64_t nanoseconds, void (*fn)(void*), void* ctx);

/// Runs `h` synchronously on this thread as part of `task` (used by `Task::immediate`).
void runInline(std::coroutine_handle<> h, TaskBase* task);

/// Parks the main thread draining the main queue. Never returns.
[[noreturn]] void dispatchMain();

bool isMainThread() noexcept;

} // namespace detail

// MARK: - Executors

/// A unit of work handed to an executor (Swift's `ExecutorJob`).
/// The executor must eventually call `runSynchronously(on:)` exactly once.
class ExecutorJob {
public:
    ExecutorJob(std::coroutine_handle<> h, detail::TaskBase* task) noexcept
        : handle_(h), task_(task) {}

    TaskPriority priority() const noexcept {
        return task_ ? task_->priority : TaskPriority::medium;
    }

    /// Resumes the job on the calling thread, treating `executor` as the current
    /// executor, then destroys the job.
    void runSynchronously(Executor& executor);

private:
    std::coroutine_handle<> handle_;
    detail::TaskRef task_;
};

/// Swift's `Executor`: something that can run jobs.
class Executor {
public:
    virtual ~Executor() = default;
    /// Takes ownership of `job`.
    virtual void enqueue(ExecutorJob* job) = 0;
};

/// Swift's `SerialExecutor`: runs at most one job at a time. Actors use these.
class SerialExecutor : public Executor {
public:
    /// True if the calling code currently runs on this executor.
    bool isIsolatingCurrentContext() const noexcept {
        return detail::currentContext().executor == this;
    }
};

/// Swift's `TaskExecutor` (see `withTaskExecutorPreference`).
class TaskExecutor : public Executor {};

/// A serial executor backed by a private serial dispatch queue.
class DispatchSerialQueueExecutor final : public SerialExecutor {
public:
    explicit DispatchSerialQueueExecutor(const char* label = nullptr);
    ~DispatchSerialQueueExecutor() override;
    DispatchSerialQueueExecutor(const DispatchSerialQueueExecutor&) = delete;
    DispatchSerialQueueExecutor& operator=(const DispatchSerialQueueExecutor&) = delete;

    void enqueue(ExecutorJob* job) override;

private:
    void* queue_;
};

/// The executor of `MainActor` (the main dispatch queue).
SerialExecutor& mainExecutor();

/// The default executor for non-isolated tasks (global concurrent dispatch queues).
TaskExecutor& globalConcurrentExecutor();

} // namespace corcurrency
