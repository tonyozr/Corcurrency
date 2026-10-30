// C++ Corcurrency — runtime core: priorities, executors, jobs and task records.
#pragma once

#include "config.hpp"

#include <atomic>
#include <coroutine>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <typeinfo>
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
struct CORCURRENCY_API CancellationError : std::exception {
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

/// Child tasks of a task group or of a task (`async let`): an intrusive
/// doubly-linked list threaded through the tasks themselves, like Swift's
/// `TaskGroupTaskStatusRecord`. O(1) insertion and removal, no allocation.
/// A task is in at most one such list. Not synchronized: guard it with the
/// owner's mutex.
class ChildList {
public:
    ChildList() = default;
    ChildList(const ChildList&) = delete;
    ChildList& operator=(const ChildList&) = delete;

    /// Links `child` and takes a reference to it. Returns false, linking
    /// nothing, if `remove(child)` already ran: the child finished first.
    bool pushBack(TaskBase* child) noexcept;
    /// Unlinks `child` and hands over the list's reference (empty if `child`
    /// was not linked). Release it outside the owner's lock.
    TaskRef remove(TaskBase* child) noexcept;
    /// New references to every linked child, to use outside the owner's lock.
    std::vector<TaskRef> snapshot() const;
    /// Unlinks every child and hands over the list's references.
    std::vector<TaskRef> clear();
    bool empty() const noexcept { return first_ == nullptr; }

private:
    TaskBase* first_ = nullptr;
    TaskBase* last_ = nullptr;
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
    CORCURRENCY_API void cancel();

    /// Unique id for `addCancellationHandler`.
    CORCURRENCY_API static std::uint64_t makeHandlerId() noexcept;

    /// Returns false (and does not register) if the task is already cancelled.
    CORCURRENCY_API bool addCancellationHandler(std::uint64_t id, std::function<void()> handler);
    CORCURRENCY_API void removeCancellationHandler(std::uint64_t id);

    /// `async let` children are cancelled with the parent. (Task groups keep
    /// their own children and follow the parent through a cancellation handler.)
    CORCURRENCY_API void addChild(TaskBase* child);
    CORCURRENCY_API void removeChild(TaskBase* child);

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
    ChildList children_;

    // Membership in a ChildList, guarded by the mutex of that list's owner.
    friend class ChildList;
    enum class Link : std::uint8_t { none, linked, removed };
    TaskBase* prevSibling_ = nullptr;
    TaskBase* nextSibling_ = nullptr;
    Link link_ = Link::none;
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

inline bool ChildList::pushBack(TaskBase* child) noexcept {
    if (child->link_ == TaskBase::Link::removed) return false;
    child->retain();
    child->link_ = TaskBase::Link::linked;
    child->prevSibling_ = last_;
    child->nextSibling_ = nullptr;
    if (last_) {
        last_->nextSibling_ = child;
    } else {
        first_ = child;
    }
    last_ = child;
    return true;
}

inline TaskRef ChildList::remove(TaskBase* child) noexcept {
    if (child->link_ != TaskBase::Link::linked) {
        child->link_ = TaskBase::Link::removed;
        return {};
    }
    auto* prev = child->prevSibling_;
    auto* next = child->nextSibling_;
    if (prev) {
        prev->nextSibling_ = next;
    } else {
        first_ = next;
    }
    if (next) {
        next->prevSibling_ = prev;
    } else {
        last_ = prev;
    }
    child->prevSibling_ = child->nextSibling_ = nullptr;
    child->link_ = TaskBase::Link::removed;
    return TaskRef(child, /*retain=*/false);
}

inline std::vector<TaskRef> ChildList::snapshot() const {
    std::vector<TaskRef> refs;
    for (auto* c = first_; c; c = c->nextSibling_) refs.emplace_back(c);
    return refs;
}

inline std::vector<TaskRef> ChildList::clear() {
    std::vector<TaskRef> refs;
    while (first_) refs.push_back(remove(first_));
    return refs;
}

/// What is running on the current thread right now.
struct ExecutionContext {
    TaskBase* task = nullptr;
    Executor* executor = nullptr;
};

CORCURRENCY_API ExecutionContext& currentContext() noexcept;

/// Enqueues `h` to resume on `executor` (the global concurrent executor if null),
/// running as part of `task`.
CORCURRENCY_API void schedule(Executor* executor, std::coroutine_handle<> h, TaskBase* task);

/// Calls `fn(ctx)` on a global queue after `nanoseconds`.
CORCURRENCY_API void scheduleAfter(std::uint64_t nanoseconds, void (*fn)(void*), void* ctx);

/// Runs `h` synchronously on this thread as part of `task` (used by `Task::immediate`).
CORCURRENCY_API void runInline(std::coroutine_handle<> h, TaskBase* task);

/// Parks the main thread draining the main queue. Never returns.
[[noreturn]] CORCURRENCY_API void dispatchMain();

CORCURRENCY_API bool isMainThread() noexcept;

/// `TaskLocal` bindings of the current thread while it runs no task. Lives in
/// the runtime, so every module of a process sees the same bindings.
CORCURRENCY_API std::shared_ptr<const TaskLocalNode>& threadTaskLocals() noexcept;

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
    CORCURRENCY_API void runSynchronously(Executor& executor);

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
class CORCURRENCY_API DispatchSerialQueueExecutor final : public SerialExecutor {
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
CORCURRENCY_API SerialExecutor& mainExecutor();

/// The default executor for non-isolated tasks (global concurrent dispatch queues).
CORCURRENCY_API TaskExecutor& globalConcurrentExecutor();

namespace detail {
/// The serial executor of the global actor `type` (`GlobalActor<Derived>`),
/// created on first use. Kept in the runtime, so the actor is one actor even
/// when it is used from several DLLs / shared libraries.
CORCURRENCY_API SerialExecutor& globalActorExecutor(const std::type_info& type);
} // namespace detail

} // namespace corcurrency
