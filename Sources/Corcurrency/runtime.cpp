// C++ Corcurrency — runtime implementation on top of libdispatch.
#include "include/corcurrency/runtime.hpp"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <thread>
#include <typeindex>

// SwiftPM does not put the Swift SDK's include directory on the C++ header
// search path, so when <dispatch/dispatch.h> is not reachable we declare the
// handful of libdispatch entry points we use ourselves (stable C ABI).
#if __has_include(<dispatch/dispatch.h>)
#include <dispatch/dispatch.h>
#else
#if defined(_WIN32)
#define CORCURRENCY_DISPATCH_IMPORT __declspec(dllimport)
#else
#define CORCURRENCY_DISPATCH_IMPORT
#endif
extern "C" {
struct dispatch_queue_s;
struct dispatch_queue_attr_s;
typedef struct dispatch_queue_s* dispatch_queue_t;
typedef struct dispatch_queue_attr_s* dispatch_queue_attr_t;
typedef std::uint64_t dispatch_time_t;
typedef void (*dispatch_function_t)(void*);

CORCURRENCY_DISPATCH_IMPORT extern struct dispatch_queue_s _dispatch_main_q;
CORCURRENCY_DISPATCH_IMPORT dispatch_queue_t dispatch_get_global_queue(std::intptr_t identifier, std::uintptr_t flags);
CORCURRENCY_DISPATCH_IMPORT dispatch_queue_t dispatch_queue_create(const char* label, dispatch_queue_attr_t attr);
CORCURRENCY_DISPATCH_IMPORT void dispatch_async_f(dispatch_queue_t queue, void* context, dispatch_function_t work);
CORCURRENCY_DISPATCH_IMPORT void dispatch_after_f(dispatch_time_t when, dispatch_queue_t queue, void* context, dispatch_function_t work);
CORCURRENCY_DISPATCH_IMPORT dispatch_time_t dispatch_time(dispatch_time_t when, std::int64_t delta);
CORCURRENCY_DISPATCH_IMPORT void dispatch_release(void* object);
CORCURRENCY_DISPATCH_IMPORT void dispatch_main(void);
}
#define DISPATCH_TIME_NOW (0ull)
#define DISPATCH_QUEUE_PRIORITY_HIGH 2
#define DISPATCH_QUEUE_PRIORITY_DEFAULT 0
#define DISPATCH_QUEUE_PRIORITY_LOW (-2)
#define DISPATCH_QUEUE_PRIORITY_BACKGROUND (-32768)
#define dispatch_get_main_queue() (&_dispatch_main_q)
#endif

// Run-loop integration SPI (what Foundation's RunLoop uses on Linux/Windows):
// lets the main thread itself drain the main queue, instead of `dispatch_main()`
// which parks the main thread and hands the main queue to worker threads.
#if defined(_WIN32)
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
extern "C" {
__declspec(dllimport) HANDLE _dispatch_get_main_queue_handle_4CF(void);
__declspec(dllimport) void _dispatch_main_queue_callback_4CF(void* msg);
}
#define CORCURRENCY_MAIN_QUEUE_RUNLOOP 1
#elif defined(__linux__) || defined(__ANDROID__)
#include <poll.h>
#include <sys/eventfd.h>
extern "C" {
int _dispatch_get_main_queue_handle_4CF(void);
void _dispatch_main_queue_callback_4CF(void* msg);
}
#define CORCURRENCY_MAIN_QUEUE_RUNLOOP 1
#endif

namespace corcurrency {
namespace detail {

namespace {
thread_local ExecutionContext tlsContext;
const std::thread::id mainThreadId = std::this_thread::get_id();
std::atomic<std::uint64_t> nextHandlerId{1};

dispatch_queue_t globalQueue(TaskPriority p) {
    std::intptr_t id = DISPATCH_QUEUE_PRIORITY_DEFAULT;
    const auto raw = static_cast<int>(p);
    if (raw >= static_cast<int>(TaskPriority::high)) id = DISPATCH_QUEUE_PRIORITY_HIGH;
    else if (raw >= static_cast<int>(TaskPriority::medium)) id = DISPATCH_QUEUE_PRIORITY_DEFAULT;
    else if (raw >= static_cast<int>(TaskPriority::low)) id = DISPATCH_QUEUE_PRIORITY_LOW;
    else id = DISPATCH_QUEUE_PRIORITY_BACKGROUND;
    return dispatch_get_global_queue(id, 0);
}

struct DispatchJob {
    ExecutorJob* job;
    Executor* executor;
};

void runDispatchJob(void* ctx) {
    auto* dj = static_cast<DispatchJob*>(ctx);
    auto* job = dj->job;
    auto* executor = dj->executor;
    delete dj;
    job->runSynchronously(*executor);
}

void enqueueOnQueue(dispatch_queue_t queue, ExecutorJob* job, Executor* executor) {
    dispatch_async_f(queue, new DispatchJob{job, executor}, &runDispatchJob);
}

class GlobalConcurrentExecutor final : public TaskExecutor {
public:
    void enqueue(ExecutorJob* job) override {
        enqueueOnQueue(globalQueue(job->priority()), job, this);
    }
};

class MainQueueExecutor final : public SerialExecutor {
public:
    void enqueue(ExecutorJob* job) override {
        enqueueOnQueue(dispatch_get_main_queue(), job, this);
    }
};
} // namespace

ExecutionContext& currentContext() noexcept { return tlsContext; }

std::shared_ptr<const TaskLocalNode>& threadTaskLocals() noexcept {
    thread_local std::shared_ptr<const TaskLocalNode> locals;
    return locals;
}

bool isMainThread() noexcept { return std::this_thread::get_id() == mainThreadId; }

void schedule(Executor* executor, std::coroutine_handle<> h, TaskBase* task) {
    if (!executor) executor = &globalConcurrentExecutor();
    executor->enqueue(new ExecutorJob(h, task));
}

void scheduleAfter(std::uint64_t nanoseconds, void (*fn)(void*), void* ctx) {
    const auto delta = static_cast<std::int64_t>(std::min<std::uint64_t>(nanoseconds, INT64_MAX));
    dispatch_after_f(dispatch_time(DISPATCH_TIME_NOW, delta), globalQueue(TaskPriority::medium), ctx, fn);
}

void runInline(std::coroutine_handle<> h, TaskBase* task) {
    auto saved = tlsContext;
    tlsContext.task = task;
    h.resume();
    tlsContext = saved;
}

void dispatchMain() {
#if defined(CORCURRENCY_MAIN_QUEUE_RUNLOOP)
    // Drain the main queue on the main thread forever, so MainActor code keeps
    // main-thread affinity (needed by GUI toolkits, like Swift's MainActor).
    const auto handle = _dispatch_get_main_queue_handle_4CF();
    for (;;) {
        _dispatch_main_queue_callback_4CF(nullptr);
#if defined(_WIN32)
        WaitForSingleObject(handle, INFINITE);
#else
        pollfd fd{handle, POLLIN, 0};
        poll(&fd, 1, -1);
        eventfd_t ignored;
        eventfd_read(handle, &ignored);
#endif
    }
#else
    dispatch_main();
    std::abort(); // unreachable
#endif
}

// MARK: TaskBase

void TaskBase::cancel() {
    if (cancelled_.exchange(true, std::memory_order_acq_rel)) return;
    std::vector<std::pair<std::uint64_t, std::function<void()>>> handlers;
    std::vector<TaskRef> children;
    {
        std::lock_guard lock(mutex_);
        handlers = handlers_;
        children = children_.snapshot();
    }
    for (auto& [id, handler] : handlers) handler();
    for (auto& child : children) child->cancel();
}

std::uint64_t TaskBase::makeHandlerId() noexcept {
    return nextHandlerId.fetch_add(1, std::memory_order_relaxed);
}

bool TaskBase::addCancellationHandler(std::uint64_t id, std::function<void()> handler) {
    std::lock_guard lock(mutex_);
    if (isCancelled()) return false;
    handlers_.emplace_back(id, std::move(handler));
    return true;
}

void TaskBase::removeCancellationHandler(std::uint64_t id) {
    std::lock_guard lock(mutex_);
    std::erase_if(handlers_, [id](auto& h) { return h.first == id; });
}

void TaskBase::addChild(TaskBase* child) {
    {
        std::lock_guard lock(mutex_);
        if (!isCancelled()) {
            children_.pushBack(child);
            return;
        }
    }
    child->cancel();
}

void TaskBase::removeChild(TaskBase* child) {
    TaskRef removed; // released after unlocking
    std::lock_guard lock(mutex_);
    removed = children_.remove(child);
}

} // namespace detail

// MARK: - Executors

void ExecutorJob::runSynchronously(Executor& executor) {
    // Keep the task alive for the duration of the run; the job itself is gone.
    auto task = std::move(task_);
    auto h = handle_;
    delete this;
    auto& ctx = detail::currentContext();
    const auto saved = ctx;
    ctx.task = task.get();
    ctx.executor = &executor;
    h.resume();
    ctx = saved;
}

DispatchSerialQueueExecutor::DispatchSerialQueueExecutor(const char* label)
    : queue_(dispatch_queue_create(label ? label : "corcurrency.actor", nullptr)) {}

DispatchSerialQueueExecutor::~DispatchSerialQueueExecutor() {
    dispatch_release(queue_);
}

void DispatchSerialQueueExecutor::enqueue(ExecutorJob* job) {
    detail::enqueueOnQueue(static_cast<dispatch_queue_t>(queue_), job, this);
}

// Intentionally leaked: worker threads may still use them during `exit()`.
SerialExecutor& mainExecutor() {
    static auto* executor = new detail::MainQueueExecutor();
    return *executor;
}

TaskExecutor& globalConcurrentExecutor() {
    static auto* executor = new detail::GlobalConcurrentExecutor();
    return *executor;
}

SerialExecutor& detail::globalActorExecutor(const std::type_info& type) {
    static auto* mutex = new std::mutex();
    static auto* executors = new std::map<std::type_index, SerialExecutor*>();
    std::lock_guard lock(*mutex);
    auto& executor = (*executors)[std::type_index(type)];
    if (!executor) executor = new DispatchSerialQueueExecutor(type.name());
    return *executor;
}

} // namespace corcurrency
