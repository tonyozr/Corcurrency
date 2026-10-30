// Test runner: every test is an async function executed in order on the main actor.
#include <corcurrency/corcurrency.hpp>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using namespace corcurrency;
using namespace std::chrono_literals;

namespace {

int failures = 0;

#define EXPECT(cond)                                                          \
    do {                                                                      \
        if (!(cond)) {                                                        \
            std::printf("    FAILED %s:%d: %s\n", __FILE__, __LINE__, #cond); \
            ++failures;                                                       \
        }                                                                     \
    } while (0)

struct TestCase {
    const char* name;
    async<void> (*fn)();
};
std::vector<TestCase>& registry() {
    static std::vector<TestCase> tests;
    return tests;
}
struct Registrar {
    Registrar(const char* name, async<void> (*fn)()) { registry().push_back({name, fn}); }
};
#define TEST(name)                                   \
    async<void> test_##name();                       \
    Registrar registrar_##name(#name, &test_##name); \
    async<void> test_##name()

// MARK: async functions

auto add(int a, int b) -> async<int> { co_return a + b; }
auto addTwice(int a, int b) -> async<int> {
    int x = co_await add(a, b);
    co_return co_await add(x, x);
}
auto throwing() -> async<int> {
    throw std::runtime_error("boom");
    co_return 0;
}

TEST(async_function_returns_value) {
    EXPECT(co_await add(2, 3) == 5);
    EXPECT(co_await addTwice(1, 2) == 6);
}

TEST(async_function_propagates_exception) {
    bool caught = false;
    try {
        (void)co_await throwing();
    } catch (const std::runtime_error& e) {
        caught = std::string(e.what()) == "boom";
    }
    EXPECT(caught);
}

// MARK: Task

TEST(task_value) {
    Task task([]() -> async<int> { co_return 42; });
    EXPECT(co_await task.value() == 42);
    EXPECT(co_await task == 42); // awaiting twice returns the same value
}

TEST(task_sync_closure) {
    Task task([] { return std::string("sync"); });
    EXPECT(co_await task.value() == "sync");
}

TEST(task_inherits_main_actor) {
    EXPECT(MainActor::isIsolatingCurrentContext());
    Task task([]() -> async<bool> { co_return MainActor::isIsolatingCurrentContext(); });
    EXPECT(co_await task.value());
}

TEST(main_actor_runs_on_main_thread) {
    EXPECT(detail::isMainThread());
    co_await Task<>::sleep(5ms);
    EXPECT(detail::isMainThread());
    bool fromDetached = co_await Task<bool>::detached([]() -> async<bool> {
        co_return co_await MainActor::run([] { return detail::isMainThread(); });
    }).value();
    EXPECT(fromDetached);
}

TEST(task_detached_runs_off_main_actor) {
    auto task = Task<bool>::detached([]() -> async<bool> { co_return MainActor::isIsolatingCurrentContext(); });
    EXPECT(!(co_await task.value()));
    EXPECT(MainActor::isIsolatingCurrentContext()); // came back to the main actor
}

TEST(task_result_and_error) {
    Task task([]() -> async<int> { co_return co_await throwing(); });
    auto result = co_await task.result();
    EXPECT(!result.isSuccess());
    bool caught = false;
    try {
        (void)co_await task.value();
    } catch (const std::runtime_error&) {
        caught = true;
    }
    EXPECT(caught);
}

TEST(task_priority_and_name) {
    auto task = Task<TaskPriority>::detached(TaskPriority::high, [] { return Task<>::currentPriority(); });
    EXPECT(co_await task.value() == TaskPriority::high);
    Task named("worker", TaskPriority::low, [] { return Task<>::name().value_or(""); });
    EXPECT(co_await named.value() == "worker");
}

TEST(task_cancellation_interrupts_sleep) {
    auto start = std::chrono::steady_clock::now();
    auto task = Task<bool>::detached([]() -> async<bool> {
        try {
            co_await Task<>::sleep(10s);
        } catch (const CancellationError&) {
            co_return Task<>::isCancelled();
        }
        co_return false;
    });
    co_await Task<>::sleep(20ms);
    task.cancel();
    EXPECT(co_await task.value());
    EXPECT(std::chrono::steady_clock::now() - start < 5s);
}

TEST(task_sleep_waits) {
    auto start = std::chrono::steady_clock::now();
    co_await Task<>::sleep(50ms);
    EXPECT(std::chrono::steady_clock::now() - start >= 45ms);
    EXPECT(MainActor::isIsolatingCurrentContext());
}

TEST(task_check_cancellation_throws) {
    auto task = Task<int>::detached([]() -> async<int> {
        while (true) {
            Task<>::checkCancellation();
            co_await Task<>::yield();
        }
    });
    task.cancel();
    bool cancelled = false;
    try {
        (void)co_await task.value();
    } catch (const CancellationError&) {
        cancelled = true;
    }
    EXPECT(cancelled);
}

TEST(task_cancellation_handler) {
    std::atomic<bool> handlerRan{false};
    auto task = Task<void>::detached([&]() -> async<void> {
        co_await withTaskCancellationHandler(
            []() -> async<void> {
                try {
                    co_await Task<>::sleep(10s);
                } catch (const CancellationError&) {
                }
            },
            [&] { handlerRan = true; });
    });
    co_await Task<>::sleep(10ms);
    task.cancel();
    co_await task.value();
    EXPECT(handlerRan.load());
}

TEST(task_immediate_starts_synchronously) {
    bool started = false;
    auto task = Task<void>::immediate([&]() -> async<void> {
        started = true;
        co_await Task<>::yield();
    });
    EXPECT(started);
    co_await task.value();
}

TEST(concurrent_leaves_actor) {
    bool onMain = co_await concurrent([] { return MainActor::isIsolatingCurrentContext(); });
    EXPECT(!onMain);
    EXPECT(MainActor::isIsolatingCurrentContext());
}

// MARK: actors

class Counter : public enable_actor_from_this<Counter> {
public:
    explicit Counter(int start = 0) : value(start) {}
    int value;
    int increment(int by) { return value += by; }
    void reset() { value = 0; }
    int get() const { return value; }
    auto slowIncrement() -> async<int> {
        int v = value;
        co_await Task<>::yield();
        value = v + 1;
        co_return value;
    }
    auto viaSelf() -> async<int> { co_return co_await self()[&Counter::get](); }
    bool isolated() const { return self().unownedExecutor().isIsolatingCurrentContext(); }
};

TEST(actor_method_call) {
    actor<Counter> counter(10);
    EXPECT(co_await counter[&Counter::increment](5) == 15);
    co_await counter[&Counter::reset]();
    EXPECT(co_await counter[&Counter::get]() == 0);
    EXPECT(co_await counter[&Counter::slowIncrement]() == 1);
    EXPECT(co_await counter[&Counter::value] == 1);
    EXPECT(co_await counter[&Counter::viaSelf]() == 1);
    EXPECT(co_await counter[&Counter::isolated]());
    EXPECT(MainActor::isIsolatingCurrentContext());
}

TEST(actor_serializes_concurrent_access) {
    actor<Counter> counter;
    std::vector<Task<void>> tasks;
    for (int i = 0; i < 100; ++i) {
        tasks.push_back(Task<void>::detached([counter]() -> async<void> {
            for (int j = 0; j < 100; ++j) co_await counter[&Counter::increment](1);
        }));
    }
    for (auto& t : tasks) co_await t.value();
    EXPECT(co_await counter[&Counter::value] == 10000);
}

TEST(actor_run_isolated_closure) {
    actor<Counter> counter(3);
    int doubled = co_await counter.run([](Counter& c) { return c.value * 2; });
    EXPECT(doubled == 6);
    EXPECT(counter.nonisolated().value == 3);
}

struct BackgroundActor : GlobalActor<BackgroundActor> {};

auto backgroundWork() -> async<bool> {
    co_await BackgroundActor::isolate(); // like `@BackgroundActor func backgroundWork()`
    co_await Task<>::sleep(1ms);
    co_return BackgroundActor::isIsolatingCurrentContext();
}

TEST(global_actor) {
    EXPECT(co_await backgroundWork());
    EXPECT(MainActor::isIsolatingCurrentContext());
    bool onBackground = co_await BackgroundActor::run([] { return BackgroundActor::isIsolatingCurrentContext(); });
    EXPECT(onBackground);
    auto detached = Task<bool>::detached([]() -> async<bool> {
        co_return co_await MainActor::run([] { return MainActor::isIsolatingCurrentContext(); });
    });
    EXPECT(co_await detached.value());
}

// MARK: task groups

TEST(task_group_collects_results) {
    int sum = co_await withTaskGroup<int>([](TaskGroup<int>& group) -> async<int> {
        for (int i = 1; i <= 10; ++i) {
            group.addTask([i]() -> async<int> {
                co_await Task<>::sleep(std::chrono::milliseconds(10 - i));
                co_return i * i;
            });
        }
        int total = 0;
        for_await (int value, group) total += value;
        co_return total;
    });
    EXPECT(sum == 385);
    EXPECT(MainActor::isIsolatingCurrentContext());
}

TEST(task_group_next_and_implicit_wait) {
    std::atomic<int> finished{0};
    co_await withTaskGroup<int>([&](auto& group) -> async<void> {
        for (int i = 0; i < 5; ++i) {
            group.addTask([&finished, i] {
                finished++;
                return i;
            });
        }
        auto first = co_await group.next();
        EXPECT(first.has_value());
    }); // remaining children are awaited implicitly
    EXPECT(finished == 5);
}

TEST(throwing_task_group_rethrows_and_cancels) {
    std::atomic<bool> siblingCancelled{false};
    bool caught = false;
    try {
        co_await withThrowingTaskGroup<int>([&](ThrowingTaskGroup<int>& group) -> async<void> {
            group.addTask([&]() -> async<int> {
                try {
                    co_await Task<>::sleep(10s);
                } catch (const CancellationError&) {
                    siblingCancelled = true;
                }
                co_return 0;
            });
            group.addTask([]() -> async<int> {
                throw std::runtime_error("child failed");
                co_return 1;
            });
            while (co_await group.next()) {
            }
        });
    } catch (const std::runtime_error&) {
        caught = true;
    }
    EXPECT(caught);
    EXPECT(siblingCancelled.load());
}

TEST(task_group_cancel_all) {
    int completed = co_await withTaskGroup<bool>([](TaskGroup<bool>& group) -> async<int> {
        for (int i = 0; i < 3; ++i) {
            group.addTask([]() -> async<bool> {
                try {
                    co_await Task<>::sleep(10s);
                    co_return true;
                } catch (const CancellationError&) {
                    co_return false;
                }
            });
        }
        group.cancelAll();
        EXPECT(group.isCancelled());
        EXPECT(!group.addTaskUnlessCancelled([] { return true; }));
        int n = 0;
        while (auto r = co_await group.next()) n += *r ? 1 : 0;
        co_return n;
    });
    EXPECT(completed == 0);
}

TEST(parent_cancellation_propagates_to_group_children) {
    auto task = Task<int>::detached([]() -> async<int> {
        co_return co_await withTaskGroup<int>([](TaskGroup<int>& group) -> async<int> {
            for (int i = 0; i < 3; ++i) {
                group.addTask([]() -> async<int> {
                    try {
                        co_await Task<>::sleep(10s);
                    } catch (const CancellationError&) {
                        co_return 1;
                    }
                    co_return 0;
                });
            }
            int cancelled = 0;
            for_await (int v, group) cancelled += v;
            co_return cancelled;
        });
    });
    co_await Task<>::sleep(20ms);
    task.cancel();
    EXPECT(co_await task.value() == 3);
}

TEST(discarding_task_group) {
    std::atomic<int> count{0};
    co_await withDiscardingTaskGroup([&](DiscardingTaskGroup& group) {
        for (int i = 0; i < 50; ++i) group.addTask([&count] { count++; });
    });
    EXPECT(count == 50);
    bool caught = false;
    try {
        co_await withThrowingDiscardingTaskGroup([](DiscardingTaskGroup& group) {
            group.addTask([]() -> async<void> {
                throw std::runtime_error("x");
                co_return;
            });
        });
    } catch (const std::runtime_error&) {
        caught = true;
    }
    EXPECT(caught);
}

// MARK: async let

auto slowValue(int v, std::chrono::milliseconds delay) -> async<int> {
    co_await Task<>::sleep(delay);
    co_return v;
}

TEST(async_let_runs_concurrently) {
    auto start = std::chrono::steady_clock::now();
    async_let a = slowValue(1, 100ms);
    async_let b = slowValue(2, 100ms);
    async_let c = [] { return 3; };
    int sum = co_await a + co_await b + co_await c;
    EXPECT(sum == 6);
    EXPECT(co_await a == 1); // can be awaited again
    EXPECT(std::chrono::steady_clock::now() - start < 190ms);
}

// MARK: continuations

TEST(checked_continuation) {
    int value = co_await withCheckedContinuation<int>([](CheckedContinuation<int> c) {
        Task<void>::detached([c] { c.resume(7); });
    });
    EXPECT(value == 7);
    EXPECT(MainActor::isIsolatingCurrentContext());

    co_await withCheckedContinuation([](CheckedContinuation<void> c) { c.resume(); });

    bool caught = false;
    try {
        co_await withCheckedThrowingContinuation<std::string>(
            [](auto c) { c.resumeThrowing(std::runtime_error("io")); });
    } catch (const std::runtime_error&) {
        caught = true;
    }
    EXPECT(caught);
}

// MARK: AsyncStream

TEST(async_stream_basic) {
    AsyncStream<int> stream([](AsyncStream<int>::Continuation c) {
        Task<void>::detached([c]() -> async<void> {
            for (int i = 1; i <= 5; ++i) {
                c.yield(i);
                co_await Task<>::sleep(1ms);
            }
            c.finish();
        });
    });
    std::vector<int> received;
    for_await (int x, stream) received.push_back(x);
    EXPECT((received == std::vector<int>{1, 2, 3, 4, 5}));
}

TEST(async_stream_operators) {
    auto [stream, continuation] = AsyncStream<int>::makeStream();
    for (int i = 1; i <= 10; ++i) continuation.yield(i);
    continuation.finish();

    auto evensTimesTen = stream.filter([](int x) { return x % 2 == 0; })
                             .map([](int x) -> async<int> { co_return x * 10; })
                             .dropFirst(1)
                             .prefix(3);
    auto values = co_await evensTimesTen.collect();
    EXPECT((values == std::vector<int>{40, 60, 80}));
}

TEST(async_stream_reduce_and_first) {
    auto [stream, continuation] = AsyncStream<int>::makeStream();
    for (int i = 1; i <= 4; ++i) continuation.yield(i);
    continuation.finish();
    auto [stream2, continuation2] = AsyncStream<std::string>::makeStream();
    continuation2.yield("a");
    continuation2.yield("bb");
    continuation2.finish();

    EXPECT(co_await stream.reduce(0, [](int acc, int x) { return acc + x; }) == 10);
    auto firstLong = co_await stream2.first([](const std::string& s) { return s.size() > 1; });
    EXPECT(firstLong == std::optional<std::string>("bb"));
}

TEST(async_stream_buffering_newest) {
    auto [stream, continuation] =
        AsyncStream<int>::makeStream(AsyncStream<int>::BufferingPolicy::bufferingNewest(2));
    for (int i = 1; i <= 5; ++i) continuation.yield(i);
    continuation.finish();
    auto values = co_await stream.collect();
    EXPECT((values == std::vector<int>{4, 5}));
}

TEST(async_stream_termination_on_cancel) {
    std::atomic<bool> terminatedByCancel{false};
    auto [stream, continuation] = AsyncStream<int>::makeStream();
    continuation.onTermination([&](auto reason) {
        terminatedByCancel = reason == AsyncStream<int>::Termination::cancelled;
    });
    auto consumer = Task<int>::detached([stream]() -> async<int> {
        int n = 0;
        for_await (int x, stream) n += x;
        co_return n;
    });
    continuation.yield(1);
    co_await Task<>::sleep(20ms);
    consumer.cancel();
    EXPECT(co_await consumer.value() == 1);
    EXPECT(terminatedByCancel.load());
}

TEST(async_stream_finishes_when_last_continuation_is_dropped) {
    std::atomic<bool> finished{false};
    AsyncStream<int> stream([&](AsyncStream<int>::Continuation c) {
        c.onTermination([&](auto reason) { finished = reason == AsyncStream<int>::Termination::finished; });
        Task<void>::detached([c]() -> async<void> {
            for (int i = 1; i <= 3; ++i) {
                c.yield(i);
                co_await Task<>::sleep(1ms);
            }
            // No finish(): dropping the last copy of the continuation ends the stream.
        });
    });
    std::vector<int> received;
    for_await (int x, stream) received.push_back(x);
    EXPECT((received == std::vector<int>{1, 2, 3}));
    EXPECT(finished.load());
}

TEST(async_stream_cancel_drops_element_owning_last_continuation) {
    using Job = std::function<void()>;
    auto [stream, continuation] = AsyncStream<Job>::makeStream();
    auto inBody = std::make_shared<std::atomic<bool>>(false);
    auto consumer = Task<int>::detached([stream, inBody]() -> async<int> {
        int n = 0;
        for_await (Job job, stream) {
            job();
            ++n;
            *inBody = true;
            try {
                co_await Task<>::sleep(10s);
            } catch (const CancellationError&) {
            }
        }
        co_return n;
    });
    continuation.yield([] {});
    while (!*inBody) co_await Task<>::sleep(1ms); // the consumer is in the loop body, not in next()
    continuation.yield([keep = continuation] {});
    { auto last = std::move(continuation); } // the buffered job owns the last copy
    consumer.cancel(); // next() drops the buffer; must not deadlock
    EXPECT(co_await consumer.value() == 1);
}

TEST(async_throwing_stream) {
    AsyncThrowingStream<int> stream([](auto c) {
        c.yield(1);
        c.finish(std::runtime_error("stream failed"));
    });
    int sum = 0;
    bool caught = false;
    try {
        for_await (int x, stream) sum += x;
    } catch (const std::runtime_error&) {
        caught = true;
    }
    EXPECT(sum == 1);
    EXPECT(caught);
}

// MARK: TaskLocal

TaskLocal<std::string> requestID{"none"};

TEST(task_local_values) {
    EXPECT(requestID.get() == "none");
    co_await requestID.withValue("outer", []() -> async<void> {
        EXPECT(requestID.get() == "outer");
        Task inherited([] { return requestID.get(); });
        auto detached = Task<std::string>::detached([] { return requestID.get(); });
        EXPECT(co_await inherited.value() == "outer");
        EXPECT(co_await detached.value() == "none");
        auto fromGroup = co_await withTaskGroup<std::string>([](auto& group) -> async<std::string> {
            group.addTask([] { return requestID.get(); });
            co_return (co_await group.next()).value();
        });
        EXPECT(fromGroup == "outer");
        auto nested = requestID.withValue("inner", [] { return requestID.get(); });
        EXPECT(nested == "inner");
        EXPECT(requestID.get() == "outer");
    });
    EXPECT(requestID.get() == "none");
}

// MARK: executors

class ThreadExecutor final : public TaskExecutor {
public:
    void enqueue(ExecutorJob* job) override {
        jobs.fetch_add(1);
        std::thread([this, job] { job->runSynchronously(*this); }).detach();
    }
    std::atomic<int> jobs{0};
};

TEST(task_executor_preference) {
    static ThreadExecutor executor;
    bool ranOnExecutor = co_await withTaskExecutorPreference(executor, []() -> async<bool> {
        co_return detail::currentContext().executor == &executor;
    });
    EXPECT(ranOnExecutor);
    EXPECT(executor.jobs.load() >= 1);
    EXPECT(MainActor::isIsolatingCurrentContext());
}

} // namespace

int main() {
    return asyncMain([]() -> async<int> {
        for (auto& test : registry()) {
            const int before = failures;
            std::printf("[ RUN  ] %s\n", test.name);
            try {
                co_await test.fn();
            } catch (const std::exception& e) {
                std::printf("    FAILED: uncaught exception %s\n", e.what());
                ++failures;
            }
            std::printf("[ %s ] %s\n", failures == before ? " OK " : "FAIL", test.name);
        }
        std::printf("\n%zu tests, %d failures\n", registry().size(), failures);
        co_return failures == 0 ? 0 : 1;
    });
}
