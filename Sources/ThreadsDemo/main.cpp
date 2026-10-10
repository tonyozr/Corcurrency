// Where does code run? A guided trace of threads and executors.
//
// Every log line shows the thread (main / W1, W2, ... for dispatch workers /
// named threads for custom executors) and the executor the code is isolated to.
// Things to notice:
//   * after every `co_await` you are back on your own executor (actor), even if
//     the callee ran somewhere else;
//   * an actor is not a thread: its jobs run on whatever dispatch worker is free,
//     but never two at a time;
//   * the main actor always runs on the main thread.
#include <corcurrency/corcurrency.hpp>

#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <deque>
#include <map>
#include <mutex>
#include <string>
#include <thread>

using namespace corcurrency;
using namespace std::chrono_literals;

// MARK: - Logging helpers

namespace {

std::mutex logMutex;
std::map<std::thread::id, std::string> threadNames;
int workerCount = 0;

void nameThisThread(std::string name) {
    std::lock_guard lock(logMutex);
    threadNames[std::this_thread::get_id()] = std::move(name);
}

std::string threadName() {
    auto id = std::this_thread::get_id();
    auto it = threadNames.find(id);
    if (it != threadNames.end()) return it->second;
    return threadNames[id] = "W" + std::to_string(++workerCount);
}

std::map<const Executor*, std::string>& executorNames() {
    static std::map<const Executor*, std::string> names;
    return names;
}

std::string executorName() {
    auto* executor = detail::currentContext().executor;
    if (executor == nullptr) return "none";
    auto it = executorNames().find(executor);
    return it != executorNames().end() ? it->second : "?";
}

void log(const char* what) {
    std::lock_guard lock(logMutex);
    std::printf("  %-48s thread=%-8s executor=%s\n", what, threadName().c_str(), executorName().c_str());
}

void section(const char* title) {
    std::lock_guard lock(logMutex);
    std::printf("\n%s\n", title);
}

// MARK: - A custom executor with its own dedicated thread

/// Swift: `final class RenderExecutor: SerialExecutor { func enqueue(_ job: consuming ExecutorJob) }`
class DedicatedThreadExecutor final : public TaskExecutor {
public:
    explicit DedicatedThreadExecutor(std::string name) {
        thread_ = std::thread([this, name] {
            nameThisThread(name);
            for (;;) {
                ExecutorJob* job;
                {
                    std::unique_lock lock(mutex_);
                    ready_.wait(lock, [this] { return !jobs_.empty(); });
                    job = jobs_.front();
                    jobs_.pop_front();
                }
                job->runSynchronously(*this);
            }
        });
        thread_.detach();
    }

    void enqueue(ExecutorJob* job) override {
        {
            std::lock_guard lock(mutex_);
            jobs_.push_back(job);
        }
        ready_.notify_one();
    }

private:
    std::thread thread_;
    std::mutex mutex_;
    std::condition_variable ready_;
    std::deque<ExecutorJob*> jobs_;
};

// MARK: - Actors

class Database {
public:
    auto query(std::string sql) -> async<int> {
        log("Database.query: start (isolated to actor)");
        co_await Task<>::sleep(5ms); // suspension: the actor is free meanwhile
        log("Database.query: after sleep (same actor!)");
        co_await concurrent([] { log("  concurrent{}: off the actor"); });
        log("Database.query: back from concurrent{}");
        co_return static_cast<int>(sql.size());
    }

    void touch(int i) {
        std::lock_guard lock(logMutex);
        std::printf("  Database.touch(%d) %-30s thread=%-8s executor=%s\n", i, "", threadName().c_str(),
                    executorName().c_str());
    }
};

struct ImageActor : GlobalActor<ImageActor> {};

auto decodeImage() -> async<void> {
    co_await ImageActor::isolate(); // `@ImageActor func decodeImage()`
    log("decodeImage (@ImageActor)");
    co_await Task<>::sleep(1ms);
    log("decodeImage after sleep (@ImageActor)");
}

auto nonisolatedHelper() -> async<void> {
    log("nonisolated async helper (runs on caller's executor)");
    co_return;
}

} // namespace

int main() {
    nameThisThread("main");
    return asyncMain([]() -> async<void> {
        actor<Database> database;
        executorNames()[&mainExecutor()] = "MainActor";
        executorNames()[&globalConcurrentExecutor()] = "global";
        executorNames()[&database.unownedExecutor()] = "actor<Database>";
        executorNames()[&ImageActor::sharedExecutor()] = "@ImageActor";

        section("1) Async main runs on the main actor (main thread)");
        log("asyncMain body");
        co_await nonisolatedHelper();

        section("2) Calling an actor hops to its executor and back");
        int rows = co_await database[&Database::query]("SELECT 1");
        log("after await database.query -> back on main");
        (void)rows;

        section("3) An actor is serial, not a thread: 6 calls, various workers, one at a time");
        co_await withDiscardingTaskGroup([database](DiscardingTaskGroup& group) {
            for (int i = 0; i < 6; ++i) {
                group.addTask([database, i]() -> async<void> { co_await database[&Database::touch](i); });
            }
        });

        section("4) Task {} inherits the main actor; Task.detached does not");
        co_await Task([]() -> async<void> {
            log("Task { } body");
            co_return;
        }).value();
        co_await Task<void>::detached([]() -> async<void> {
            log("Task.detached body");
            co_await Task<>::sleep(1ms);
            log("Task.detached after sleep (maybe another worker)");
        }).value();

        section("5) Task group children run concurrently on global workers");
        co_await withTaskGroup<int>([](TaskGroup<int>& group) -> async<void> {
            for (int i = 0; i < 4; ++i) {
                group.addTask([i]() -> async<int> {
                    log(("child " + std::to_string(i)).c_str());
                    co_await Task<>::sleep(std::chrono::milliseconds(2));
                    co_return i;
                });
            }
            for_await (int i, group) {
                (void)i;
                log("for await child result -> on main");
            }
        });

        section("6) Global actors");
        co_await decodeImage();
        log("after decodeImage -> back on main");
        co_await MainActor::run([] { log("MainActor.run { }"); });

        section("7) A custom executor owns a dedicated thread");
        static DedicatedThreadExecutor renderExecutor("render");
        executorNames()[&renderExecutor] = "RenderExecutor";
        co_await withTaskExecutorPreference(renderExecutor, []() -> async<void> {
            log("withTaskExecutorPreference(render)");
            co_await Task<>::sleep(1ms);
            log("after sleep -> still the render thread");
        });
        log("back on main");

        section("8) Task.immediate starts synchronously on the current thread");
        auto immediate = Task<void>::immediate([]() -> async<void> {
            log("Task.immediate before first suspension");
            co_await Task<>::yield();
            log("Task.immediate after yield");
        });
        log("right after Task.immediate returned");
        co_await immediate.value();

        section("9) A callback API resumed from a foreign thread returns to our executor");
        int value = co_await withCheckedContinuation<int>([](CheckedContinuation<int> continuation) {
            std::thread([continuation] {
                nameThisThread("legacy");
                log("legacy callback thread resumes the continuation");
                continuation.resume(42);
            }).detach();
        });
        log("after withCheckedContinuation -> back on main");
        (void)value;
    });
}
