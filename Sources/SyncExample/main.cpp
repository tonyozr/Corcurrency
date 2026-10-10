// Using Corcurrency from ordinary synchronous C++ — no `asyncMain`.
//
// This is how the library is embedded into an existing application that owns
// its main thread (a game loop, a GUI toolkit, a server): async work is started
// with `Task<T>::detached`, and results come back to synchronous code through a
// `std::future` or a callback.
//
// Note: the main thread is not draining the main dispatch queue here, so
// `MainActor` work would never run. Code below only uses actors and the global
// concurrent executor. (`asyncMain` exists precisely to drive the main actor.)
#include <corcurrency/corcurrency.hpp>

#include <chrono>
#include <cstdio>
#include <functional>
#include <future>
#include <string>
#include <thread>

using namespace corcurrency;
using namespace std::chrono_literals;

class Inventory {
public:
    int add(const std::string& item, int count) {
        std::printf("  [actor] add %d x %s\n", count, item.c_str());
        return total += count;
    }

    auto slowCount() -> async<int> {
        co_await Task<>::sleep(20ms);
        co_return total;
    }

    int total = 0;
};

auto fetchQuote(std::string symbol) -> async<double> {
    co_await Task<>::sleep(30ms);
    co_return static_cast<double>(symbol.size()) * 10.0;
}

/// Bridge #1: run an async operation and hand its result to sync code as a
/// `std::future` (exceptions are forwarded too).
template <class F>
auto runAsFuture(F operation) -> std::future<detail::async_result_t<F&>> {
    using T = detail::async_result_t<F&>;
    auto promise = std::make_shared<std::promise<T>>();
    auto future = promise->get_future();
    Task<void>::detached([promise, operation = std::move(operation)]() mutable -> async<void> {
        try {
            if constexpr (std::is_void_v<T>) {
                co_await operation();
                promise->set_value();
            } else {
                promise->set_value(co_await operation());
            }
        } catch (...) {
            promise->set_exception(std::current_exception());
        }
    });
    return future;
}

/// Bridge #2: classic completion-handler API on top of an async function.
void fetchQuote(std::string symbol, std::function<void(double)> completion) {
    Task<void>::detached([symbol = std::move(symbol), completion = std::move(completion)]() -> async<void> {
        completion(co_await fetchQuote(symbol));
    });
}

int main() {
    actor<Inventory> inventory;

    // 1. Block the calling thread until an async result is ready.
    std::puts("1) blocking on a future:");
    auto total = runAsFuture([inventory]() -> async<int> {
                     co_await inventory[&Inventory::add]("apples", 3);
                     co_await inventory[&Inventory::add]("pears", 2);
                     co_return co_await inventory[&Inventory::slowCount]();
                 }).get();
    std::printf("   total = %d\n", total);

    // 2. Fan out concurrent work with a task group, wait for it from sync code.
    std::puts("2) task group from sync code:");
    auto sum = runAsFuture([]() -> async<double> {
                   co_return co_await withTaskGroup<double>([](TaskGroup<double>& group) -> async<double> {
                       for (std::string symbol : {"AAPL", "MSFT", "NVIDIA"}) {
                           group.addTask([symbol] { return fetchQuote(symbol); });
                       }
                       double sum = 0;
                       for_await (double quote, group) sum += quote;
                       co_return sum;
                   });
               }).get();
    std::printf("   sum of quotes = %.1f\n", sum);

    // 3. Errors thrown inside async code surface from `future.get()`.
    std::puts("3) error propagation:");
    try {
        runAsFuture([]() -> async<void> {
            co_await Task<>::sleep(5ms);
            throw std::runtime_error("network unreachable");
        }).get();
    } catch (const std::exception& e) {
        std::printf("   caught: %s\n", e.what());
    }

    // 4. Fire-and-forget with a completion callback; the main thread keeps
    //    doing its own work (here: a toy event loop) meanwhile.
    std::puts("4) completion handler + own event loop:");
    std::atomic<bool> done{false};
    fetchQuote("GOOG", [&](double quote) {
        std::printf("   callback on a worker thread: GOOG = %.1f\n", quote);
        done = true;
    });
    int frames = 0;
    while (!done) {
        ++frames;
        std::this_thread::sleep_for(5ms); // "render a frame"
    }
    std::printf("   main loop ran %d frames while waiting\n", frames);

    // 5. Cancellation from sync code.
    std::puts("5) cancellation:");
    auto longTask = Task<void>::detached([]() -> async<void> { co_await Task<>::sleep(10s); });
    longTask.cancel();
    auto cancelled = runAsFuture([longTask]() -> async<bool> {
                         co_return !(co_await longTask.result()).isSuccess();
                     }).get();
    std::printf("   long task cancelled: %s\n", cancelled ? "yes" : "no");

    return 0;
}
