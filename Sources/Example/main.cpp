// A tour of C++ Corcurrency. Compare with the equivalent Swift in the comments.
#include <corcurrency/corcurrency.hpp>

#include <chrono>
#include <cstdio>
#include <string>
#include <vector>

using namespace corcurrency;
using namespace std::chrono_literals;

// actor BankAccount {
//     private(set) var balance: Int
//     func deposit(_ amount: Int) { balance += amount }
//     func withdraw(_ amount: Int) throws -> Int { ... }
// }
class BankAccount {
public:
    explicit BankAccount(int initial) : balance(initial) {}

    int balance;

    void deposit(int amount) { balance += amount; }

    int withdraw(int amount) {
        if (amount > balance) throw std::runtime_error("insufficient funds");
        balance -= amount;
        return balance;
    }

    // func audit() async -> String
    auto audit() -> async<std::string> {
        co_await Task<>::sleep(10ms); // the actor is re-entrant while suspended
        co_return "balance=" + std::to_string(balance);
    }
};

// func fetchPrice(_ symbol: String) async throws -> Double
auto fetchPrice(std::string symbol) -> async<double> {
    co_await Task<>::sleep(std::chrono::milliseconds(20 + symbol.size() * 5));
    co_return static_cast<double>(symbol.size()) * 100.0;
}

// @MainActor func render(_ text: String)
auto render(std::string text) -> async<void> {
    co_await MainActor::isolate();
    std::printf("[main actor] %s\n", text.c_str());
}

TaskLocal<std::string> traceID{"-"};

int main() {
    return asyncMain([]() -> async<int> {
        // let account = BankAccount(initial: 100)
        actor<BankAccount> account(100);

        // await withDiscardingTaskGroup { group in for _ in 0..<10 { group.addTask { await account.deposit(10) } } }
        co_await withDiscardingTaskGroup([account](DiscardingTaskGroup& group) {
            for (int i = 0; i < 10; ++i) {
                group.addTask([account]() -> async<void> { co_await account[&BankAccount::deposit](10); });
            }
        });
        std::printf("balance after deposits: %d\n", co_await account[&BankAccount::balance]);

        try {
            co_await account[&BankAccount::withdraw](1000);
        } catch (const std::exception& e) {
            std::printf("withdraw failed: %s\n", e.what());
        }
        std::printf("audit: %s\n", (co_await account[&BankAccount::audit]()).c_str());

        // async let a = fetchPrice("AAPL"); async let b = fetchPrice("GOOG")
        async_let apple = fetchPrice("AAPL");
        async_let google = fetchPrice("GOOG");
        std::printf("total price: %.1f\n", co_await apple + co_await google);

        // let prices = await withTaskGroup(of: (String, Double).self) { ... }
        auto prices = co_await withTaskGroup<std::pair<std::string, double>>([](auto& group) -> async<std::vector<std::string>> {
            for (std::string symbol : {"MSFT", "NVDA", "TSLA"}) {
                group.addTask([symbol]() -> async<std::pair<std::string, double>> {
                    co_return std::pair{symbol, co_await fetchPrice(symbol)};
                });
            }
            std::vector<std::string> lines;
            for_await (auto entry, group) lines.push_back(entry.first + "=" + std::to_string(entry.second));
            co_return lines;
        });
        for (auto& line : prices) std::printf("  %s\n", line.c_str());

        // let ticks = AsyncStream<Int> { continuation in ... }
        AsyncStream<int> ticks([](AsyncStream<int>::Continuation c) {
            Task<void>::detached([c]() -> async<void> {
                for (int i = 1; i <= 5; ++i) {
                    c.yield(i);
                    co_await Task<>::sleep(5ms);
                }
                c.finish();
            });
        });
        // for await tick in ticks.map({ $0 * $0 }) { ... }
        for_await (int square, ticks.map([](int x) { return x * x; })) {
            std::printf("tick^2 = %d\n", square);
        }

        // let task = Task.detached { try await Task.sleep(for: .seconds(10)) }; task.cancel()
        auto longTask = Task<void>::detached([]() -> async<void> { co_await Task<>::sleep(10s); });
        longTask.cancel();
        auto result = co_await longTask.result();
        std::printf("long task cancelled: %s\n", result.isSuccess() ? "no" : "yes");

        // await $traceID.withValue("req-42") { ... }
        co_await traceID.withValue("req-42", []() -> async<void> {
            Task child([] { return traceID.get(); });
            co_await render("trace id in child task: " + co_await child.value());
        });

        co_return 0;
    });
}
