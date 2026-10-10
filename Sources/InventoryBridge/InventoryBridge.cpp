// C++ side of the Swift example: ordinary Corcurrency code behind a C API.
#include "InventoryBridge.h"

#include <corcurrency/corcurrency.hpp>

#include <chrono>
#include <sstream>
#include <string>

using namespace corcurrency;
using namespace std::chrono_literals;

namespace {

class Inventory {
public:
    int add(const std::string& item, int count) {
        (void)item;
        return total += count;
    }

    int total = 0;
};

auto fetchQuote(std::string symbol) -> async<double> {
    co_await Task<>::sleep(std::chrono::milliseconds(10 * symbol.size()));
    co_return static_cast<double>(symbol.size()) * 10.0;
}

} // namespace

struct cc_inventory {
    actor<Inventory> inventory;
};

struct cc_task {
    Task<void> task;
};

extern "C" {

cc_inventory* cc_inventory_create(void) { return new cc_inventory(); }

void cc_inventory_release(cc_inventory* inventory) { delete inventory; }

void cc_inventory_add(cc_inventory* inventory, const char* item, int count, void* context,
                      void (*done)(void* context, int total)) {
    Task<void>::detached([inventory = inventory->inventory, item = std::string(item), count, context,
                          done]() -> async<void> {
        done(context, co_await inventory[&Inventory::add](item, count));
    });
}

void cc_inventory_total(cc_inventory* inventory, void* context, void (*done)(void* context, int total)) {
    Task<void>::detached([inventory = inventory->inventory, context, done]() -> async<void> {
        done(context, co_await inventory[&Inventory::total]);
    });
}

void cc_sum_quotes(const char* symbols, void* context, void (*done)(void* context, double sum)) {
    Task<void>::detached([symbols = std::string(symbols), context, done]() -> async<void> {
        double sum = co_await withTaskGroup<double>([&](TaskGroup<double>& group) -> async<double> {
            std::istringstream stream(symbols);
            for (std::string symbol; std::getline(stream, symbol, ',');) {
                group.addTask([symbol] { return fetchQuote(symbol); });
            }
            double total = 0;
            for_await (double quote, group) total += quote;
            co_return total;
        });
        done(context, sum);
    });
}

cc_task* cc_start_sleep(int milliseconds, void* context, void (*done)(void* context, bool cancelled)) {
    auto task = Task<void>::detached([milliseconds, context, done]() -> async<void> {
        bool cancelled = false;
        try {
            co_await Task<>::sleep(std::chrono::milliseconds(milliseconds));
        } catch (const CancellationError&) {
            cancelled = true;
        }
        done(context, cancelled);
    });
    return new cc_task{task};
}

void cc_task_cancel(cc_task* task) { task->task.cancel(); }

void cc_task_release(cc_task* task) { delete task; }

} // extern "C"
