// A C API over a Corcurrency-based C++ component, so Swift (or anything that
// speaks C) can use it. Asynchronous operations report back through completion
// callbacks invoked on a Corcurrency worker thread; Swift wraps them with
// `withCheckedContinuation`.
#pragma once

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/// An `actor<Inventory>` living on the C++ side.
typedef struct cc_inventory cc_inventory;

cc_inventory* cc_inventory_create(void);
void cc_inventory_release(cc_inventory* inventory);

/// `await inventory.add(item, count)` — `done(context, newTotal)`.
void cc_inventory_add(cc_inventory* inventory, const char* item, int count, void* context,
                      void (*done)(void* context, int total));

/// `await inventory.total` — `done(context, total)`.
void cc_inventory_total(cc_inventory* inventory, void* context, void (*done)(void* context, int total));

/// Fetches quotes for comma-separated `symbols` concurrently in a C++ task group
/// and reports their sum — `done(context, sum)`.
void cc_sum_quotes(const char* symbols, void* context, void (*done)(void* context, double sum));

/// A long-running C++ task that can be cancelled.
typedef struct cc_task cc_task;

/// Sleeps for `milliseconds` in a C++ `Task`; `done(context, cancelled)`.
cc_task* cc_start_sleep(int milliseconds, void* context, void (*done)(void* context, bool cancelled));
void cc_task_cancel(cc_task* task);
void cc_task_release(cc_task* task);

#ifdef __cplusplus
}
#endif
