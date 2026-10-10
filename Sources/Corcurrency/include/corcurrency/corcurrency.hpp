// C++ Corcurrency — Swift Concurrency for C++20 on top of libdispatch.
//
//     #include <corcurrency/corcurrency.hpp>
//     using namespace corcurrency;
//
// Swift                                   C++ Corcurrency
// -----------------------------------     ------------------------------------------------
// func f() async -> Int                   auto f() -> async<int>
// await f() / return x                    co_await f() / co_return x
// actor Counter { ... }                   class Counter { ... };  actor<Counter> counter;
// await counter.increment(by: 1)          co_await counter[&Counter::increment](1)
// await counter.value                     co_await counter[&Counter::value]
// Task { ... }                            Task([]() -> async<T> { ... })
// Task.detached(priority: .high) { }      Task<T>::detached(TaskPriority::high, ...)
// try await Task.sleep(for: .seconds(1))  co_await Task<>::sleep(1s)
// withTaskGroup(of: Int.self) { ... }     withTaskGroup<int>([](TaskGroup<int>& g) -> async<...> { ... })
// async let x = f()                       async_let x = f();  ... co_await x
// for await x in seq { }                  for_await (auto x, seq) { }
// @MainActor / MainActor.run { }          co_await MainActor::isolate(); / MainActor::run(...)
// @TaskLocal static var id                inline TaskLocal<T> id;  id.withValue(v, ...)
// withCheckedContinuation { c in }        withCheckedContinuation<T>([](CheckedContinuation<T> c) { })
#pragma once

#include <corcurrency/runtime.hpp>
#include <corcurrency/async.hpp>
#include <corcurrency/task.hpp>
#include <corcurrency/actor.hpp>
#include <corcurrency/task_group.hpp>
#include <corcurrency/continuation.hpp>
#include <corcurrency/async_sequence.hpp>
#include <corcurrency/task_local.hpp>

#include <chrono>
#include <type_traits>

namespace corcurrency {

/// Swift's `Sendable` marker. C++ cannot check data-race safety, so this is an
/// opt-out trait for documentation and constraints: raw pointers are not
/// sendable by default; specialize `is_sendable` for `@unchecked Sendable` types.
template <class T>
struct is_sendable : std::bool_constant<!std::is_pointer_v<T>> {};
template <class T>
concept Sendable = is_sendable<std::remove_cvref_t<T>>::value;

/// Swift's clocks and `Duration`, mapped onto <chrono>.
using ContinuousClock = std::chrono::steady_clock;
using SuspendingClock = std::chrono::steady_clock;
using Duration = std::chrono::nanoseconds;

} // namespace corcurrency
