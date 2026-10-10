# C++ Corcurrency

**Coroutines + Concurrency**: Swift Concurrency for C++20, built on libdispatch.

The goal is to write C++ that reads and behaves like Swift concurrency code: actors, `Task`, task groups,
`async let`, `AsyncStream`, `@MainActor`, `@TaskLocal`, cancellation. Swift is used only for its build
system (SwiftPM) and for the libdispatch that ships with the toolchain. There is no Swift interop: the
library is pure C++.

```cpp
#include <corcurrency/corcurrency.hpp>
using namespace corcurrency;

class Counter {                       // a plain C++ class...
public:
    int value = 0;
    int increment(int by) { return value += by; }
};

auto work() -> async<int> {           // func work() async -> Int
    actor<Counter> counter;           // ...isolated by an actor
    co_await counter[&Counter::increment](5);         // await counter.increment(by: 5)
    co_return co_await counter[&Counter::value];      // await counter.value
}

int main() {
    return asyncMain([]() -> async<int> {             // @main static func main() async
        Task task(work);                              // Task { await work() }
        co_return co_await task.value() == 5 ? 0 : 1; // await task.value
    });
}
```

## Swift → C++ cheat sheet

| Swift | C++ Corcurrency |
|---|---|
| `func f() async throws -> T` | `auto f() -> async<T>` (errors are exceptions) |
| `await f()` / `return x` | `co_await f()` / `co_return x` |
| `actor A { ... }` | `class A { ... };` + `actor<A> a(args...);` |
| `await a.method(x)` | `co_await a[&A::method](x)` |
| `await a.property` | `co_await a[&A::property]` |
| `nonisolated` members | `a.nonisolated().member` |
| `self` inside an actor | derive from `enable_actor_from_this<A>`, use `self()` |
| `a.assumeIsolated { }` / `preconditionIsolated()` | same names |
| custom `unownedExecutor` | `SerialExecutor& unownedExecutor()` member on `A` |
| `Task { }` / `Task(priority:) { }` / `Task(name:priority:)` | `Task(fn)` / `Task(TaskPriority::high, fn)` / `Task("name", p, fn)` |
| `Task.detached { }` | `Task<T>::detached(fn)` |
| `Task.immediate { }` | `Task<T>::immediate(fn)` |
| `await task.value` / `task.result` | `co_await task.value()` / `co_await task.result()` |
| `task.cancel()` / `task.isCancelled` | `task.cancel()` / `task.isCancelled()` |
| `Task.isCancelled`, `Task.checkCancellation()` | `Task<>::isCancelled()`, `Task<>::checkCancellation()` |
| `Task.sleep(for:)`, `Task.yield()` | `co_await Task<>::sleep(100ms)`, `co_await Task<>::yield()` |
| `Task.currentPriority` | `Task<>::currentPriority()` |
| `withTaskCancellationHandler` | `withTaskCancellationHandler(op, onCancel)` |
| `withTaskGroup(of:)` / `withThrowingTaskGroup(of:)` | `withTaskGroup<T>(body)` / `withThrowingTaskGroup<T>(body)` |
| `withDiscardingTaskGroup` / throwing | `withDiscardingTaskGroup(body)` / `withThrowingDiscardingTaskGroup(body)` |
| `group.addTask`, `next()`, `cancelAll()`, `waitForAll()` | same names |
| `async let x = f()` | `async_let x = f();` then `co_await x` |
| `for await x in seq { }` | `for_await (auto x, seq) { }` |
| `AsyncStream` / `AsyncThrowingStream`, `makeStream` | same names |
| `.map`, `.filter`, `.compactMap`, `.prefix`, `.dropFirst`, `.reduce`, `.first(where:)` | same names (`prefixWhile`, `reduceInto`) |
| `withCheckedContinuation` / `withCheckedThrowingContinuation` | same, `c.resume(v)`, `c.resumeThrowing(e)` |
| `withUnsafeContinuation` | same |
| `@MainActor func f()` | first statement `co_await MainActor::isolate();` |
| `await MainActor.run { }` | `co_await MainActor::run(fn)` |
| `@globalActor actor DB` | `struct DB : GlobalActor<DB> {};` |
| `@concurrent func` | `co_await concurrent(fn)` |
| `@TaskLocal static var id` | `inline TaskLocal<T> id{def};` + `id.withValue(v, fn)`, `id.get()` |
| `withTaskExecutorPreference` | same, with a `TaskExecutor&` |
| `SerialExecutor`, `TaskExecutor`, `ExecutorJob` | same names, override `enqueue(ExecutorJob*)` |
| `Sendable` | `Sendable` concept / `is_sendable<T>` trait (opt-out, not enforced) |

## Execution model

* `async<T>` is lazy, like a Swift async function: it starts when awaited, inline, on the caller's task and
  executor, and resumes the caller back on the caller's executor. A non-isolated async function therefore
  runs on its caller's actor, matching Swift 6.2 `nonisolated(nonsending)` semantics. To leave the actor,
  use `concurrent(...)`.
* Each `actor<T>` owns a serial dispatch queue. Every `actor[&T::member]` call hops onto it, and the
  caller resumes on its own executor afterwards. Actors are re-entrant at suspension points, as in Swift.
* `Task { }` inherits the creating context's executor (actor isolation), priority and task-locals.
  `Task::detached` inherits none of them. Children of task groups and `async let` run on the global
  concurrent executor and are cancelled together with their parent.
* `MainActor` is the main dispatch queue, drained on the main thread. On Windows and Linux, `asyncMain`
  runs your async entry point on it and services the queue from the main thread through libdispatch's
  run-loop hooks (`_dispatch_get_main_queue_handle_4CF` / `_dispatch_main_queue_callback_4CF`, the same
  ones Foundation's `RunLoop` uses). Plain `dispatch_main()` would hand the main queue to worker
  threads. Other platforms fall back to `dispatch_main()`; this path isn't verified yet.
* `TaskPriority` maps onto dispatch QoS global queues.

### Differences from Swift

* The compiler doesn't check data races: there is no `Sendable` checking or isolation inference. Actor
  state is protected only if you access it through `actor[...]`.
* An `async_let` that goes out of scope without being awaited is cancelled, but not awaited (C++
  destructors cannot suspend). Always await what you start.
* Arguments of an actor call are copied into the call, like Swift's `Sendable` values.
* `TaskGroup` and `ThrowingTaskGroup` are the same type, because every C++ function can throw.

## Building

Requires a Swift 5.9+ toolchain (for SwiftPM and libdispatch) and a C++20 compiler (the toolchain's clang).

```sh
swift build
swift run CorcurrencyTests   # test suite
swift run Example            # guided tour (asyncMain + MainActor)
swift run ThreadsDemo        # which thread / executor every step runs on
swift run SyncExample        # embedding in plain synchronous C++ (no asyncMain)
swift run SwiftExample       # Swift concurrency driving Corcurrency C++ code through a C API
```

To use it from another SwiftPM package, add a dependency on this package and depend on the `Corcurrency`
library product from a C++ target. On Windows, SwiftPM links `swiftrt.obj` into every executable, so
pure C++ executable targets also need `linkerSettings: [.linkedLibrary("swiftCore", .when(platforms: [.windows]))]`.

### Using it from Swift

`Sources/InventoryBridge` is a C++ component written with Corcurrency (an actor, tasks and a task group)
behind a small C API with completion callbacks. `Sources/SwiftExample` calls that API from native Swift
concurrency. It turns each callback into an `async` function with `withCheckedContinuation`, and forwards
Swift task cancellation to `Task::cancel()` through `withTaskCancellationHandler`. Swift never imports
Corcurrency's headers: they're C++20 coroutine templates, and the C boundary keeps the two runtimes
independent.

The public headers don't include `<dispatch/dispatch.h>`; only `runtime.cpp` talks to libdispatch. If the
header isn't on the include path (SwiftPM doesn't add the Swift SDK's include directory for C++ targets),
the runtime declares the few libdispatch C functions it uses itself.

## License

MIT. See [LICENSE](LICENSE).
