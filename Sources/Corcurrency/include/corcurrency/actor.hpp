// C++ Corcurrency — actors.
//
//     class Counter {                      // a plain C++ class...
//     public:
//         int value = 0;
//         int increment(int by) { return value += by; }
//         auto slowIncrement() -> async<int> { co_await Task<>::sleep(1ms); co_return ++value; }
//     };
//
//     actor<Counter> counter;                                  // ...becomes an actor
//     int v  = co_await counter[&Counter::increment](5);       // await counter.increment(by: 5)
//     int v2 = co_await counter[&Counter::slowIncrement]();
//     int x  = co_await counter[&Counter::value];              // await counter.value
//
// `actor<T>` has reference semantics (like a Swift actor): copies share the same
// instance. Every access from outside goes through `operator[]` and is awaited;
// the member runs isolated on the actor's serial executor. Async members are
// re-entrant at suspension points, exactly like Swift actors.
#pragma once

#include <corcurrency/task.hpp>

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <type_traits>
#include <typeinfo>

namespace corcurrency {

template <class T>
class actor;

/// Opt-in base giving an actor's state access to its own actor reference
/// (Swift's `self` inside an actor), e.g. to pass it along or call through it.
template <class T>
class enable_actor_from_this {
public:
    /// The actor that owns this object.
    actor<T> self() const;

private:
    template <class>
    friend class actor;
    mutable std::weak_ptr<void> weakStorage_;
};

namespace detail {

template <class T, class = void>
struct has_custom_executor : std::false_type {};
template <class T>
struct has_custom_executor<T, std::void_t<decltype(std::declval<T&>().unownedExecutor())>> : std::true_type {};

} // namespace detail

/// Swift's `actor`: wraps a class so that all access to it is serialized on a
/// dedicated serial executor (a serial dispatch queue).
///
/// To use a custom `SerialExecutor`, give `T` a member
/// `SerialExecutor& unownedExecutor()` (Swift's `unownedExecutor`).
template <class T>
class actor {
    struct Storage {
        template <class... Args>
        explicit Storage(Args&&... args) : object(std::forward<Args>(args)...) {}

        SerialExecutor& executor() {
            if constexpr (detail::has_custom_executor<T>::value) {
                return object.unownedExecutor();
            } else {
                return defaultExecutor;
            }
        }

        DispatchSerialQueueExecutor defaultExecutor{typeid(T).name()};
        T object;
    };

    template <class... Args>
    static constexpr bool isCopyCtorLike =
        sizeof...(Args) == 1 && (std::is_same_v<std::remove_cvref_t<Args>, actor> && ...);

public:
    using isolated_type = T;

    /// Creates a new actor instance, forwarding `args` to `T`'s constructor.
    template <class... Args>
        requires(!isCopyCtorLike<Args...> && std::is_constructible_v<T, Args && ...>)
    explicit actor(Args&&... args) : storage_(std::make_shared<Storage>(std::forward<Args>(args)...)) {
        if constexpr (std::is_base_of_v<enable_actor_from_this<T>, T>) {
            static_cast<const enable_actor_from_this<T>&>(storage_->object).weakStorage_ = storage_;
        }
    }

    actor(const actor&) = default;
    actor(actor&&) noexcept = default;
    actor& operator=(const actor&) = default;
    actor& operator=(actor&&) noexcept = default;

    // MARK: Isolated access

    /// `actor[&T::method](args...)` — returns an awaitable call of `method`
    /// that runs isolated on this actor. Arguments are copied (like Sendable values).
    template <class Method>
        requires std::is_member_function_pointer_v<Method>
    auto operator[](Method method) const {
        return [storage = storage_, method](auto&&... args) {
            return call(storage, method, std::decay_t<decltype(args)>(std::forward<decltype(args)>(args))...);
        };
    }

    /// `co_await actor[&T::property]` — reads a stored property on the actor.
    template <class R>
        requires std::is_member_object_pointer_v<R T::*>
    async<std::remove_cvref_t<R>> operator[](R T::* property) const {
        return read(storage_, property);
    }

    /// Runs `body(T&)` isolated on this actor (like a Swift function taking an
    /// `isolated` actor parameter). `body` may be sync or async.
    template <class F>
    auto run(F body) const -> async<detail::async_result_t<F&, T&>> {
        auto storage = storage_;
        co_await hopTo(storage->executor());
        co_return co_await detail::callAsync(body, storage->object);
    }

    // MARK: Non-isolated access

    /// Access for `nonisolated` members (immutable state, thread-safe members).
    /// Swift: calling a `nonisolated` member does not require `await`.
    const T& nonisolated() const noexcept { return storage_->object; }

    /// Swift's `unownedExecutor`.
    SerialExecutor& unownedExecutor() const noexcept { return storage_->executor(); }

    /// `actor.assumeIsolated { $0 ... }` — synchronously access isolated state
    /// when the caller knows it is already running on this actor. Traps otherwise.
    template <class F>
    decltype(auto) assumeIsolated(F&& body) const {
        preconditionIsolated();
        return std::forward<F>(body)(storage_->object);
    }

    /// `assertIsolated()` — debug-only check.
    void assertIsolated() const noexcept { assert(storage_->executor().isIsolatingCurrentContext()); }

    /// `preconditionIsolated()` — checked in all builds.
    void preconditionIsolated() const noexcept {
        if (!storage_->executor().isIsolatingCurrentContext()) {
            std::fputs("Corcurrency: incorrect actor executor assumption\n", stderr);
            std::abort();
        }
    }

    friend bool operator==(const actor& a, const actor& b) noexcept { return a.storage_ == b.storage_; }

private:
    template <class>
    friend class enable_actor_from_this;

    explicit actor(std::shared_ptr<Storage> storage) noexcept : storage_(std::move(storage)) {}

    template <class Method, class... Args>
    static auto call(std::shared_ptr<Storage> storage, Method method, Args... args)
        -> async<detail::async_result_t<Method, T&, Args&&...>> {
        co_await hopTo(storage->executor());
        co_return co_await detail::invokeAsync(method, std::ref(storage->object), std::move(args)...);
    }

    template <class R>
    static auto read(std::shared_ptr<Storage> storage, R T::* property) -> async<std::remove_cvref_t<R>> {
        co_await hopTo(storage->executor());
        co_return storage->object.*property;
    }

    std::shared_ptr<Storage> storage_;
};

template <class T>
actor<T> enable_actor_from_this<T>::self() const {
    auto storage = weakStorage_.lock();
    assert(storage && "self() called on an object not owned by an actor");
    return actor<T>(std::static_pointer_cast<typename actor<T>::Storage>(storage));
}

// MARK: - Global actors

/// Swift's `@globalActor`. Declare one with
///
///     struct DatabaseActor : GlobalActor<DatabaseActor> {};
///
/// and isolate code to it with `co_await DatabaseActor::run(fn)` or by starting
/// an async function with `co_await DatabaseActor::isolate();` (the equivalent
/// of annotating the function `@DatabaseActor`). A derived type may provide its
/// own `static SerialExecutor& sharedExecutor()`.
template <class Derived>
struct GlobalActor {
    static SerialExecutor& sharedExecutor() {
        static SerialExecutor& executor = detail::globalActorExecutor(typeid(Derived));
        return executor;
    }

    /// `await Derived.run { ... }`
    template <class F>
    static auto run(F body) -> async<detail::async_result_t<F&>> {
        co_await hopTo(Derived::sharedExecutor());
        co_return co_await detail::invokeAsync(std::move(body));
    }

    /// `co_await Derived::isolate();` — the rest of the current async function
    /// runs on this global actor; its caller resumes on its own executor.
    static detail::HopToExecutor isolate() { return {&Derived::sharedExecutor()}; }

    /// `Derived.assumeIsolated { ... }`
    template <class F>
    static decltype(auto) assumeIsolated(F&& body) {
        preconditionIsolated();
        return std::forward<F>(body)();
    }

    static bool isIsolatingCurrentContext() noexcept {
        return Derived::sharedExecutor().isIsolatingCurrentContext();
    }

    static void assertIsolated() noexcept { assert(isIsolatingCurrentContext()); }

    static void preconditionIsolated() noexcept {
        if (!isIsolatingCurrentContext()) {
            std::fputs("Corcurrency: incorrect global actor executor assumption\n", stderr);
            std::abort();
        }
    }
};

/// Swift's `@MainActor`, backed by the main dispatch queue.
struct MainActor : GlobalActor<MainActor> {
    static SerialExecutor& sharedExecutor() { return mainExecutor(); }

    /// Code running directly on the main thread (e.g. a main-queue callback)
    /// counts as main-actor isolated.
    static bool isIsolatingCurrentContext() noexcept {
        return mainExecutor().isIsolatingCurrentContext() ||
               (detail::currentContext().executor == nullptr && detail::isMainThread());
    }

    template <class F>
    static decltype(auto) assumeIsolated(F&& body) {
        if (!isIsolatingCurrentContext()) {
            std::fputs("Corcurrency: MainActor.assumeIsolated called off the main actor\n", stderr);
            std::abort();
        }
        return std::forward<F>(body)();
    }
};

// MARK: - async main

/// Swift's `@main ... static func main() async`. Runs `body` as a task on the
/// main actor, drains the main queue, and exits the process when it finishes:
///
///     int main() {
///         return asyncMain([]() -> async<int> { ...; co_return 0; });
///     }
///
/// `body` may return `async<void>` (exit code 0) or `async<int>`.
/// An uncaught exception terminates with exit code 1.
template <class F>
[[noreturn]] int asyncMain(F body) {
    detail::TaskSpawnOptions options;
    options.executor = &mainExecutor();
    options.priority = TaskPriority::userInitiated;
    detail::spawnTask<void>(
        [body = std::move(body)]() mutable -> async<void> {
            int code = 0;
            try {
                if constexpr (std::is_same_v<detail::async_result_t<F&>, int>) {
                    code = co_await detail::invokeAsync(std::move(body));
                } else {
                    co_await detail::invokeAsync(std::move(body));
                }
            } catch (const std::exception& e) {
                std::fprintf(stderr, "Fatal error: uncaught exception: %s\n", e.what());
                code = 1;
            } catch (...) {
                std::fputs("Fatal error: uncaught exception\n", stderr);
                code = 1;
            }
            std::fflush(stdout);
            std::exit(code);
        },
        std::move(options));
    detail::dispatchMain();
}

} // namespace corcurrency
