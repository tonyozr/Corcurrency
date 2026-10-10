// C++ Corcurrency — `async<T>`: the return type of an async function.
//
//     auto fetchUser(int id) -> async<User> {
//         auto data = co_await download(id);
//         co_return User(data);
//     }
//
// Like a Swift async function, an `async<T>` is lazy: its body starts running
// only when it is awaited, inline, in the caller's task and on the caller's
// executor. When it completes it resumes the caller back on the caller's
// executor — so an actor-isolated caller stays isolated across `co_await`.
#pragma once

#include <corcurrency/runtime.hpp>

#include <cassert>
#include <exception>
#include <functional>
#include <optional>
#include <type_traits>
#include <utility>

namespace corcurrency {

template <class T = void>
class async;

namespace detail {

template <class T> struct is_async : std::false_type {};
template <class T> struct is_async<async<T>> : std::true_type {};
template <class T> inline constexpr bool is_async_v = is_async<std::remove_cvref_t<T>>::value;

template <class R> struct unwrap_async { using type = R; };
template <class T> struct unwrap_async<async<T>> { using type = T; };

/// The value type produced by calling `F` with `Args`, looking through `async<T>`.
template <class F, class... Args>
using async_result_t = typename unwrap_async<std::remove_cvref_t<std::invoke_result_t<F, Args...>>>::type;

/// Returns the continuation to run after a coroutine finishes, hopping back to
/// the awaiting coroutine's executor when it differs from the current one.
inline std::coroutine_handle<> continuationFor(std::coroutine_handle<> continuation,
                                               Executor* resumeExecutor) noexcept {
    if (!continuation) return std::noop_coroutine();
    auto& ctx = currentContext();
    if (resumeExecutor == nullptr || resumeExecutor == ctx.executor) return continuation;
    schedule(resumeExecutor, continuation, ctx.task);
    return std::noop_coroutine();
}

struct PromiseBase {
    std::coroutine_handle<> continuation;
    Executor* resumeExecutor = nullptr;
    std::exception_ptr error;

    std::suspend_always initial_suspend() noexcept { return {}; }

    struct FinalAwaiter {
        bool await_ready() noexcept { return false; }
        template <class P>
        std::coroutine_handle<> await_suspend(std::coroutine_handle<P> h) noexcept {
            auto& p = h.promise();
            return continuationFor(p.continuation, p.resumeExecutor);
        }
        void await_resume() noexcept {}
    };
    FinalAwaiter final_suspend() noexcept { return {}; }

    void unhandled_exception() noexcept { error = std::current_exception(); }
};

template <class T>
struct Promise : PromiseBase {
    std::optional<T> value;

    async<T> get_return_object() noexcept;

    template <class U = T>
        requires std::is_convertible_v<U&&, T>
    void return_value(U&& v) noexcept(std::is_nothrow_constructible_v<T, U&&>) {
        value.emplace(std::forward<U>(v));
    }

    T take() {
        if (error) std::rethrow_exception(error);
        return std::move(*value);
    }
};

template <>
struct Promise<void> : PromiseBase {
    async<void> get_return_object() noexcept;
    void return_void() noexcept {}
    void take() {
        if (error) std::rethrow_exception(error);
    }
};

} // namespace detail

/// The result of an async function. Await it with `co_await`.
template <class T>
class [[nodiscard]] async {
    static_assert(!std::is_reference_v<T>, "async<T&> is not supported; return a pointer or value");

public:
    using promise_type = detail::Promise<T>;
    using value_type = T;
    using handle_type = std::coroutine_handle<promise_type>;

    explicit async(handle_type h) noexcept : h_(h) {}
    async(async&& o) noexcept : h_(std::exchange(o.h_, {})) {}
    async& operator=(async&& o) noexcept {
        if (this != &o) {
            if (h_) h_.destroy();
            h_ = std::exchange(o.h_, {});
        }
        return *this;
    }
    async(const async&) = delete;
    async& operator=(const async&) = delete;
    ~async() {
        if (h_) h_.destroy();
    }

    struct Awaiter {
        handle_type h;
        bool await_ready() const noexcept { return false; }
        std::coroutine_handle<> await_suspend(std::coroutine_handle<> caller) noexcept {
            auto& p = h.promise();
            p.continuation = caller;
            p.resumeExecutor = detail::currentContext().executor;
            return h; // start the callee inline (symmetric transfer)
        }
        T await_resume() { return h.promise().take(); }
    };

    Awaiter operator co_await() && noexcept {
        assert(h_ && "awaiting an empty or already awaited async<T>");
        return Awaiter{h_};
    }
    Awaiter operator co_await() & noexcept { return std::move(*this).operator co_await(); }

    explicit operator bool() const noexcept { return static_cast<bool>(h_); }

private:
    handle_type h_;
};

namespace detail {

template <class T>
async<T> Promise<T>::get_return_object() noexcept {
    return async<T>(std::coroutine_handle<Promise<T>>::from_promise(*this));
}
inline async<void> Promise<void>::get_return_object() noexcept {
    return async<void>(std::coroutine_handle<Promise<void>>::from_promise(*this));
}

/// Invokes `f(args...)` and adapts the result to `async<R>` (sync callables
/// become trivially-async). Mirrors Swift accepting sync closures where async
/// ones are expected.
template <class F, class... Args>
auto invokeAsync(F f, Args... args) -> async<async_result_t<F&, Args&&...>> {
    using Raw = std::remove_cvref_t<std::invoke_result_t<F&, Args&&...>>;
    using R = async_result_t<F&, Args&&...>;
    if constexpr (is_async<Raw>::value) {
        if constexpr (std::is_void_v<R>) {
            co_await std::invoke(f, std::move(args)...);
        } else {
            co_return co_await std::invoke(f, std::move(args)...);
        }
    } else if constexpr (std::is_void_v<R>) {
        std::invoke(f, std::move(args)...);
        co_return;
    } else {
        co_return std::invoke(f, std::move(args)...);
    }
}

/// Like `invokeAsync`, but passes `f` and `args` by reference (they must
/// outlive the returned async, i.e. it must be awaited immediately).
template <class F, class... Args>
auto callAsync(F& f, Args&... args) -> async<async_result_t<F&, Args&...>> {
    using Raw = std::remove_cvref_t<std::invoke_result_t<F&, Args&...>>;
    using R = async_result_t<F&, Args&...>;
    if constexpr (is_async<Raw>::value) {
        if constexpr (std::is_void_v<R>) {
            co_await std::invoke(f, args...);
        } else {
            co_return co_await std::invoke(f, args...);
        }
    } else if constexpr (std::is_void_v<R>) {
        std::invoke(f, args...);
        co_return;
    } else {
        co_return std::invoke(f, args...);
    }
}

/// Moves execution of the current coroutine onto `target` (no-op if already there).
struct HopToExecutor {
    Executor* target;
    bool await_ready() const noexcept { return currentContext().executor == target; }
    void await_suspend(std::coroutine_handle<> h) const {
        schedule(target, h, currentContext().task);
    }
    void await_resume() const noexcept {}
};

} // namespace detail

/// `co_await hopTo(executor)` — continue the current async function on `executor`.
/// When the function returns, its caller resumes on the caller's own executor.
inline detail::HopToExecutor hopTo(Executor& executor) noexcept { return {&executor}; }

/// Swift's `@concurrent` (SE-0461): run `fn` on the global concurrent executor,
/// off the caller's actor, and come back with the result.
template <class F, class... Args>
auto concurrent(F fn, Args... args) -> async<detail::async_result_t<F&, Args&&...>> {
    co_await hopTo(globalConcurrentExecutor());
    co_return co_await detail::invokeAsync(std::move(fn), std::move(args)...);
}

} // namespace corcurrency
