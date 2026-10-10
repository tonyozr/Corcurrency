// C++ Corcurrency — bridging callback APIs: checked / unsafe continuations.
//
//     int v = co_await withCheckedContinuation<int>([](CheckedContinuation<int> c) {
//         legacyFetch([c](int result) { c.resume(result); });
//     });
//
//     auto data = co_await withCheckedThrowingContinuation<std::string>([](auto c) {
//         download([c](std::string s, std::exception_ptr e) {
//             if (e) c.resumeThrowing(e); else c.resume(std::move(s));
//         });
//     });
#pragma once

#include <corcurrency/task.hpp>

#include <cstdio>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <optional>

namespace corcurrency {

namespace detail {

template <class T>
struct ContinuationState {
    using Stored = std::conditional_t<std::is_void_v<T>, std::monostate, T>;

    std::mutex mutex;
    bool resumed = false;
    bool checked = true;
    std::optional<Result<T>> result;
    std::coroutine_handle<> handle;
    Executor* executor = nullptr;
    TaskRef task;
    /// Set while resumed-but-not-yet-consumed so the awaiter can read the result
    /// even after every continuation copy is gone.
    std::shared_ptr<ContinuationState> keepAlive;

    ~ContinuationState() {
        if (checked && !resumed) {
            std::fputs("SWIFT TASK CONTINUATION MISUSE: leaked its continuation without resuming it. "
                       "This may cause tasks waiting on it to remain suspended forever.\n",
                       stderr);
        }
    }

    void complete(const std::shared_ptr<ContinuationState>& self, Result<T> r) {
        {
            std::lock_guard lock(mutex);
            if (resumed) {
                std::fputs("SWIFT TASK CONTINUATION MISUSE: tried to resume its continuation more than once\n",
                           stderr);
                std::abort();
            }
            resumed = true;
            result.emplace(std::move(r));
            keepAlive = self;
        }
        schedule(executor, handle, task.get());
    }
};

template <class T, class Continuation, class Body>
struct ContinuationAwaiter {
    Body& body;
    bool checked;
    ContinuationState<T>* state = nullptr;

    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) {
        auto& ctx = currentContext();
        auto st = std::make_shared<ContinuationState<T>>();
        st->checked = checked;
        st->handle = h;
        st->executor = ctx.executor;
        st->task = TaskRef(ctx.task);
        state = st.get();
        // `body` may resume synchronously; after this call `this` may be gone.
        body(Continuation(std::move(st)));
    }
    T await_resume() {
        auto keep = std::move(state->keepAlive);
        return keep->result->get();
    }
};

} // namespace detail

/// Swift's `CheckedContinuation<T, Error>`. Copyable; must be resumed exactly once.
/// Double resumption traps; dropping every copy without resuming logs a warning.
template <class T>
class CheckedContinuation {
public:
    explicit CheckedContinuation(std::shared_ptr<detail::ContinuationState<T>> state) : state_(std::move(state)) {}

    /// `resume(returning:)`
    template <class U = T>
        requires(!std::is_void_v<T> && std::is_convertible_v<U &&, T>)
    void resume(U&& value) const {
        state_->complete(state_, Result<T>::success(T(std::forward<U>(value))));
    }
    /// `resume()` for `Void` continuations.
    void resume() const
        requires std::is_void_v<T>
    {
        state_->complete(state_, Result<T>::success());
    }
    /// `resume(throwing:)`
    void resumeThrowing(std::exception_ptr error) const { state_->complete(state_, Result<T>::failure(std::move(error))); }
    template <class E>
        requires std::is_base_of_v<std::exception, std::remove_cvref_t<E>>
    void resumeThrowing(E&& error) const {
        resumeThrowing(std::make_exception_ptr(std::forward<E>(error)));
    }
    /// `resume(with: result)`
    void resumeWith(Result<T> result) const { state_->complete(state_, std::move(result)); }

private:
    std::shared_ptr<detail::ContinuationState<T>> state_;
};

/// Swift's `UnsafeContinuation` — same API, misuse is not diagnosed.
template <class T>
class UnsafeContinuation : public CheckedContinuation<T> {
public:
    using CheckedContinuation<T>::CheckedContinuation;
};

/// `await withCheckedContinuation { continuation in ... }`
template <class T = void, class Body>
auto withCheckedContinuation(Body body) -> async<T> {
    co_return co_await detail::ContinuationAwaiter<T, CheckedContinuation<T>, Body>{body, true};
}

/// `try await withCheckedThrowingContinuation { continuation in ... }`
template <class T = void, class Body>
auto withCheckedThrowingContinuation(Body body) -> async<T> {
    co_return co_await detail::ContinuationAwaiter<T, CheckedContinuation<T>, Body>{body, true};
}

/// `await withUnsafeContinuation { continuation in ... }`
template <class T = void, class Body>
auto withUnsafeContinuation(Body body) -> async<T> {
    co_return co_await detail::ContinuationAwaiter<T, UnsafeContinuation<T>, Body>{body, false};
}

/// `try await withUnsafeThrowingContinuation { continuation in ... }`
template <class T = void, class Body>
auto withUnsafeThrowingContinuation(Body body) -> async<T> {
    co_return co_await detail::ContinuationAwaiter<T, UnsafeContinuation<T>, Body>{body, false};
}

} // namespace corcurrency
