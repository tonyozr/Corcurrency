// C++ Corcurrency — AsyncSequence, `for_await`, AsyncStream, AsyncThrowingStream.
//
//     AsyncStream<int> numbers([](AsyncStream<int>::Continuation c) {
//         for (int i = 0; i < 3; ++i) c.yield(i);
//         c.finish();
//     });
//
//     for_await (int n, numbers.map([](int x) { return x * 10; })) {   // for await n in numbers.map { $0 * 10 }
//         std::println("{}", n);
//     }
//
// An async sequence is any type with `makeAsyncIterator()` whose iterator has a
// `next()` that is awaitable and yields `std::optional<Element>` (`nullopt` = end).
#pragma once

#include <corcurrency/task.hpp>

#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

// MARK: - for await

#define CORCURRENCY_CONCAT_IMPL(a, b) a##b
#define CORCURRENCY_CONCAT(a, b) CORCURRENCY_CONCAT_IMPL(a, b)

/// `for_await (decl, sequence) { ... }` — Swift's `for await decl in sequence { ... }`.
/// `break` and `continue` work as in a normal loop. Errors thrown by the
/// sequence (AsyncThrowingStream) propagate like `for try await`.
#define for_await(decl, ...)                                                                              \
    for (auto&& CORCURRENCY_CONCAT(corcurrency_it_, __LINE__) = ::corcurrency::makeAsyncIterator(__VA_ARGS__); \
         auto CORCURRENCY_CONCAT(corcurrency_el_, __LINE__) = co_await CORCURRENCY_CONCAT(corcurrency_it_, __LINE__).next();) \
        if (decl = std::move(*CORCURRENCY_CONCAT(corcurrency_el_, __LINE__)); false) {                     \
        } else

namespace corcurrency {

/// Calls `sequence.makeAsyncIterator()`.
template <class Sequence>
auto makeAsyncIterator(Sequence&& sequence) {
    return sequence.makeAsyncIterator();
}

/// The element type of an async sequence.
template <class Sequence>
using AsyncElement = typename std::remove_cvref_t<Sequence>::Element;

template <class Base, class F>
class AsyncMapSequence;
template <class Base, class F>
class AsyncFilterSequence;
template <class Base, class F>
class AsyncCompactMapSequence;
template <class Base>
class AsyncPrefixSequence;
template <class Base>
class AsyncDropFirstSequence;
template <class Base, class F>
class AsyncPrefixWhileSequence;

namespace detail {

template <class Sequence, class R, class F>
auto reduce(Sequence sequence, R initial, F f) -> async<R> {
    auto it = sequence.makeAsyncIterator();
    while (auto element = co_await it.next()) initial = co_await callAsync(f, initial, *element);
    co_return initial;
}

template <class Sequence, class R, class F>
auto reduceInto(Sequence sequence, R initial, F f) -> async<R> {
    auto it = sequence.makeAsyncIterator();
    while (auto element = co_await it.next()) co_await callAsync(f, initial, *element);
    co_return initial;
}

template <class Sequence, class F>
auto first(Sequence sequence, F predicate) -> async<std::optional<AsyncElement<Sequence>>> {
    auto it = sequence.makeAsyncIterator();
    while (auto element = co_await it.next()) {
        if (co_await callAsync(predicate, *element)) co_return std::move(*element);
    }
    co_return std::nullopt;
}

template <class Sequence>
auto collect(Sequence sequence) -> async<std::vector<AsyncElement<Sequence>>> {
    std::vector<AsyncElement<Sequence>> result;
    auto it = sequence.makeAsyncIterator();
    while (auto element = co_await it.next()) result.push_back(std::move(*element));
    co_return result;
}

} // namespace detail

/// Swift's `AsyncSequence` protocol extension methods, mixed into sequences via CRTP.
/// Transforming closures may be sync or return `async<...>`.
template <class Derived>
class AsyncSequenceOps {
public:
    /// `sequence.map { ... }`
    template <class F>
    auto map(F transform) const {
        return AsyncMapSequence<Derived, F>(self(), std::move(transform));
    }
    /// `sequence.filter { ... }`
    template <class F>
    auto filter(F isIncluded) const {
        return AsyncFilterSequence<Derived, F>(self(), std::move(isIncluded));
    }
    /// `sequence.compactMap { ... }` — `transform` returns `std::optional<U>`.
    template <class F>
    auto compactMap(F transform) const {
        return AsyncCompactMapSequence<Derived, F>(self(), std::move(transform));
    }
    /// `sequence.prefix(n)`
    auto prefix(std::size_t maxLength) const { return AsyncPrefixSequence<Derived>(self(), maxLength); }
    /// `sequence.prefix(while:)`
    template <class F>
    auto prefixWhile(F predicate) const {
        return AsyncPrefixWhileSequence<Derived, F>(self(), std::move(predicate));
    }
    /// `sequence.dropFirst(n)`
    auto dropFirst(std::size_t count = 1) const { return AsyncDropFirstSequence<Derived>(self(), count); }

    /// `await sequence.reduce(initial) { acc, x in ... }`
    template <class R, class F>
    auto reduce(R initial, F next) const -> async<R> {
        return detail::reduce(self(), std::move(initial), std::move(next));
    }
    /// `await sequence.reduce(into: initial) { acc, x in ... }` — `f(R&, Element)`.
    template <class R, class F>
    auto reduceInto(R initial, F update) const -> async<R> {
        return detail::reduceInto(self(), std::move(initial), std::move(update));
    }
    /// `await sequence.first(where:)`
    template <class F>
    auto first(F predicate) const {
        return detail::first(self(), std::move(predicate));
    }
    /// `await sequence.contains(value)`
    template <class U>
    auto contains(U value) const -> async<bool> {
        auto found = co_await detail::first(self(), [value](const auto& x) { return x == value; });
        co_return found.has_value();
    }
    /// Collects all elements (Swift: `await Array(sequence)` via reduce).
    auto collect() const { return detail::collect(self()); }

private:
    const Derived& self() const { return static_cast<const Derived&>(*this); }
};

// MARK: - Adapters

template <class Base, class F>
class AsyncMapSequence : public AsyncSequenceOps<AsyncMapSequence<Base, F>> {
public:
    using Element = detail::async_result_t<F&, AsyncElement<Base>&>;

    AsyncMapSequence(Base base, F f) : base_(std::move(base)), f_(std::move(f)) {}

    class AsyncIterator {
    public:
        AsyncIterator(decltype(std::declval<Base&>().makeAsyncIterator()) it, F f) : it_(std::move(it)), f_(std::move(f)) {}
        auto next() -> async<std::optional<Element>> {
            auto element = co_await it_.next();
            if (!element) co_return std::nullopt;
            co_return co_await detail::callAsync(f_, *element);
        }

    private:
        decltype(std::declval<Base&>().makeAsyncIterator()) it_;
        F f_;
    };
    AsyncIterator makeAsyncIterator() const {
        auto base = base_;
        return AsyncIterator(base.makeAsyncIterator(), f_);
    }

private:
    Base base_;
    F f_;
};

template <class Base, class F>
class AsyncFilterSequence : public AsyncSequenceOps<AsyncFilterSequence<Base, F>> {
public:
    using Element = AsyncElement<Base>;

    AsyncFilterSequence(Base base, F f) : base_(std::move(base)), f_(std::move(f)) {}

    class AsyncIterator {
    public:
        AsyncIterator(decltype(std::declval<Base&>().makeAsyncIterator()) it, F f) : it_(std::move(it)), f_(std::move(f)) {}
        auto next() -> async<std::optional<Element>> {
            while (auto element = co_await it_.next()) {
                if (co_await detail::callAsync(f_, *element)) co_return element;
            }
            co_return std::nullopt;
        }

    private:
        decltype(std::declval<Base&>().makeAsyncIterator()) it_;
        F f_;
    };
    AsyncIterator makeAsyncIterator() const {
        auto base = base_;
        return AsyncIterator(base.makeAsyncIterator(), f_);
    }

private:
    Base base_;
    F f_;
};

template <class Base, class F>
class AsyncCompactMapSequence : public AsyncSequenceOps<AsyncCompactMapSequence<Base, F>> {
public:
    using Element = typename detail::async_result_t<F&, AsyncElement<Base>&>::value_type;

    AsyncCompactMapSequence(Base base, F f) : base_(std::move(base)), f_(std::move(f)) {}

    class AsyncIterator {
    public:
        AsyncIterator(decltype(std::declval<Base&>().makeAsyncIterator()) it, F f) : it_(std::move(it)), f_(std::move(f)) {}
        auto next() -> async<std::optional<Element>> {
            while (auto element = co_await it_.next()) {
                std::optional<Element> mapped = co_await detail::callAsync(f_, *element);
                if (mapped) co_return mapped;
            }
            co_return std::nullopt;
        }

    private:
        decltype(std::declval<Base&>().makeAsyncIterator()) it_;
        F f_;
    };
    AsyncIterator makeAsyncIterator() const {
        auto base = base_;
        return AsyncIterator(base.makeAsyncIterator(), f_);
    }

private:
    Base base_;
    F f_;
};

template <class Base>
class AsyncPrefixSequence : public AsyncSequenceOps<AsyncPrefixSequence<Base>> {
public:
    using Element = AsyncElement<Base>;

    AsyncPrefixSequence(Base base, std::size_t n) : base_(std::move(base)), n_(n) {}

    class AsyncIterator {
    public:
        AsyncIterator(decltype(std::declval<Base&>().makeAsyncIterator()) it, std::size_t n) : it_(std::move(it)), remaining_(n) {}
        auto next() -> async<std::optional<Element>> {
            if (remaining_ == 0) co_return std::nullopt;
            --remaining_;
            co_return co_await it_.next();
        }

    private:
        decltype(std::declval<Base&>().makeAsyncIterator()) it_;
        std::size_t remaining_;
    };
    AsyncIterator makeAsyncIterator() const {
        auto base = base_;
        return AsyncIterator(base.makeAsyncIterator(), n_);
    }

private:
    Base base_;
    std::size_t n_;
};

template <class Base, class F>
class AsyncPrefixWhileSequence : public AsyncSequenceOps<AsyncPrefixWhileSequence<Base, F>> {
public:
    using Element = AsyncElement<Base>;

    AsyncPrefixWhileSequence(Base base, F f) : base_(std::move(base)), f_(std::move(f)) {}

    class AsyncIterator {
    public:
        AsyncIterator(decltype(std::declval<Base&>().makeAsyncIterator()) it, F f) : it_(std::move(it)), f_(std::move(f)) {}
        auto next() -> async<std::optional<Element>> {
            if (done_) co_return std::nullopt;
            auto element = co_await it_.next();
            if (element && co_await detail::callAsync(f_, *element)) co_return element;
            done_ = true;
            co_return std::nullopt;
        }

    private:
        decltype(std::declval<Base&>().makeAsyncIterator()) it_;
        F f_;
        bool done_ = false;
    };
    AsyncIterator makeAsyncIterator() const {
        auto base = base_;
        return AsyncIterator(base.makeAsyncIterator(), f_);
    }

private:
    Base base_;
    F f_;
};

template <class Base>
class AsyncDropFirstSequence : public AsyncSequenceOps<AsyncDropFirstSequence<Base>> {
public:
    using Element = AsyncElement<Base>;

    AsyncDropFirstSequence(Base base, std::size_t n) : base_(std::move(base)), n_(n) {}

    class AsyncIterator {
    public:
        AsyncIterator(decltype(std::declval<Base&>().makeAsyncIterator()) it, std::size_t n) : it_(std::move(it)), toDrop_(n) {}
        auto next() -> async<std::optional<Element>> {
            while (toDrop_ > 0) {
                --toDrop_;
                if (!co_await it_.next()) co_return std::nullopt;
            }
            co_return co_await it_.next();
        }

    private:
        decltype(std::declval<Base&>().makeAsyncIterator()) it_;
        std::size_t toDrop_;
    };
    AsyncIterator makeAsyncIterator() const {
        auto base = base_;
        return AsyncIterator(base.makeAsyncIterator(), n_);
    }

private:
    Base base_;
    std::size_t n_;
};

// MARK: - AsyncStream

namespace detail {

template <class Element>
class StreamState {
public:
    enum class Termination { finished, cancelled };
    enum class Policy { unbounded, bufferingOldest, bufferingNewest };

    StreamState(Policy policy, std::size_t limit) : policy_(policy), limit_(limit) {}

    struct YieldOutcome {
        enum Kind { enqueued, dropped, terminated } kind;
        std::size_t remaining = 0;
        std::optional<Element> droppedElement;
    };

    YieldOutcome yield(Element value) {
        std::optional<Waiter> waiter;
        YieldOutcome outcome{YieldOutcome::enqueued};
        {
            std::lock_guard lock(mutex_);
            if (finished_) return {YieldOutcome::terminated};
            if (policy_ == Policy::bufferingOldest && buffer_.size() >= limit_ && !waiter_) {
                return {YieldOutcome::dropped, 0, std::move(value)};
            }
            buffer_.push_back(std::move(value));
            if (policy_ == Policy::bufferingNewest && buffer_.size() > limit_ && !waiter_) {
                outcome = {YieldOutcome::dropped, 0, std::move(buffer_.front())};
                buffer_.pop_front();
            }
            if (policy_ != Policy::unbounded && outcome.kind == YieldOutcome::enqueued) {
                outcome.remaining = limit_ > buffer_.size() ? limit_ - buffer_.size() : 0;
            }
            waiter = takeWaiterLocked();
        }
        if (waiter) waiter->resume();
        return outcome;
    }

    void finish(std::exception_ptr error = nullptr) { terminate(Termination::finished, error); }

    /// Shared by every copy of a stream's continuation. Destroying the last
    /// copy finishes the stream, as in Swift, so a consumer is never left
    /// waiting on a stream that nobody can feed any more.
    struct Producer {
        std::shared_ptr<StreamState> state;
        ~Producer() { state->finish(); }
    };

    static std::shared_ptr<Producer> producer(const std::shared_ptr<StreamState>& state) {
        std::lock_guard lock(state->mutex_);
        auto p = state->producer_.lock();
        if (!p) {
            p = std::shared_ptr<Producer>(new Producer{state});
            state->producer_ = p;
        }
        return p;
    }

    void setOnTermination(std::function<void(Termination)> handler) {
        std::lock_guard lock(mutex_);
        if (!finished_) onTermination_ = std::move(handler);
    }

    // MARK: consumer side

    bool await_ready_locked() const { return !buffer_.empty() || finished_; }

    struct NextAwaiter {
        std::shared_ptr<StreamState> state;
        bool await_ready() {
            auto* task = currentContext().task;
            if (task && task->isCancelled()) {
                state->terminate(Termination::cancelled, nullptr);
                return true;
            }
            std::lock_guard lock(state->mutex_);
            return state->await_ready_locked();
        }
        bool await_suspend(std::coroutine_handle<> h) {
            auto& ctx = currentContext();
            std::uint64_t handlerId = 0;
            if (ctx.task) {
                handlerId = TaskBase::makeHandlerId();
                std::weak_ptr<StreamState> weak = state;
                if (!ctx.task->addCancellationHandler(handlerId, [weak] {
                        if (auto s = weak.lock()) s->terminate(Termination::cancelled, nullptr);
                    })) {
                    state->terminate(Termination::cancelled, nullptr);
                    return false;
                }
            }
            std::lock_guard lock(state->mutex_);
            if (state->await_ready_locked()) {
                if (ctx.task) ctx.task->removeCancellationHandler(handlerId);
                return false;
            }
            state->waiter_ = Waiter::current(h);
            state->waiterHandlerId_ = handlerId;
            return true;
        }
        std::optional<Element> await_resume() {
            if (auto* task = currentContext().task; task && state->waiterHandlerId_) {
                task->removeCancellationHandler(state->waiterHandlerId_);
            }
            std::lock_guard lock(state->mutex_);
            state->waiterHandlerId_ = 0;
            if (!state->buffer_.empty()) {
                auto v = std::move(state->buffer_.front());
                state->buffer_.pop_front();
                return v;
            }
            if (state->error_) {
                auto e = std::exchange(state->error_, nullptr);
                std::rethrow_exception(e);
            }
            return std::nullopt;
        }
    };

    void terminate(Termination reason, std::exception_ptr error) {
        std::optional<Waiter> waiter;
        std::function<void(Termination)> handler;
        {
            std::lock_guard lock(mutex_);
            if (finished_) return;
            finished_ = true;
            error_ = error;
            if (reason == Termination::cancelled) buffer_.clear();
            handler = std::move(onTermination_);
            onTermination_ = nullptr;
            waiter = takeWaiterLocked();
        }
        if (handler) handler(reason);
        if (waiter) waiter->resume();
    }

private:
    std::optional<Waiter> takeWaiterLocked() {
        std::optional<Waiter> w = std::move(waiter_);
        waiter_.reset();
        return w;
    }

    std::mutex mutex_;
    Policy policy_;
    std::size_t limit_;
    std::deque<Element> buffer_;
    bool finished_ = false;
    std::exception_ptr error_;
    std::optional<Waiter> waiter_;
    std::uint64_t waiterHandlerId_ = 0;
    std::function<void(Termination)> onTermination_;
    std::weak_ptr<Producer> producer_;
};

/// Shared implementation of AsyncStream / AsyncThrowingStream.
template <class T, bool Throwing, class Derived>
class BasicAsyncStream : public AsyncSequenceOps<Derived> {
    using State = StreamState<T>;

public:
    using Element = T;
    using Termination = typename State::Termination;

    /// Swift's `AsyncStream.Continuation.BufferingPolicy`.
    struct BufferingPolicy {
        typename State::Policy policy = State::Policy::unbounded;
        std::size_t limit = 0;
        static BufferingPolicy unbounded() { return {}; }
        static BufferingPolicy bufferingOldest(std::size_t n) { return {State::Policy::bufferingOldest, n}; }
        static BufferingPolicy bufferingNewest(std::size_t n) { return {State::Policy::bufferingNewest, n}; }
    };

    /// Swift's `AsyncStream.Continuation.YieldResult`.
    struct YieldResult {
        enum Kind { enqueued, dropped, terminated } kind;
        std::size_t remaining = 0;               // for `enqueued` with a bounded buffer
        std::optional<Element> droppedElement;   // for `dropped`
    };

    /// Swift's `AsyncStream.Continuation` — the producer side. Copyable, thread-safe.
    /// When the last copy is destroyed, the stream finishes (like `finish()`).
    class Continuation {
    public:
        explicit Continuation(const std::shared_ptr<State>& s) : producer_(State::producer(s)) {}

        YieldResult yield(Element value) const {
            auto o = producer_->state->yield(std::move(value));
            return {static_cast<typename YieldResult::Kind>(o.kind), o.remaining, std::move(o.droppedElement)};
        }
        /// `continuation.finish()`
        void finish() const { producer_->state->finish(); }
        /// `continuation.finish(throwing: error)` (AsyncThrowingStream only).
        void finish(std::exception_ptr error) const
            requires Throwing
        {
            producer_->state->finish(std::move(error));
        }
        template <class E>
            requires(Throwing && std::is_base_of_v<std::exception, std::remove_cvref_t<E>>)
        void finish(E&& error) const {
            producer_->state->finish(std::make_exception_ptr(std::forward<E>(error)));
        }
        /// `continuation.onTermination = { termination in ... }`
        void onTermination(std::function<void(Termination)> handler) const {
            producer_->state->setOnTermination(std::move(handler));
        }

    private:
        std::shared_ptr<typename State::Producer> producer_;
    };

    class AsyncIterator {
    public:
        explicit AsyncIterator(std::shared_ptr<State> s) : state_(std::move(s)) {}
        /// `await iterator.next()`
        typename State::NextAwaiter next() { return {state_}; }

    private:
        std::shared_ptr<State> state_;
    };

    AsyncIterator makeAsyncIterator() const { return AsyncIterator(state_); }

protected:
    BasicAsyncStream(BufferingPolicy policy) : state_(std::make_shared<State>(policy.policy, policy.limit)) {}

    Continuation continuation() const { return Continuation(state_); }

    std::shared_ptr<State> state_;
};

} // namespace detail

/// Swift's `AsyncStream<Element>`.
template <class Element>
class AsyncStream : public detail::BasicAsyncStream<Element, false, AsyncStream<Element>> {
    using Base = detail::BasicAsyncStream<Element, false, AsyncStream<Element>>;

public:
    using typename Base::BufferingPolicy;
    using typename Base::Continuation;

    /// `AsyncStream { continuation in ... }` — `build` runs synchronously.
    template <class Build>
        requires std::invocable<Build&, Continuation>
    explicit AsyncStream(Build build, BufferingPolicy policy = BufferingPolicy::unbounded()) : Base(policy) {
        build(this->continuation());
    }

    /// `AsyncStream.makeStream(of:bufferingPolicy:)`
    static std::pair<AsyncStream, Continuation> makeStream(BufferingPolicy policy = BufferingPolicy::unbounded()) {
        AsyncStream stream(policy);
        auto c = stream.continuation();
        return {std::move(stream), std::move(c)};
    }

private:
    explicit AsyncStream(BufferingPolicy policy) : Base(policy) {}
};

/// Swift's `AsyncThrowingStream<Element, Error>`; iterate with `for_await`
/// (errors passed to `finish(throwing)` are rethrown from the loop).
template <class Element>
class AsyncThrowingStream : public detail::BasicAsyncStream<Element, true, AsyncThrowingStream<Element>> {
    using Base = detail::BasicAsyncStream<Element, true, AsyncThrowingStream<Element>>;

public:
    using typename Base::BufferingPolicy;
    using typename Base::Continuation;

    template <class Build>
        requires std::invocable<Build&, Continuation>
    explicit AsyncThrowingStream(Build build, BufferingPolicy policy = BufferingPolicy::unbounded()) : Base(policy) {
        build(this->continuation());
    }

    static std::pair<AsyncThrowingStream, Continuation> makeStream(
        BufferingPolicy policy = BufferingPolicy::unbounded()) {
        AsyncThrowingStream stream(policy);
        auto c = stream.continuation();
        return {std::move(stream), std::move(c)};
    }

private:
    explicit AsyncThrowingStream(BufferingPolicy policy) : Base(policy) {}
};

} // namespace corcurrency
