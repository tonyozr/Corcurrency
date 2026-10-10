// C++ Corcurrency — task-local values (Swift's `@TaskLocal`).
//
//     inline TaskLocal<std::string> requestID{"none"};       // @TaskLocal static var requestID = "none"
//
//     co_await requestID.withValue("abc", []() -> async<void> {   // await $requestID.withValue("abc") { ... }
//         log(requestID.get());                                  // "abc" — also in child tasks
//     });
//
// Values are inherited by `Task {}`, task-group children and `async let`, but
// not by `Task<T>::detached`.
#pragma once

#include <corcurrency/async.hpp>

#include <memory>
#include <type_traits>
#include <utility>

namespace corcurrency {

namespace detail {

/// The binding list of the current task (or of the thread outside any task).
inline std::shared_ptr<const TaskLocalNode>& currentLocals() {
    auto* task = currentContext().task;
    return task ? task->locals : threadTaskLocals();
}

} // namespace detail

template <class T>
class TaskLocal {
public:
    explicit TaskLocal(T defaultValue = T()) : defaultValue_(std::move(defaultValue)) {}
    TaskLocal(const TaskLocal&) = delete;
    TaskLocal& operator=(const TaskLocal&) = delete;

    /// The value bound in the current task, or the default.
    const T& get() const {
        for (auto* node = detail::currentLocals().get(); node; node = node->next.get()) {
            if (node->key == this) return *static_cast<const T*>(node->value.get());
        }
        return defaultValue_;
    }
    const T& operator*() const { return get(); }
    const T* operator->() const { return &get(); }

    /// `$local.withValue(value) { ... }` — binds `value` for the duration of
    /// `operation`. If `operation` is async the result is `async<R>` to await,
    /// otherwise the call is synchronous.
    template <class F>
    auto withValue(T value, F operation) const {
        if constexpr (detail::is_async_v<std::invoke_result_t<F&>>) {
            return withValueAsync(std::move(value), std::move(operation));
        } else {
            Binding binding(this, std::move(value));
            return operation();
        }
    }

private:
    /// Pushes a binding on construction and pops it on destruction.
    class Binding {
    public:
        Binding(const TaskLocal* key, T value) : slot_(detail::currentLocals()), saved_(slot_) {
            slot_ = std::make_shared<const detail::TaskLocalNode>(
                detail::TaskLocalNode{key, std::make_shared<T>(std::move(value)), saved_});
        }
        ~Binding() { slot_ = std::move(saved_); }
        Binding(const Binding&) = delete;
        Binding& operator=(const Binding&) = delete;

    private:
        std::shared_ptr<const detail::TaskLocalNode>& slot_;
        std::shared_ptr<const detail::TaskLocalNode> saved_;
    };

    template <class F>
    auto withValueAsync(T value, F operation) const -> async<detail::async_result_t<F&>> {
        Binding binding(this, std::move(value));
        co_return co_await detail::callAsync(operation);
    }

    T defaultValue_;
};

} // namespace corcurrency
