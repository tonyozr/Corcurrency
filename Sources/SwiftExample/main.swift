// Swift side: native Swift concurrency driving a Corcurrency-based C++ component.
//
// The C++ code (Sources/InventoryBridge) is written with Corcurrency — actors,
// tasks, task groups — and exposed through a small C API with completion
// callbacks. Here each callback is turned back into a Swift `async` function
// with `withCheckedContinuation`, and C++ cancellation is wired to Swift
// cancellation with `withTaskCancellationHandler`.
import InventoryBridge

// MARK: - Callback → async plumbing

/// Retains a continuation across the C boundary as an opaque `void*` context.
private final class ContinuationBox<T> {
    let continuation: CheckedContinuation<T, Never>
    init(_ continuation: CheckedContinuation<T, Never>) { self.continuation = continuation }

    static func context(_ continuation: CheckedContinuation<T, Never>) -> UnsafeMutableRawPointer {
        Unmanaged.passRetained(ContinuationBox(continuation)).toOpaque()
    }

    static func resume(_ context: UnsafeMutableRawPointer?, returning value: T) {
        Unmanaged<ContinuationBox<T>>.fromOpaque(context!).takeRetainedValue().continuation.resume(returning: value)
    }
}

// MARK: - Swift wrappers

/// Swift handle to the C++ `actor<Inventory>`. Safe to share: the C++ actor
/// serializes all access.
final class CppInventory: @unchecked Sendable {
    private let handle: OpaquePointer

    init() { handle = cc_inventory_create() }
    deinit { cc_inventory_release(handle) }

    func add(_ item: String, count: Int) async -> Int {
        await withCheckedContinuation { continuation in
            cc_inventory_add(handle, item, Int32(count), ContinuationBox.context(continuation)) { context, total in
                ContinuationBox<Int>.resume(context, returning: Int(total))
            }
        }
    }

    var total: Int {
        get async {
            await withCheckedContinuation { continuation in
                cc_inventory_total(handle, ContinuationBox.context(continuation)) { context, total in
                    ContinuationBox<Int>.resume(context, returning: Int(total))
                }
            }
        }
    }
}

/// Sums quotes in a C++ task group.
func sumQuotes(_ symbols: [String]) async -> Double {
    await withCheckedContinuation { continuation in
        cc_sum_quotes(symbols.joined(separator: ","), ContinuationBox.context(continuation)) { context, sum in
            ContinuationBox<Double>.resume(context, returning: sum)
        }
    }
}

/// Sleeps in a C++ task; cancelling the Swift task cancels the C++ task.
/// Returns true if it was cancelled.
func cppSleep(milliseconds: Int) async -> Bool {
    final class TaskHandle: @unchecked Sendable {
        var task: OpaquePointer?
        deinit { if let task { cc_task_release(task) } }
    }
    let handle = TaskHandle()
    return await withTaskCancellationHandler {
        await withCheckedContinuation { continuation in
            handle.task = cc_start_sleep(Int32(milliseconds), ContinuationBox.context(continuation)) { context, cancelled in
                ContinuationBox<Bool>.resume(context, returning: cancelled)
            }
            if Task.isCancelled { cc_task_cancel(handle.task) }
        }
    } onCancel: {
        if let task = handle.task { cc_task_cancel(task) }
    }
}

// MARK: - Demo (top-level code runs as Swift's async main)

let inventory = CppInventory()

// Swift task group → many concurrent calls into the C++ actor.
print("1) Swift TaskGroup calling a C++ actor concurrently:")
await withTaskGroup(of: Int.self) { group in
    for i in 1...100 {
        group.addTask { await inventory.add("item-\(i)", count: 1) }
    }
}
print("   total = \(await inventory.total) (expected 100)")

// A C++ task group, awaited from Swift.
print("2) C++ TaskGroup awaited from Swift:")
print("   sum of quotes = \(await sumQuotes(["AAPL", "MSFT", "NVIDIA"]))")

// async let in Swift over C++ async work.
print("3) Swift async let over C++ work:")
async let first = sumQuotes(["A"])
async let second = sumQuotes(["BB", "CCC"])
print("   \(await first) + \(await second) = \(await first + second)")

// Cancellation crosses the boundary: Swift cancel → C++ Task.cancel().
print("4) Swift cancellation cancels the C++ task:")
let sleeper = Task { await cppSleep(milliseconds: 10_000) }
try? await Task.sleep(for: .milliseconds(20))
sleeper.cancel()
print("   C++ task cancelled: \(await sleeper.value)")
