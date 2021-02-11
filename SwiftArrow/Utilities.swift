//
//  Utilities.swift
//  SwiftArrow
//
//  Created by Marc Prud'hommeaux on 2/10/21.
//

import Foundation


// MARK: Utilities

/// Warning marker for a work-in-progress
@available(*, deprecated, message: "work in progress")
@inlinable func wip<T>(_ value: T) -> T {
    value
}

#if canImport(OSLog)
import OSLog

/// debug message to NSLog only when NDEBUG is not set
@inlinable public func dbg(level: OSLogType = .default, _ arg1: @autoclosure () throws -> CVarArg? = nil, _ arg2: @autoclosure () -> CVarArg? = nil, _ arg3: @autoclosure () -> CVarArg? = nil, _ arg4: @autoclosure () -> CVarArg? = nil, _ arg5: @autoclosure () -> CVarArg? = nil, _ arg6: @autoclosure () -> CVarArg? = nil, _ arg7: @autoclosure () -> CVarArg? = nil, _ arg8: @autoclosure () -> CVarArg? = nil, _ arg9: @autoclosure () -> CVarArg? = nil, _ arg10: @autoclosure () -> CVarArg? = nil, _ arg11: @autoclosure () -> CVarArg? = nil, _ arg12: @autoclosure () -> CVarArg? = nil, separator: String = " ", functionName: StaticString = #function, fileName: StaticString = #file, lineNumber: Int = #line) rethrows {
    //#if DEBUG
    let items = try [arg1(), arg2(), arg3(), arg4(), arg5(), arg6(), arg7(), arg8(), arg9(), arg10(), arg11(), arg12()]
    let msg = items.compactMap({ $0 }).map({ String(describing: $0) }).joined(separator: separator)

    // use just the file name
    let filePath = fileName.description.split(separator: "/").last?.description ?? fileName.description

    let message = "\(filePath):\(lineNumber) \(functionName): \(msg)"
    // os_log("%{public}@", /* log: log, */ type: level, message)

    os_log(level, "%{public}@", message)
    //#endif
}
#endif


#if canImport(Dispatch)
extension Collection {
    /// Executes the given block concurrently using `DispatchQueue.concurrentPerform`, returning the array of results
    @inlinable public func qmap<T>(concurrent: Bool = true, block: (Element) throws -> (T)) throws -> [T] {
        let queue = DispatchQueue(label: "resultsLock")

        let items = Array(self)
        var results: [Result<T, Error>?] = Array(repeating: nil, count: items.count)

        DispatchQueue.concurrentPerform(iterations: items.count) { i in
            let result = Result { try block(items[i]) }
            queue.sync { results[i] = result }
        }

        // returns all the results, or throws the first error encountered
        return try results.map { result in try result!.get() }
    }
}
#endif



#if canImport(Darwin)
/// The current total memory size.
/// Thanks, Quinn: https://developer.apple.com/forums/thread/105088
@inlinable public func memoryFootprint() -> mach_vm_size_t? {
    // The `TASK_VM_INFO_COUNT` and `TASK_VM_INFO_REV1_COUNT` macros are too
    // complex for the Swift C importer, so we have to define them ourselves.
    let TASK_VM_INFO_COUNT = mach_msg_type_number_t(MemoryLayout<task_vm_info_data_t>.size / MemoryLayout<integer_t>.size)
    let TASK_VM_INFO_REV1_COUNT = mach_msg_type_number_t(MemoryLayout.offset(of: \task_vm_info_data_t.min_address)! / MemoryLayout<integer_t>.size)
    var info = task_vm_info_data_t()
    var count = TASK_VM_INFO_COUNT
    let kr = withUnsafeMutablePointer(to: &info) { infoPtr in
        infoPtr.withMemoryRebound(to: integer_t.self, capacity: Int(count)) { intPtr in
            task_info(mach_task_self_, task_flavor_t(TASK_VM_INFO), intPtr, &count)
        }
    }
    guard
        kr == KERN_SUCCESS,
        count >= TASK_VM_INFO_REV1_COUNT
    else { return nil }
    return info.phys_footprint
}
#endif

