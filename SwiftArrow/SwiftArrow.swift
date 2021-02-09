//
//  SwiftArrow.swift
//  SwiftArrow
//
//  Created by Marc Prud'hommeaux on 1/25/21.
//

import Foundation

public enum SwiftArrowError : Error {
    case general
    case missingFileError(url: URL)
    case missingPointer
    case noBuffers
    case noMultiBufferSupport
    case nullsUnsupported
    case nullsInconsistent
    case emptyBuffer
    case wrongBufferCount(Int)
}

/// Setup Rust logging. This can be called multiple times, from multiple threads.
func initRustLogging() {
    initialize_logging()
}

func arrowToJSON(arrowData: Data, arrowFile: URL, JSONFile: URL) throws -> Any {
    if FileManager.default.isDeletableFile(atPath: JSONFile.path) {
        try FileManager.default.removeItem(at: JSONFile)
    }

    try arrowData.write(to: arrowFile)

    arrow_to_json()

    let JSONData = try Data(contentsOf: JSONFile)
    return try JSONSerialization.jsonObject(with: JSONData, options: [])
}

func JSONToArrow(arrow: NSDictionary, JSONFile: URL, arrowFile: URL) throws -> Data {
    if FileManager.default.isDeletableFile(atPath: arrowFile.path) {
        try FileManager.default.removeItem(at: arrowFile)
    }

    let JSONData = try JSONSerialization.data(withJSONObject: arrow, options: [])

    try JSONData.write(to: JSONFile)

    json_to_arrow()

    let arrowData = try Data(contentsOf: arrowFile)
    return arrowData
}

enum SwiftRustError : Error {
    case generic(String?)

    /// Passes the given value through the SwiftRust error checking
    static func checking<T>(_ value: T!) throws -> T! {
        let errlen = last_error_length()
        if errlen <= 0 { return value }

        let buffer = UnsafeMutablePointer<Int8>.allocate(capacity: Int(errlen))
        defer { buffer.deallocate() }

        if last_error_message(buffer, errlen) != 0 {
            throw Self.generic(String(validatingUTF8: buffer))
        }

        return value
    }
}

extension ArrowVectorFFI {
    func roundTrip() -> ArrowVectorFFI {
        withUnsafePointer(to: self) {
            arrow_array_ffi_roundtrip($0)
        }
    }
}

//extension ArrowArray {
//    func argParamDemo(param: Int64) {
//        arrow_array_ffi_arg_param_demo(self, param)
//    }
//}

class ArrowCSV {
    private let fileURL: URL

    init(fileURL: URL) {
        // ptr = request_create(url)
        self.fileURL = fileURL
    }

    deinit {
        // request_destroy(ptr)
    }

    func load(printRows: Int64 = 0) throws -> OpaquePointer? {
        try fileURL.path.withCString({
            try SwiftRustError.checking(arrow_load_csv($0, printRows))
        })
    }
}

// https://github.com/nickwilcox/recipe-swift-rust-callbacks/blob/main/wrapper.swift

private class WrapClosure<T> {
    fileprivate let closure: T
    init(closure: T) {
        self.closure = closure
    }
}

public func invokeCallbackBool(millis: UInt64, closure: @escaping (Bool) -> Void) {
    let wrappedClosure = WrapClosure(closure: closure)
    let userdata = Unmanaged.passRetained(wrappedClosure).toOpaque()
    let callback: @convention(c) (UnsafeMutableRawPointer?, Bool) -> Void = { (_ userdata: UnsafeMutableRawPointer?, _ success: Bool) in
        let wrappedClosure: WrapClosure<(Bool) -> Void> = Unmanaged.fromOpaque(userdata!).takeRetainedValue()
        wrappedClosure.closure(success)
    }
    let completion = CallbackBool(userdata: userdata, callback: callback)
    callback_bool_after(millis, completion)
}

public func invokeCallbackInt64(millis: UInt64, value: Int64, closure: @escaping (Int64) -> Void) {
    let wrappedClosure = WrapClosure(closure: closure)
    let userdata = Unmanaged.passRetained(wrappedClosure).toOpaque()
    let completion = CallbackInt64(userdata: userdata) { data, i in
        let wrappedClosure: WrapClosure<(Int64) -> Void> = Unmanaged.fromOpaque(data!).takeRetainedValue()
        wrappedClosure.closure(i)
    }
    callback_int64_after(millis, value, completion)
}

public class DFExecutionContext {
    let ptr: OpaquePointer

    public init() {
        ptr = datafusion_context_create()
    }

    deinit {
        datafusion_context_destroy(ptr)
    }

    /// Registers the given URL to a `.parquet` file as the given table name
    public func register(parquet: URL, tableName: String) throws {
        try SwiftRustError.checking(datafusion_context_register_parquet(ptr, parquet.path, tableName))
    }

    /// Registers the given URL to a `.csv` file as the given table name
    public func register(csv: URL, tableName: String) throws {
        try SwiftRustError.checking(datafusion_context_register_csv(ptr, csv.path, tableName))
    }

    /// Registers the given `.parquet` file directly
    public func load(parquet: URL) throws -> DFDataFrame? {
        try DFDataFrame(checking: datafusion_context_read_parquet(ptr, parquet.path))
    }

    /// Registers the given `.csv` file directly
    public func load(csv: URL) throws -> DFDataFrame? {
        try DFDataFrame(checking: datafusion_context_read_csv(ptr, csv.path))
    }

    /// Issues a SQL query against the context
    public func query(sql: String) throws -> DFDataFrame? {
        try DFDataFrame(checking: datafusion_context_execute_sql(ptr, sql))
    }
}

public class DFDataFrame {
    let ptr: OpaquePointer

    init(ptr: OpaquePointer) {
        self.ptr = ptr
    }

    init?(checking ptr: OpaquePointer?) throws {
        guard let ptr = try SwiftRustError.checking(ptr) else { return nil }
        self.ptr = ptr
    }

    deinit {
        datafusion_dataframe_destroy(ptr)
    }

    public func limit(count: UInt) throws -> DFDataFrame {
        DFDataFrame(ptr: try SwiftRustError.checking(datafusion_dataframe_limit(ptr, count)))
    }

    /// Executes the DataFrame and returns the count
    public func collectionCount() throws -> Int {
        try collectVector(index: 0).bufferLength
        // try SwiftRustError.checking(datafusion_dataframe_collect_count(ptr))
    }

    /// Executes the DataFrame and returns the first column
    public func collectVector(index: UInt) throws -> ArrowVector {
        ArrowVector(ffi: try SwiftRustError.checking(datafusion_dataframe_collect_vector(ptr, index)))
    }

//    /// Executes the DataFrame and returns all the columns
//    public func collectedAerays() throws -> ArrowVectorFFI {
//        try SwiftRustError.checking(datafusion_dataframe_collect_vector(ptr, index))
//    }
}

/// A wrapper for ArrowVectorFFI that manages deallocation, as per
/// http://arrow.apache.org/docs/format/CDataInterface.html
public final class ArrowVector {
    @usableFromInline
    let ffi: ArrowVectorFFI

    fileprivate init(ffi: ArrowVectorFFI) {
        self.ffi = ffi
    }

    deinit {
        ffi.array.pointee.release(OpaquePointer(ffi.array))
        ffi.schema.pointee.release(OpaquePointer(ffi.schema))
    }

    // MARK: Schema Functions

    @usableFromInline var schema: FFI_ArrowSchema {
        ffi.schema.pointee
    }

    @inlinable public var name: String? {
        schema.name.flatMap(String.init(cString:))
    }

    @inlinable public var format: ArrowDataType? {
        schema.format.flatMap(String.init(cString:)).flatMap(ArrowDataType.init(_:))
    }

    @inlinable public var metadata: String? {
        wip(nil) // “This string is not null-terminated but follows a specific format…” – http://arrow.apache.org/docs/format/CDataInterface.html#c.ArrowSchema.metadata
        // schema.metadata.flatMap(String.init(cString:))
    }

    @inlinable public var schemaChildCount: Int {
        .init(schema.n_children)
    }

    @inlinable public var flags: Int64 {
        schema.flags
    }

    // MARK: Array Functions

    @usableFromInline var array: FFI_ArrowArray {
        ffi.array.pointee
    }

    /// The number of null items in the array. MAY be -1 if not yet computed.
    @inlinable public var nullCount: Int {
        .init(array.null_count)
    }

    /// The logical offset inside the array (i.e. the number of items from the physical start of the buffers). MUST be 0 or positive.
    @inlinable public var offset: Int {
        .init(array.offset)
    }

    /// The number of children this array has. The number of children is a function of the data type, as described in the Columnar format specification.
    @inlinable public var arrayChildCount: Int {
        .init(array.n_children)
    }

    /// The number of physical buffers backing this array. The number of buffers is a function of the data type, as described in the Columnar format specification.
    @inlinable public var bufferCount: Int {
        .init(array.n_buffers)
    }

    /// The logical length of the array (i.e. its number of items).
    @inlinable public var bufferLength: Int {
        .init(array.length)
    }

    /// The sequential buffers
    @usableFromInline var bufferContents: UnsafeBufferPointer<UnsafeRawPointer?> {
        let buf: UnsafeMutablePointer<UnsafeRawPointer?>! = array.buffers
        let buffers = UnsafeBufferPointer(start: buf, count: .init(bufferCount))
        return buffers
    }

    /// For types whose first buffer is the validity bitfield, returns the mapped data
    @usableFromInline var validityBitfield: Data? {
        guard let validityBitfield = bufferContents.first else {
            return nil
        }

        guard let validity = validityBitfield else {
            return nil
        }

        let capacity = Int(array.length + array.offset)
        let nullBufferSize = Int(ceil(Double(capacity) / 8))
        return Data(bytesNoCopy: UnsafeMutableRawPointer(mutating: validity), count: nullBufferSize, deallocator: .none)
    }

    /// Returns true if the element at the given index is valid
    /// - Returns: `false` if the element is null (as indicated by being absent in the validity bitfield)
    @inlinable public func isValid(at index: Int) -> Bool {
        validityBitfield?[bitfieldElement: index] != false
    }

    @inlinable public var capacity: Int {
        Int(array.length + array.offset)
    }
}

/// A type that can be represented efficiently by an Arrow array.
/// These include primitives, strings, and other custom types.
public protocol ArrowDataRepresentable {
    static var arrowDataType: ArrowDataType { get }
    associatedtype BufferView : ArrowBufferView where BufferView.DataType == Self
}

extension Int8 : ArrowDataRepresentable {
    public static let arrowDataType = ArrowDataType.int8
    public typealias BufferView = TypedBufferView<Self>
}

extension Int16 : ArrowDataRepresentable {
    public static let arrowDataType = ArrowDataType.int16
    public typealias BufferView = TypedBufferView<Self>
}

extension Int32 : ArrowDataRepresentable {
    public static let arrowDataType = ArrowDataType.int32
    public typealias BufferView = TypedBufferView<Self>
}

extension Int64 : ArrowDataRepresentable {
    public static let arrowDataType = ArrowDataType.int64
    public typealias BufferView = TypedBufferView<Self>
}

extension UInt8 : ArrowDataRepresentable {
    public static let arrowDataType = ArrowDataType.uint8
    public typealias BufferView = TypedBufferView<Self>
}

extension UInt16 : ArrowDataRepresentable {
    public static let arrowDataType = ArrowDataType.uint16
    public typealias BufferView = TypedBufferView<Self>
}

extension UInt32 : ArrowDataRepresentable {
    public static let arrowDataType = ArrowDataType.uint32
    public typealias BufferView = TypedBufferView<Self>
}

extension UInt64 : ArrowDataRepresentable {
    public static let arrowDataType = ArrowDataType.uint64
    public typealias BufferView = TypedBufferView<Self>
}

#if !os(macOS) // “'Float16' is unavailable in macOS”
extension Float16 : ArrowDataRepresentable {
    public static let arrowDataType = ArrowDataType.float16
    public typealias BufferView = TypedBufferView<Self>
}
#endif

extension Float32 : ArrowDataRepresentable {
    public static let arrowDataType = ArrowDataType.float32
    public typealias BufferView = TypedBufferView<Self>
}

extension Float64 : ArrowDataRepresentable {
    public static let arrowDataType = ArrowDataType.float64
    public typealias BufferView = TypedBufferView<Self>
}


public protocol ArrowBufferView : Collection where Element == Optional<DataType> {
    associatedtype DataType
    var dataType: ArrowDataType { get }
    var vector: ArrowVector { get }

    init(vector: ArrowVector) throws
}

extension ArrowBufferView {
    public var startIndex: Int { 0 }
    public var endIndex: Int { vector.bufferLength }
    public func index(after i: Int) -> Int { i + 1 }
}

/// Primitive (fixed-size): a sequence of values each having the same byte or bit width
/// https://github.com/apache/arrow/blob/master/docs/source/format/Columnar.rst#physical-memory-layout
public protocol PrimitiveBufferView where Self : ArrowBufferView {
    var buffer: UnsafeBufferPointer<DataType> { get }
}

extension PrimitiveBufferView {
    public subscript(position: Int) -> DataType? {
        if !vector.isValid(at: .init(position)) { return nil }
        return buffer[position]
    }
}



public final class TypedBufferView<T: ArrowDataRepresentable> : PrimitiveBufferView {
    public typealias DataType = T
    public var dataType: ArrowDataType { DataType.arrowDataType }
    public let vector: ArrowVector
    public let buffer: UnsafeBufferPointer<DataType>

    public init(vector: ArrowVector) throws {
        if vector.bufferCount != 2 {
            // primitive buffer views always two buffers: validity and contents
            throw SwiftArrowError.wrongBufferCount(vector.bufferCount)
        }
        self.vector = vector
        let target = vector.bufferContents[1]?.bindMemory(to: DataType.self, capacity: vector.capacity)
        self.buffer = UnsafeBufferPointer(start: target, count: vector.capacity)
    }
}



/// Variable-size Binary: a sequence of values each having a variable byte length. Two variants of this layout are supported using 32-bit and 64-bit length encoding.
/// TODO: this is how we will support UTF-8 strings
protocol VariableBufferView where Self : ArrowBufferView {
}

extension Data {
    /// Returns the bit at the current index for the given bitfield data
    // https://github.com/apache/arrow/blob/master/docs/source/format/Columnar.rst#validity-bitmaps
    @usableFromInline subscript(bitfieldElement i: Int) -> Bool {
        (self[i / 8] & (1 << (i % 8))) != 0
    }
}



import OSLog

/// debug message to NSLog only when NDEBUG is not set
@inlinable public func dbg(level: OSLogType = .default, _ arg1: @autoclosure () -> CVarArg? = nil, _ arg2: @autoclosure () -> CVarArg? = nil, _ arg3: @autoclosure () -> CVarArg? = nil, _ arg4: @autoclosure () -> CVarArg? = nil, _ arg5: @autoclosure () -> CVarArg? = nil, _ arg6: @autoclosure () -> CVarArg? = nil, _ arg7: @autoclosure () -> CVarArg? = nil, _ arg8: @autoclosure () -> CVarArg? = nil, _ arg9: @autoclosure () -> CVarArg? = nil, _ arg10: @autoclosure () -> CVarArg? = nil, _ arg11: @autoclosure () -> CVarArg? = nil, _ arg12: @autoclosure () -> CVarArg? = nil, separator: String = " ", functionName: StaticString = #function, fileName: StaticString = #file, lineNumber: Int = #line) {
    #if DEBUG
    let items = [arg1(), arg2(), arg3(), arg4(), arg5(), arg6(), arg7(), arg8(), arg9(), arg10(), arg11(), arg12()]
    let msg = items.compactMap({ $0 }).map({ String(describing: $0) }).joined(separator: separator)

    // use just the file name
    let filePath = fileName.description.split(separator: "/").last?.description ?? fileName.description

    let message = "\(filePath):\(lineNumber) \(functionName): \(msg)"
    // need to use public to log the message to the console; failure to do so will cause the strings to be logged in my dev builds, but downloaded release builds will jsut show "<private>" in the log messages
    // os_log("%{public}@", /* log: log, */ type: level, message)

    os_log(level, "%{public}@", message, arg1() ?? "", arg2() ?? "", arg3() ?? "", arg4() ?? "", arg5() ?? "", arg6() ?? "", arg7() ?? "", arg8() ?? "", arg9() ?? "", arg10() ?? "", arg11() ?? "", arg12() ?? "")
    #endif
}


public extension FFI_ArrowSchema {
    var dataType: ArrowDataType? {
        ArrowDataType(String(cString: self.format))
    }
}

/// Data type description – format strings
///
/// A data type is described using a format string. The format string only encodes information about the top-level type; for nested type, child types are described separately. Also, metadata is encoded in a separate string.
///
/// The format strings are designed to be easily parsable, even from a language such as C. The most common primitive formats have one-character format strings.
public enum ArrowDataType {
    case null
    case boolean
    case int8
    case uint8
    case int16
    case uint16
    case int32
    case uint32
    case int64
    case uint64
    case float16
    case float32
    case float64
    case binary
    case binaryLarge
    case utf8
    case utf8Large
    case date32
    case date64
    case time64

    public init?(_ rawValue: String) {
        switch rawValue {
        case "n": self = .null
        case "b": self = .boolean
        case "c": self = .int8
        case "C": self = .uint8
        case "s": self = .int16
        case "S": self = .uint16
        case "i": self = .int32
        case "I": self = .uint32
        case "l": self = .int64
        case "L": self = .uint64
        case "e": self = .float16
        case "f": self = .float32
        case "g": self = .float64
        case "z": self = .binary
        case "Z": self = .binaryLarge
        case "u": self = .utf8
        case "U": self = .utf8Large
        case "tdD": self = .date32
        case "tdm": self = .date64
        case "ttu": self = .time64


        //
        // [days]
        //
        // [milliseconds]
        //tts
        //time32 [seconds]
        //ttm
        //time32 [milliseconds]
        //ttu
        //time64 [microseconds]
        //ttn
        //time64 [nanoseconds]

        default: return nil
        }
    }

    var formatCode: String {
        switch self {
        case .null: return "n"
        case .boolean: return "b"
        case .int8: return "c"
        case .uint8: return "C"
        case .int16: return "s"
        case .uint16: return "S"
        case .int32: return "i"
        case .uint32: return "I"
        case .int64: return "l"
        case .uint64: return "L"
        case .float16: return "e"
        case .float32: return "f"
        case .float64: return "g"
        case .binary: return "z"
        case .binaryLarge: return "Z"
        case .utf8: return "u"
        case .utf8Large: return "U"
        case .date32: return "tdD"
        case .date64: return "tdm"
        case .time64: return "ttu"
        }
    }

    //    Supported Data Types
    //
    //    DataFusion uses Arrow, and thus the Arrow type system, for query execution. The SQL types from sqlparser-rs are mapped to Arrow types according to the following table
    //
    //    SQL Data Type    Arrow DataType
    //    CHAR    Utf8
    //    VARCHAR    Utf8
    //    UUID    Not yet supported
    //    CLOB    Not yet supported
    //    BINARY    Not yet supported
    //    VARBINARY    Not yet supported
    //    DECIMAL    Float64
    //    FLOAT    Float32
    //    SMALLINT    Int16
    //    INT    Int32
    //    BIGINT    Int64
    //    REAL    Float64
    //    DOUBLE    Float64
    //    BOOLEAN    Boolean
    //    DATE    Date32
    //    TIME    Time64(TimeUnit::Millisecond)
    //    TIMESTAMP    Date64
    //    INTERVAL    Not yet supported
    //    REGCLASS    Not yet supported
    //    TEXT    Not yet supported
    //    BYTEA    Not yet supported
    //    CUSTOM    Not yet supported
    //    ARRAY    Not yet supported
    public var sqlTypes: [String] {
        //    BOOLEAN    Boolean
        //    INT    Int32
        //    SMALLINT    Int16
        //    CHAR    Utf8
        //    VARCHAR    Utf8
        //    DECIMAL    Float64
        //    FLOAT    Float32
        //    BIGINT    Int64
        //    REAL    Float64
        //    DOUBLE    Float64
        //    DATE    Date32
        //    TIMESTAMP    Date64
        //    TODO: TIME    Time64(TimeUnit::Millisecond)
        switch self {
        case .null: return ["NULL"]
        case .boolean: return ["BOOLEAN"]
        case .utf8: return ["VARCHAR", "CHAR"]
        case .int16: return ["SMALLINT"]
        case .int32: return ["INT"]
        case .int64: return ["BIGINT"]
        case .float32: return ["FLOAT"]
        case .float64: return ["DECIMAL", "REAL"]
        case .date32: return ["DATE"]
        case .date64: return ["TIMESTAMP"]
        default: return [] // unsupported
        }
    }

    public var isSupported: Bool {
        !sqlTypes.isEmpty
    }

    // TODO: remaining complex codes

//d:19,10
//decimal128 [precision 19, scale 10]
//d:19,10,NNN
//decimal bitwidth = NNN [precision 19, scale 10]
//w:42
//fixed-width binary [42 bytes]
//Temporal types have multi-character format strings starting with t:
//
//Format string
//Arrow data type
//Notes
//tdD
//date32 [days]
//tdm
//date64 [milliseconds]
//tts
//time32 [seconds]
//ttm
//time32 [milliseconds]
//ttu
//time64 [microseconds]
//ttn
//time64 [nanoseconds]
//tss:...
//timestamp [seconds] with timezone “…”
//(1)
//tsm:...
//timestamp [milliseconds] with timezone “…”
//(1)
//tsu:...
//timestamp [microseconds] with timezone “…”
//(1)
//tsn:...
//timestamp [nanoseconds] with timezone “…”
//(1)
//tDs
//duration [seconds]
//tDm
//duration [milliseconds]
//tDu
//duration [microseconds]
//tDn
//duration [nanoseconds]
//tiM
//interval [months]
//tiD
//interval [days, time]
//Dictionary-encoded types do not have a specific format string. Instead, the format string of the base array represents the dictionary index type, and the value type can be read from the dependent dictionary array (see below “Dictionary-encoded arrays”).
//
//Nested types have multiple-character format strings starting with +. The names and types of child fields are read from the child arrays.
//
//Format string
//Arrow data type
//Notes
//+l
//list
//+L
//large list
//+w:123
//fixed-sized list [123 items]
//+s
//struct
//+m
//map
//(2)
//+ud:I,J,...
//dense union with type ids I,J…
//+us:I,J,...
//sparse union with type ids I,J…
}


// MARK: Utilities

@available(*, deprecated, message: "work in progress")
@inlinable func wip<T>(_ value: T) -> T {
    value
}
