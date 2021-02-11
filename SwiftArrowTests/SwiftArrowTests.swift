//
//  SwiftArrowTests.swift
//  SwiftArrowTests
//
//  Created by Marc Prud'hommeaux on 1/25/21.
//

import XCTest
import SwiftArrow
import MiscKit

/// Whether to run additional measurement & stress tests
let stressTest = false

class SwiftArrowTests: XCTestCase {
    func sampleFile(ext: String, _ index: Int = 1) throws -> URL {
        let url = URL(fileURLWithPath: #file)
            .deletingLastPathComponent()
            .appendingPathComponent("../arcolyte/test/data/\(ext)/userdata\(index).\(ext)")
        return try checkURL(url)
    }

    func checkURL(_ url: URL) throws -> URL {
        if !FileManager.default.fileExists(atPath: url.path) {
            throw SwiftArrowError.missingFileError(url: url)
        }
        return url
    }

    func testLoadArrow() throws {
        for i in 1...5 {
            let url = try sampleFile(ext: "csv", i)
            dbg("loading url:", url.lastPathComponent)
            XCTAssertNoThrow(try ArrowCSV(fileURL: url).load())
        }

        XCTAssertThrowsError(try ArrowCSV(fileURL: URL(fileURLWithPath: "/DOES_NOT_EXIST")).load())
    }

    func testAsyncOperation() throws {
        class LifetimeExample {
            let sema: DispatchSemaphore
            init(_ sema: DispatchSemaphore) {
                self.sema = sema
                dbg("start of test lifetime")
            }

            deinit {
                dbg("end of test lifetime")
            }

            func completed(_ success: Bool) {
                dbg("the async operation has completed with result \(success)")
                sema.signal()
            }
        }

        func startOperation(_ sema: DispatchSemaphore) {
            let test = LifetimeExample(sema)
            dbg("starting async operation")
            invokeCallbackBool(millis: 1) { [test] success in
                test.completed(success)
            }
        }

        let semaphore = DispatchSemaphore(value: 0)
        startOperation(semaphore)
        semaphore.wait()
    }

    func testMultiAsync() {
        let count = 20
        let xpc = (0..<count).map({ expectation(description: "expect #\($0)") })

        let maxTime: UInt64 = 1000

        DispatchQueue.concurrentPerform(iterations: count) { i in
            // invokeCallbackBool creates a new thread each time, so don't have too many concurrent operations at once
            invokeCallbackBool(millis: UInt64.random(in: 1...maxTime)) { success in
                xpc[i].fulfill()
            }
        }

        wait(for: xpc, timeout: .init(2 * maxTime) / 1000)
    }

    func testBadFiles() {
        let ctx = DFExecutionContext()
        XCTAssertThrowsError(try ctx.load(csv: URL(fileURLWithPath: "/nonexistant/path")))
        XCTAssertThrowsError(try ctx.load(parquet: URL(fileURLWithPath: "/nonexistant/path")))
        XCTAssertThrowsError(try ctx.register(csv: URL(fileURLWithPath: "/nonexistant/path"), tableName: "x"))
        XCTAssertThrowsError(try ctx.register(parquet: URL(fileURLWithPath: "/nonexistant/path"), tableName: "x"))
    }

    func testQueryCSV() throws {
        try mmeasure {
            demoExecutionContext(ext: "csv")
        }
    }

    func testQueryParquet() throws {
        try mmeasure {
            demoExecutionContext(ext: "parquet")
        }
    }

    func testCSVDataFrame() throws {
        try mmeasure {
            XCTAssertNoThrow(try self.demoDataFrame(ext: "csv"))
        }
    }

    // FIXME: thread '<unnamed>' panicked at 'called `Result::unwrap()` on an `Err` value: CDataInterface("The datatype \"Timestamp(Nanosecond, None)\" is still not supported in Rust implementation")', src/arrowz.rs:614:79
    func XXXtestParquetDataFrame() throws {
        try mmeasure {
            XCTAssertNoThrow(try self.demoDataFrame(ext: "parquet"))
        }
    }

    func testSimpleQueries() throws {
        try simpleQueryTest(.utf8)
        try simpleQueryTest(.boolean)
        try simpleQueryTest(.int16)
        try simpleQueryTest(.int32)
        try simpleQueryTest(.int64)
//        try simpleQueryTest(.float32)
//        try simpleQueryTest(.float64)
//        try simpleQueryTest(.date32)
//        try simpleQueryTest(.date64)

    }

    /// https://github.com/apache/arrow/blob/master/rust/datafusion/README.md#supported-data-types
    func simpleQueryTest(_ type: ArrowDataType) throws {
        let ctx = DFExecutionContext()

        let sql: String
        guard let sqlType = type.sqlTypes.first else {
            return XCTFail("unhandled type: \(type)")
        }
        switch type {
        case .utf8: sql = "select '1'"
        case .boolean: sql = "select CAST (1 AS \(sqlType))"
        case .int16: sql = "select CAST (1 AS \(sqlType))"
        case .int32: sql = "select CAST (1 AS \(sqlType))"
        case .int64: sql = "select CAST (1 AS \(sqlType))"
        case .float32: sql = "select CAST (1 AS \(sqlType))"
        case .float64: sql = "select CAST (1 AS \(sqlType))"
        default: return XCTFail("unhandled type: \(type)")
        }

        guard let df = try ctx.query(sql: sql) else {
            return XCTFail("unable to issue query")
        }

        XCTAssertEqual(1, try df.collectionCount())

        let schemaArray: ArrowVector = try df.collectVector(index: 0)
        // dbg(schemaArray.array.debugDescription)

        XCTAssertEqual(schemaArray.bufferCount, type == .utf8 ? 3 : 2)
        XCTAssertEqual(schemaArray.count, 1)
        XCTAssertEqual(schemaArray.nullCount, 0)
        XCTAssertEqual(schemaArray.offset, 0)
        XCTAssertEqual(schemaArray.arrayChildCount, 0)
    }

    func checkArrowType<T: ArrowDataRepresentable>(ctx: DFExecutionContext = DFExecutionContext(), _ type: ArrowDataType, sqlValue: String = "NULL", sql literalSQL: String? = nil) throws -> [T?] {
        guard let sqlType = type.sqlTypes.first else {
            XCTFail("no SQL type")
            throw SwiftArrowError.general
        }

        // "VALUES" syntax doesn't seem to work
        // VALUES (1, 'one'), (2, 'two'), (3, 'three')
        // let sql = "VALUES (CAST (\(sqlValue) AS \(sqlType)))"

        let sql = literalSQL ?? "select CAST (\(sqlValue) AS \(sqlType)) as COL"

        //dbg("executing", sql)

        guard let df = try ctx.query(sql: sql) else {
            XCTFail("no return frame")
            throw SwiftArrowError.general
        }

        let vector = try df.collectVector(index: 0)

        // “The number of children is a function of the data type, as described in the Columnar format specification.” http://arrow.apache.org/docs/format/Columnar.html#format-columnar
        XCTAssertEqual(type == .utf8 ? 3 : 2, vector.bufferCount)
        // XCTAssertEqual(1, vector.count)
        XCTAssertEqual(0, vector.arrayChildCount)
        XCTAssertEqual(0, vector.offset)
        XCTAssertEqual(0, vector.flags)
        XCTAssertEqual(nil, vector.name)
        XCTAssertEqual(type, vector.dataType)
        XCTAssertEqual(nil, vector.metadata)

        return Array(try T.BufferView(vector: vector))
    }

    func testFusionDataTypes() throws {
        // Debug stressTest=true: 135.419 seconds
        // Release stressTest=true: 29.676 seconds

        let ctx = try loadedContext(csv: 1...5, parquet: 1...5)

        /// Returns an array of the results of the string query
        func stringQL(_ sql: String) throws -> [String?] {
            try checkArrowType(ctx: ctx, .utf8, sql: sql) as [String?]
        }

        // check data columns
        try results { _ in
            XCTAssertEqual(Array<Int32>(1...1_000), try checkArrowType(ctx: ctx, .int32, sql: "select id from parquet1"))
        }

        XCTAssertEqual(Array<Double>([49756.53, 150280.17]), try checkArrowType(ctx: ctx, .float64, sql: "select salary from parquet1 LIMIT 2"))

        XCTAssertEqual(["Amanda", "Albert", "Evelyn"], try checkArrowType(ctx: ctx, .utf8, sql: "select first_name from parquet1 LIMIT 3"))


        XCTAssertEqual(["\u{202b}test\u{202b}"], try stringQL("select comments from parquet1 where email = 'sadams2p@imdb.com'"), "unicode support")

        XCTAssertEqual(["̡͓̞ͅI̗̘̦͝n͇͇͙v̮̫ok̲̫̙͈i̖͙̭̹̠̞n̡̻̮̣̺g̲͈͙̭͙̬͎ ̰t͔̦h̞̲e̢̤ ͍̬̲͖f̴̘͕̣è͖ẹ̥̩l͖͔͚i͓͚̦͠n͖͍̗͓̳̮g͍ ̨o͚̪͡f̘̣̬ ̖̘͖̟͙̮c҉͔̫͖͓͇͖ͅh̵̤̣͚͔á̗̼͕ͅo̼̣̥s̱͈̺̖̦̻͢.̛̖̞̠̫̰"], try stringQL("select comments from parquet1 where email = 'wweaver2r@google.de'"), "unicode support")

        XCTAssertEqual(["社會科學院語學研究所"], try stringQL("select comments from parquet1 where ip_address = '250.178.192.2'"), "unicode support")

        try results { _ in
            for format in [
                "parquet",
                "csv",
            ].shuffled() {
                let isCSV = format == "csv"

                XCTAssertEqual(198, Set(try stringQL("select first_name from \(format)1")).count)
                XCTAssertEqual(247, Set(try stringQL("select last_name from \(format)1")).count)
                XCTAssertEqual(985, Set(try stringQL("select email from \(format)1")).count)
                XCTAssertEqual(isCSV ? 7 : 3, Set(try stringQL("select gender from \(format)1")).count)
                XCTAssertEqual(1000, Set(try stringQL("select ip_address from \(format)1")).count)
                XCTAssertEqual(120, Set(try stringQL("select country from \(format)1")).count)
                XCTAssertEqual(isCSV ? 787 : 788, Set(try stringQL("select birthdate from \(format)1")).count)
                XCTAssertEqual(isCSV ? 181 : 182, Set(try stringQL("select title from \(format)1")).count)
                XCTAssertEqual(isCSV ? 88 : 85, Set(try stringQL("select comments from \(format)1")).count)

                XCTAssertEqual(199, Set(try stringQL("select first_name from \(format)2")).count)
                XCTAssertEqual(242, Set(try stringQL("select last_name from \(format)2")).count)
                XCTAssertEqual(979, Set(try stringQL("select email from \(format)2")).count)
                XCTAssertEqual(3, Set(try stringQL("select gender from \(format)2")).count)
                XCTAssertEqual(1000, Set(try stringQL("select ip_address from \(format)2")).count)
                XCTAssertEqual(129, Set(try stringQL("select country from \(format)2")).count)
                XCTAssertEqual(775, Set(try stringQL("select birthdate from \(format)2")).count)
                XCTAssertEqual(179, Set(try stringQL("select title from \(format)2")).count)
                XCTAssertEqual(isCSV ? 91 : 87, Set(try stringQL("select comments from \(format)2")).count)

                XCTAssertEqual(201, Set(try stringQL("select first_name from \(format)3")).count)
                XCTAssertEqual(246, Set(try stringQL("select last_name from \(format)3")).count)
                XCTAssertEqual(977, Set(try stringQL("select email from \(format)3")).count)
                XCTAssertEqual(3, Set(try stringQL("select gender from \(format)3")).count)
                XCTAssertEqual(1000, Set(try stringQL("select ip_address from \(format)3")).count)
                XCTAssertEqual(131, Set(try stringQL("select country from \(format)3")).count)
                XCTAssertEqual(764, Set(try stringQL("select birthdate from \(format)3")).count)
                XCTAssertEqual(180, Set(try stringQL("select title from \(format)3")).count)
                XCTAssertEqual(isCSV ? 93 : 89, Set(try stringQL("select comments from \(format)3")).count)

                XCTAssertEqual(199, Set(try stringQL("select first_name from \(format)4")).count)
                XCTAssertEqual(247, Set(try stringQL("select last_name from \(format)4")).count)
                XCTAssertEqual(984, Set(try stringQL("select email from \(format)4")).count)
                XCTAssertEqual(3, Set(try stringQL("select gender from \(format)4")).count)
                XCTAssertEqual(1000, Set(try stringQL("select ip_address from \(format)4")).count)
                XCTAssertEqual(119, Set(try stringQL("select country from \(format)4")).count)
                XCTAssertEqual(774, Set(try stringQL("select birthdate from \(format)4")).count)
                XCTAssertEqual(175, Set(try stringQL("select title from \(format)4")).count)
                XCTAssertEqual(isCSV ? 96 : 92, Set(try stringQL("select comments from \(format)4")).count)

                XCTAssertEqual(200, Set(try stringQL("select first_name from \(format)5")).count)
                XCTAssertEqual(243, Set(try stringQL("select last_name from \(format)5")).count)
                XCTAssertEqual(980, Set(try stringQL("select email from \(format)5")).count)
                XCTAssertEqual(3, Set(try stringQL("select gender from \(format)5")).count)
                XCTAssertEqual(1000, Set(try stringQL("select ip_address from \(format)5")).count)
                XCTAssertEqual(129, Set(try stringQL("select country from \(format)5")).count)
                XCTAssertEqual(784, Set(try stringQL("select birthdate from \(format)5")).count)
                XCTAssertEqual(181, Set(try stringQL("select title from \(format)5")).count)
                XCTAssertEqual(isCSV ? 93 : 90, Set(try stringQL("select comments from \(format)5")).count)
            }
        }

        // check ordering
        try results { _ in
            XCTAssertEqual(Array<Int32>(1...1_000).reversed(), try checkArrowType(ctx: ctx, .int32, sql: "select id from parquet1 order by id desc"))
        }


        func checkArrowValues<T: ArrowDataRepresentable & Equatable>(_ makeValue: @autoclosure () -> T) throws {
            try results { _ in
                XCTAssertEqual([T?.none], try checkArrowType(ctx: ctx, T.arrowDataType, sqlValue: "NULL"))
                let val = makeValue()
                XCTAssertEqual([val], try checkArrowType(ctx: ctx, T.arrowDataType, sqlValue: "\(val)"))
            }
        }

        try checkArrowValues(Int16.random(in: (.min)...(.max)))
        // try checkArrowValues(UInt16.random(in: (.min)...(.max))) // no SQL type
        try checkArrowValues(Int32.random(in: (.min)...(.max)))
        // try checkArrowValues(UInt32.random(in: (.min)...(.max))) // no SQL type
        try checkArrowValues(Int64.random(in: (.min)...(.max)))
        // try checkArrowValues(UInt64.random(in: (.min)...(.max))) // no SQL type

        try checkArrowValues(Double.random(in: (-9_999_999)...(+9_999_999)))
        //try checkArrowValues(Float.random(in: (-9_999_999)...(+9_999_999)))
    }

    @discardableResult func results<T>(count: Int = stressTest ? 999 : 1, concurrent: Bool = true, block: (Int) throws -> T) throws -> [T] {
        try (1...count).qmap(concurrent: concurrent, block: block)
    }

    /// Creates a context with various sample data registered.
    func loadedContext(csv csvTableRange: ClosedRange<Int>? = nil, parquet parquetTableRange: ClosedRange<Int>? = nil) throws -> DFExecutionContext {
        let ctx = DFExecutionContext()

        if let csv = csvTableRange {
            for i in csv {
                try ctx.register(csv: sampleFile(ext: "csv", i), tableName: "csv\(i)")
            }
        }

        if let parquet = parquetTableRange {
            for i in parquet {
                try ctx.register(parquet: sampleFile(ext: "parquet", i), tableName: "parquet\(i)")
            }
        }

        return ctx
    }

    func testJoinQueries() throws {
        let ctx = try loadedContext(csv: 3...4)
        XCTAssertEqual(1_000, try ctx.query(sql: "SELECT * FROM csv3")?.collectionCount())
        XCTAssertEqual(1_000, try ctx.query(sql: "SELECT * FROM csv4")?.collectionCount())
        // XCTAssertEqual(1_000 * 1_000, try ctx.query(sql: "SELECT * FROM csv3, csv4")?.collectionCount()) // not yet, apparently…
    }

    func testCSVQueries() throws {
        let ctx = try loadedContext(csv: 1...5, parquet: 1...5)

        XCTAssertEqual(1_000, try ctx.query(sql: "SELECT * FROM csv1")?.collectionCount())
        XCTAssertEqual(1, try ctx.query(sql: "SELECT COUNT(*) FROM csv1")?.collectionCount())

        XCTAssertEqual(5, try ctx.query(sql: "SELECT * FROM csv1 WHERE first_name = 'Todd'")?.collectionCount())
        XCTAssertEqual(1, try ctx.query(sql: "SELECT * FROM csv1 WHERE first_name = 'Todd' AND last_name = 'Alvarez'")?.collectionCount())
        XCTAssertEqual(2, try ctx.query(sql: "SELECT * FROM csv1 WHERE email LIKE '%@whitehouse.gov'")?.collectionCount())
        XCTAssertEqual(25, try ctx.query(sql: "SELECT * FROM csv1 WHERE email LIKE '%@___.gov'")?.collectionCount()) // e.g., epa.gov

        do {
            guard let vector = try ctx.query(sql: "SELECT first_name FROM csv1 WHERE first_name = 'Todd'")?.collectVector(index: 0) else {
                return XCTFail("could not execute query")
            }
            XCTAssertEqual(ArrowDataType.utf8, vector.dataType)
        }
    }

    func checkColumnType(_ ctx: DFExecutionContext, column: String, dataType: ArrowDataType, table: String) throws {
        guard let vector = try ctx.query(sql: "SELECT \(column) FROM \(table)")?.collectVector(index: 0) else {
            return XCTFail("could not execute query")
        }

        XCTAssertEqual(dataType, vector.dataType)
    }

    func testCSVDataTypes() throws {
        for i in 1...5 {
            try testCSVDataType(index: i)
        }
    }

    func testCSVDataType(index: Int) throws {

        let tbl = "csv\(index)"
        let ctx = try loadedContext(csv: index...index)

        // note that the types may differ due to different type inference
        try checkColumnType(ctx, column: "registration_dttm", dataType: .utf8, table: tbl)
        try checkColumnType(ctx, column: "id", dataType: index == 1 ? .int64 : index == 2 ? .utf8 : .int64, table: tbl)
        try checkColumnType(ctx, column: "first_name", dataType: .utf8, table: tbl)
        try checkColumnType(ctx, column: "last_name", dataType: .utf8, table: tbl)
        try checkColumnType(ctx, column: "email", dataType: .utf8, table: tbl)
        try checkColumnType(ctx, column: "gender", dataType: .utf8, table: tbl)
        try checkColumnType(ctx, column: "ip_address", dataType: .utf8, table: tbl)
        try checkColumnType(ctx, column: "cc", dataType: .int64, table: tbl)
        try checkColumnType(ctx, column: "country", dataType: .utf8, table: tbl)
        try checkColumnType(ctx, column: "birthdate", dataType: .utf8, table: tbl)
        try checkColumnType(ctx, column: "salary", dataType: index == 1 ? .float64 : index == 2 ? .utf8 : .float64, table: tbl)
        try checkColumnType(ctx, column: "title", dataType: .utf8, table: tbl)
        try checkColumnType(ctx, column: "comments", dataType: .utf8, table: tbl)
    }

    /// Perform queries against the taxi data from https://cran.r-project.org/web/packages/arrow/vignettes/dataset.html
    /// and https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
    /// This is too big (~350mb) to commit to git, so only run the test if the file is present
    func testTaxiData() throws {
        let dataFile = "taxidata_2009.parquet"
        let expectedCount: Int64 = 14_092_413 // 14M column elements

        let url = URL(fileURLWithPath: dataFile, relativeTo: FileManager.default.urls(for: .downloadsDirectory, in: .userDomainMask).first)

        if !FileManager.default.fileExists(atPath: url.path) {
            throw XCTSkip("skipping absent \(dataFile)")
        }

        let ctx = DFExecutionContext()
        try ctx.register(parquet: url, tableName: "taxidata")

        do {
            let sql = "select count(payment_type) from taxidata"
            guard let result = try ctx.query(sql: sql) else {
                return XCTFail("no results")
            }

            let vec = try result.collectVector(index: 0)
            XCTAssertEqual(expectedCount, try Int64.BufferView(vector: vec)[0])
        }

        do {
            // 431 batches
            let sql = "select payment_type from taxidata"
            guard let result = try ctx.query(sql: sql) else {
                return XCTFail("no results")
            }

            XCTAssertEqual(.init(expectedCount), try result.collectionCount())
        }
    }

    func testParquetDataTypes() throws {
        for i in 1...5 {
            try testParquetDataType(index: i)
        }
    }

    func testParquetDataType(index: Int) throws {

        let tbl = "parquet\(index)"
        let ctx = try loadedContext(parquet: index...index)

        // try checkColumnType(column: "registration_dttm", dataType: .utf8) // “The datatype \"Timestamp(Nanosecond, None)\" is still not supported in Rust implementation"
        try checkColumnType(ctx, column: "id", dataType: .int32, table: tbl)
        try checkColumnType(ctx, column: "first_name", dataType: .utf8, table: tbl)
        try checkColumnType(ctx, column: "last_name", dataType: .utf8, table: tbl)
        try checkColumnType(ctx, column: "email", dataType: .utf8, table: tbl)
        try checkColumnType(ctx, column: "gender", dataType: .utf8, table: tbl)
        try checkColumnType(ctx, column: "ip_address", dataType: .utf8, table: tbl)
        try checkColumnType(ctx, column: "cc", dataType: .utf8, table: tbl)
        try checkColumnType(ctx, column: "country", dataType: .utf8, table: tbl)
        try checkColumnType(ctx, column: "birthdate", dataType: .utf8, table: tbl)
        try checkColumnType(ctx, column: "salary", dataType: .float64, table: tbl)
        try checkColumnType(ctx, column: "title", dataType: .utf8, table: tbl)
        try checkColumnType(ctx, column: "comments", dataType: .utf8, table: tbl)
    }

    func demoDataFrame(ext: String) throws -> [DFDataFrame] {
        var frames: [DFDataFrame] = []
        let ctx = DFExecutionContext()
        for i in 0..<5 {
            let index = i + 1
            switch ext {
            case "csv":
                if let df = try ctx.load(csv: sampleFile(ext: ext, index)) {
                    frames.append(df)
                }
            case "parquet":
                if let df = try ctx.load(parquet: sampleFile(ext: ext, index)) {
                    frames.append(df)
                }
            default:
                XCTFail("unknown extension \(ext)")
            }
        }

        var counts: [Int] = []

        for frame in frames {
            var df = frame
            for _ in 1...Int.random(in: 3...5) {
                counts.append(try df.collectionCount())
                df = try df.limit(count: .random(in: 1...999))
            }
        }

        // make sure the counts are in descending order
        // XCTAssertEqual(counts, counts.sorted().reversed()) // note: previous limit overrides later limit

        return frames
    }

    func demoExecutionContext(ext: String, num: Int = 5, queries: Int = 10) {
        let ctx = DFExecutionContext()

        DispatchQueue.concurrentPerform(iterations: num) { i in
            let index = i + 1 // csv files go from 1...5
            // dbg("registering source", index)
            do {
                switch ext {
                case "csv":
                    try ctx.register(csv: sampleFile(ext: ext, index), tableName: "\(ext)\(index)")
                case "parquet":
                    try ctx.register(parquet: sampleFile(ext: ext, index), tableName: "\(ext)\(index)")
                default:
                    return XCTFail("unknown extension \(ext)")
                }
            } catch {
                return XCTFail("error: \(error)")
            }
        }

        // perform a bunch of queries on multiple threads and ensure they all return 1,000 rows (which each of the sample data frames implement)
        DispatchQueue.concurrentPerform(iterations: queries) { i in
            do {
                let countAggCount = try ctx.query(sql: "select count(*) from \(ext)\(Int.random(in: 1...num))")?.collectionCount()
                XCTAssertEqual(1, countAggCount)

//                let allRecordsCount = try ctx.query(sql: "select * from \(ext)\(Int.random(in: 1...num))")?.collectionCount()
//                XCTAssertEqual(1_000, allRecordsCount)

            } catch {
                return XCTFail("error: \(error)")
            }
        }
    }
}

extension DFDataFrame {
    /// Executes the DataFrame and returns the Nth column
    @inlinable public func collectVectors<R: RangeExpression>(index: UInt, batch: R) throws -> ArraySlice<ArrowVector> where R.Bound == Int {
        try collectResults().columnSets[Int(index)].batches[batch]
    }

    /// Executes the DataFrame and returns the Nth column
    @inlinable public func collectVector(index: UInt) throws -> ArrowVector {
        try collectVectors(index: index, batch: 0...0)[0]
    }

    /// Executes the DataFrame and returns the count
    @inlinable func collectionCount() throws -> Int {
        try collectVectors(index: 0, batch: 0...).map(\.bufferLength).reduce(0, +)
    }
}


#if canImport(XCTest)
extension XCTestCase {
    /// Measures the block, other once or multiple times, depending on whether `stressTest` is true.
    public func mmeasure(_ block: () throws -> ()) throws {
        if stressTest {
            var errors: [Error] = []
            measure {
                do {
                    try block()
                } catch {
                    errors.append(error)
                }
            }

            if let error = errors.first {
                throw error
            }
        } else {
            try block()
        }
    }
}
#endif
