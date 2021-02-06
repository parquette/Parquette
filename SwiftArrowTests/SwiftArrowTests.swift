//
//  SwiftArrowTests.swift
//  SwiftArrowTests
//
//  Created by Marc Prud'hommeaux on 1/25/21.
//

import XCTest
@testable import SwiftArrow

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

    let stressTest = true

    /// Measures the block, other once or multiple times, depending on whether `stressTest` is true.
    func mmeasure(_ block: () throws -> ()) throws {
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

    func testParquetDataFrame() throws {
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

        let schemaArray: ArrowSchemaArray = try df.arrayAt(index: 0)
        // dbg(schemaArray.array.debugDescription)

        XCTAssertEqual(schemaArray.array.pointee.n_buffers, type == .utf8 ? 3 : 2)
        XCTAssertEqual(schemaArray.array.pointee.length, 1)
        XCTAssertEqual(schemaArray.array.pointee.null_count, 0)
        XCTAssertEqual(schemaArray.array.pointee.offset, 0)
        XCTAssertEqual(schemaArray.array.pointee.n_children, 0)

        XCTAssertEqual(schemaArray.schema.pointee.n_children, 0)

        if let fmt = schemaArray.schema.pointee.format {
            XCTAssertEqual(type, ArrowDataType(String(cString: fmt)))
        }

        if let md = schemaArray.schema.pointee.metadata {
            XCTAssertEqual(String(cString: md), "")
        }
        if let nm = schemaArray.schema.pointee.name {
            XCTAssertEqual(String(cString: nm), "")
        }

        // XCTAssertEqual(1, try ctx.query(sql: "SELECT NOW()")?.collectionCount()) // doesn't work
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

    func testCSVQueries() throws {
        let ctx = try loadedContext(csv: 1...5, parquet: 1...5)

        XCTAssertEqual(1_000, try ctx.query(sql: "SELECT * FROM csv1")?.collectionCount())
        XCTAssertEqual(1, try ctx.query(sql: "SELECT COUNT(*) FROM csv1")?.collectionCount())

        XCTAssertEqual(5, try ctx.query(sql: "SELECT * FROM csv1 WHERE first_name = 'Todd'")?.collectionCount())
        XCTAssertEqual(1, try ctx.query(sql: "SELECT * FROM csv1 WHERE first_name = 'Todd' AND last_name = 'Alvarez'")?.collectionCount())
        XCTAssertEqual(2, try ctx.query(sql: "SELECT * FROM csv1 WHERE email LIKE '%@whitehouse.gov'")?.collectionCount())
        XCTAssertEqual(25, try ctx.query(sql: "SELECT * FROM csv1 WHERE email LIKE '%@___.gov'")?.collectionCount()) // e.g., epa.gov


        do {
            guard let array = try ctx.query(sql: "SELECT first_name FROM csv1 WHERE first_name = 'Todd'")?.arrayAt(index: 0) else {
                return XCTFail("could not execute query")
            }
            XCTAssertEqual(ArrowDataType.utf8, array.schema.pointee.dataType)
        }

    }

    func checkColumnType(_ ctx: DFExecutionContext, column: String, dataType: ArrowDataType, table: String) throws {
        guard let array = try ctx.query(sql: "SELECT \(column) FROM \(table)")?.arrayAt(index: 0) else {
            return XCTFail("could not execute query")
        }

        XCTAssertEqual(dataType, array.schema.pointee.dataType)
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

        var counts: [UInt] = []

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

                let allRecordsCount = try ctx.query(sql: "select * from \(ext)\(Int.random(in: 1...num))")?.collectionCount()
                XCTAssertEqual(1_000, allRecordsCount)

            } catch {
                return XCTFail("error: \(error)")
            }
        }
    }
}

