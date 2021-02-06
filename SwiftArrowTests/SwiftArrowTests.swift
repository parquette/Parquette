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

    func testQueryCSV() {
        //measure {
            demoExecutionContext(ext: "csv")
        //}
    }

    func testQueryParquet() {
        //measure {
            demoExecutionContext(ext: "parquet")
        //}
    }

    func testCSVDataFrame() {
        let block = { try! { XCTAssertNoThrow(try self.demoDataFrame(ext: "csv")) }() }
        // measure(block)
        block()
    }

    func testParquetDataFrame() {
        let block = { try! { XCTAssertNoThrow(try self.demoDataFrame(ext: "parquet")) }() }
        // measure(block)
        block()
    }

    func testSimpleQueries() throws {
        let ctx = DFExecutionContext()

        guard let df = try ctx.query(sql: "SELECT 1 AS INT") else {
            return XCTFail("unable to issue query")
        }

        XCTAssertEqual(1, try df.collectionCount())

        let array: DFArray = try df.arrayAt(index: 0)
        // XCTAssertEqual(1, try ctx.query(sql: "SELECT NOW()")?.collectionCount()) // doesn't work
    }

    func testCSVQueries() throws {
        let ctx = DFExecutionContext()

        try ctx.register(csv: sampleFile(ext: "csv", 1), tableName: "csv1")
        try ctx.register(csv: sampleFile(ext: "csv", 2), tableName: "csv2")
        try ctx.register(csv: sampleFile(ext: "csv", 3), tableName: "csv3")
        try ctx.register(csv: sampleFile(ext: "csv", 4), tableName: "csv4")
        try ctx.register(csv: sampleFile(ext: "csv", 5), tableName: "csv5")

        try ctx.register(parquet: sampleFile(ext: "parquet", 1), tableName: "parquet1")
        try ctx.register(parquet: sampleFile(ext: "parquet", 2), tableName: "parquet2")
        try ctx.register(parquet: sampleFile(ext: "parquet", 3), tableName: "parquet3")
        try ctx.register(parquet: sampleFile(ext: "parquet", 4), tableName: "parquet4")
        try ctx.register(parquet: sampleFile(ext: "parquet", 5), tableName: "parquet5")

        XCTAssertEqual(1_000, try ctx.query(sql: "SELECT * FROM csv1")?.collectionCount())
        XCTAssertEqual(1, try ctx.query(sql: "SELECT COUNT(*) FROM csv1")?.collectionCount())

        XCTAssertEqual(5, try ctx.query(sql: "SELECT * FROM csv1 WHERE first_name = 'Todd'")?.collectionCount())
        XCTAssertEqual(1, try ctx.query(sql: "SELECT * FROM csv1 WHERE first_name = 'Todd' AND last_name = 'Alvarez'")?.collectionCount())
        XCTAssertEqual(2, try ctx.query(sql: "SELECT * FROM csv1 WHERE email LIKE '%@whitehouse.gov'")?.collectionCount())
        XCTAssertEqual(25, try ctx.query(sql: "SELECT * FROM csv1 WHERE email LIKE '%@___.gov'")?.collectionCount()) // e.g., epa.gov

        // unable to exeecute SQL…
        // XCTAssertEqual(1_000, try ctx.query(sql: "SELECT * FROM csv1, csv2 WHERE csv1.first_name = csv2.first_name")?.collectionCount())
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

