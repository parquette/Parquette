//
//  ParquetteDocument.swift
//  Shared
//
//  Created by Marc Prud'hommeaux on 2/4/21.
//

import SwiftUI
import UniformTypeIdentifiers
import SwiftArrow

public enum ParquetteError : Error {
    case writeNotSupported
}

extension UTType {
    static var parquette_parquet: UTType {
        UTType(importedAs: "net.parquette.format.parquet")
    }

    static var parquette_csv: UTType {
        UTType(importedAs: "net.parquette.format.csv")
    }
}

final class ParquetteDocument: ReferenceFileDocument {
    static var readableContentTypes: [UTType] { [.parquette_parquet] }

    let ctx = DFExecutionContext()
    let tableName = "table"

    init() {
    }

    required init(configuration: ReadConfiguration) throws {
        guard let filename = configuration.file.filename else {
            throw CocoaError(.fileReadCorruptFile)
        }

        print("opening file", filename)
        // ctx.register(parquet: configuration.file.filename, tableName: tableName)
    }

    func snapshot(contentType: UTType) throws -> Void {
    }

    func fileWrapper(snapshot: Void, configuration: WriteConfiguration) throws -> FileWrapper {
        throw ParquetteError.writeNotSupported
    }
}
