//
//  ParquetteDocument.swift
//  Shared
//
//  Created by Marc Prud'hommeaux on 2/4/21.
//

import SwiftUI
import UniformTypeIdentifiers
import SwiftArrow

public enum ParquetteError : LocalizedError {
    case writeNotSupported
    case unknownContentType(UTType)

    public var failureReason: String? {
        switch self {
        case .writeNotSupported:
            return NSLocalizedString("Write is not supported", comment: "")
        case .unknownContentType:
            return NSLocalizedString("Unknown content type", comment: "")
        }
    }

    public var recoverySuggestion: String? {
        switch self {
        case .writeNotSupported:
            return NSLocalizedString("Parquette currently only supports reading files", comment: "")
        case .unknownContentType:
            return NSLocalizedString("Save as a supported content type", comment: "")
        }
    }
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
    static var readableContentTypes: [UTType] { [.parquette_parquet, .parquette_csv] }
    static var writableContentTypes: [UTType] { [] }

    @Published var groupTitle = ""

    let ctx = DFExecutionContext()
    let tableName = "table"

    init() {
    }

    required init(configuration: ReadConfiguration) throws {

        guard let filename = configuration.file.filename else {
            throw CocoaError(.fileReadCorruptFile)
        }

        groupTitle = filename
        let url: URL! = nil

        print("opening file", configuration, filename, configuration.file) // , NSApp.currentEvent?.window?.windowController?.document)

        if let url = url {
            switch configuration.contentType {
            case .parquette_parquet:
                try ctx.register(parquet: url, tableName: tableName)
            case .parquette_csv:
                try ctx.register(csv: url, tableName: tableName)
            default:
                throw ParquetteError.unknownContentType(configuration.contentType)
            }
        }
    }

    func snapshot(contentType: UTType) throws -> Void {
    }

    func fileWrapper(snapshot: Void, configuration: WriteConfiguration) throws -> FileWrapper {
        if let file = configuration.existingFile {
            return file
        }

        throw ParquetteError.writeNotSupported
    }
}
