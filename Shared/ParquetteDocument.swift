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
    let tableName = "data"

    init() {
    }

    required init(configuration: ReadConfiguration) throws {

        guard let filename = configuration.file.filename else {
            throw CocoaError(.fileReadCorruptFile)
        }

        groupTitle = filename
        let url: URL! = nil

        print("opening file", configuration, filename, configuration.file)

        let doc = wip(NSDocumentController.shared.currentDirectory)
        print("### currentDocument", NSDocumentController.shared.currentDocument?.fileURL)
        print("### currentDirectory", NSDocumentController.shared.currentDirectory)
        print("### docs", NSDocumentController.shared.documents)

        let tmpFile = UUID().uuidString

        // we need to operate on a physical file, and NSFileWrapper doens't expose the underlying URL, so we cheat by copying the file into a temporary file and opening that one
        let tmpURL = URL(fileURLWithPath: tmpFile, relativeTo: URL(fileURLWithPath: NSTemporaryDirectory()))
            .appendingPathExtension("parquet")

        try configuration.file.write(to: tmpURL, options: .withNameUpdating, originalContentsURL: nil)

        print("### wrote to", tmpURL)

        switch configuration.contentType {
        case .parquette_parquet:
            try ctx.register(parquet: tmpURL, tableName: tableName)
        case .parquette_csv:
            try ctx.register(csv: tmpURL, tableName: tableName)
        default:
            throw ParquetteError.unknownContentType(configuration.contentType)
        }

        print("### opened", tmpURL)

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
