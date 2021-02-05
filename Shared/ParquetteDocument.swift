//
//  ParquetteDocument.swift
//  Shared
//
//  Created by Marc Prud'hommeaux on 2/4/21.
//

import SwiftUI
import UniformTypeIdentifiers
import SwiftArrow

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
    @Published var data: Data

    init(data: Data = .init()) {
        self.data = data
    }


    required init(configuration: ReadConfiguration) throws {
        guard let data = configuration.file.regularFileContents else {
            throw CocoaError(.fileReadCorruptFile)
        }
        self.data = data
    }

    func snapshot(contentType: UTType) throws -> Data {
        data
    }

    func fileWrapper(snapshot: Data, configuration: WriteConfiguration) throws -> FileWrapper {
        .init(regularFileWithContents: snapshot)
    }
}
