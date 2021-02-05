//
//  ParquetteApp.swift
//  Shared
//
//  Created by Marc Prud'hommeaux on 2/4/21.
//

import SwiftUI

@main
struct ParquetteApp: App {
    var body: some Scene {
        DocumentGroup(newDocument: ParquetteDocument()) { file in
            ContentView(document: file.$document)
        }
    }
}
