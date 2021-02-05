//
//  ContentView.swift
//  Shared
//
//  Created by Marc Prud'hommeaux on 2/4/21.
//

import SwiftUI

struct ContentView: View {
    @ObservedObject var document: ParquetteDocument

    var body: some View {
        ParquetViewer(data: $document.data)
    }
}

struct ParquetViewer: View {
    @Binding var data: Data

    var body: some View {
        TextEditor(text: .constant("Parquet data"))
    }
}
