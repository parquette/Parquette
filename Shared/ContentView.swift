//
//  ContentView.swift
//  Shared
//
//  Created by Marc Prud'hommeaux on 2/4/21.
//

import SwiftUI

struct ContentView: View {
    // @Binding var document: ParquetteDocument
    @ObservedObject var document: ParquetteDocument

    var body: some View {
        ParquetViewer(data: $document.data)
    }
}

struct ParquetViewer: View {
    @Binding var data: Data

    var body: some View {
        Text("Parquet data")
    }
}
