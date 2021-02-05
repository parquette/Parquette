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
        HSplitView {
            List() {
                Section(header: Text(document.groupTitle)) {
                    Text("Colum 1")
                    Text("Colum 2")
                    Text("Colum 3")
                    Text("Colum 4")
                    Text("Colum 5")
                }
            }
            .listStyle(SidebarListStyle())
            .frame(minWidth: 140) // same as Mail.app
            .layoutPriority(0.5)

            ParquetViewer(document: document)
                .layoutPriority(1)
        }
    }
}

struct ParquetViewer: View {
    @ObservedObject var document: ParquetteDocument
    @State var sql = ""
    @Environment(\.font) var font

    var body: some View {
        VStack {
            DataTableView()

            HStack {
                TextField(NSLocalizedString("SELECT * FROM table", comment: ""), text: $sql, onCommit: performQuery)
                    .font(font?.monospacedDigit())
                Button(action: performQuery) {
                    Label(NSLocalizedString("Execute", comment: ""), systemImage: "play.fill")
                        .labelStyle(IconOnlyLabelStyle())
                }
                // .keyboardShortcut(KeyEquivalent.return)
            }
        }
    }

    func performQuery() {
        print("performQuery")
    }
}


struct DataTableView : NSViewRepresentable {
    typealias NSViewType = NSScrollView
    
    func makeNSView(context: Context) -> NSViewType {
        let tableView = NSTableView()
        tableView.dataSource = context.coordinator
        tableView.delegate = context.coordinator

        let scrollView = NSScrollView()
        scrollView.documentView = tableView
        scrollView.focusRingType = .none
        return scrollView
    }

    func updateNSView(_ view: NSViewType, context: Context) {
        let tableView = view.documentView as! NSTableView

        if tableView.tableColumns.isEmpty {
            let col = NSTableColumn(identifier: NSUserInterfaceItemIdentifier("COL1"))
            col.title = "Column 1"
            tableView.addTableColumn(col)
        }
    }

    static func dismantleNSView(_ view: NSViewType, coordinator: Coordinator) {
        let tableView = view.documentView as! NSTableView

        tableView.dataSource = nil
        tableView.delegate = nil
    }

    func makeCoordinator() -> Coordinator {
        Coordinator()
    }

    final class Coordinator : NSObject, NSTableViewDelegate, NSTableViewDataSource {
        override init() {

        }

        func tableView(_ tableView: NSTableView, objectValueFor tableColumn: NSTableColumn?, row: Int) -> Any? {
            "XXX"
        }

        func numberOfRows(in tableView: NSTableView) -> Int {
            10
        }
    }
}
