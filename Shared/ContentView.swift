//
//  ContentView.swift
//  Shared
//
//  Created by Marc Prud'hommeaux on 2/4/21.
//

import SwiftUI

let colcount = 7

@available(*, deprecated, message: "work in progress")
func wip<T>(_ value: T) -> T {
    value
}

struct ContentView: View {
    @ObservedObject var document: ParquetteDocument
    @AppStorage("theme") private var theme = AppTheme.system
    // @AppStorage("size") private var size = ControlSize.regular // need to wrap

    var body: some View {
        contentBody
            .preferredColorScheme(theme.colorScheme)
    }

    var contentBody: some View {
        HSplitView {
            List() {
                Section(header: Text(document.groupTitle)) {
                    ForEach(0..<colcount) { i in
                        Text("Colum \(i + 1)")
                    }
                }
            }
            .listStyle(SidebarListStyle())
            .frame(minWidth: 140) // same as Mail.app
            .layoutPriority(0.5)

            ParquetViewer(document: document)
                .layoutPriority(1)
        }
        .toolbar {
            ToolbarItem(id: "performShuffle", placement: ToolbarItemPlacement.automatic, showsByDefault: true) {
                Button(action: performShuffle) {
                    Image(systemName: "shuffle")
                }
            }

            ToolbarItem(id: "performInfo", placement: ToolbarItemPlacement.automatic, showsByDefault: true) {
                Button(action: performShuffle) {
                    Image(systemName: "info.circle.fill")
                        .renderingMode(.original)
                }

            }

            ToolbarItem(id: "performBookmark", placement: ToolbarItemPlacement.automatic, showsByDefault: true) {
                Button(action: performBookmark) {
                    Image(systemName: "bookmark.fill")
                        .renderingMode(.original)
                }
            }

            ToolbarItem(id: "performLink", placement: ToolbarItemPlacement.automatic, showsByDefault: true) {
                Button(action: performLink) {
                    Image(systemName: "link.badge.plus")
                        .renderingMode(.original)
                }
            }

            ToolbarItem(id: "performFlag", placement: ToolbarItemPlacement.automatic, showsByDefault: true) {
                Button(action: performFlag) {
                    Image(systemName: "flag.fill")
                        .renderingMode(.original)
                }
            }

            ToolbarItem(id: "performTimer", placement: ToolbarItemPlacement.automatic, showsByDefault: true) {
                Button(action: performTimer) {
                    Image(systemName: "timer")
                        .renderingMode(.original)
                }
            }
        }
    }

    func performBookmark() {
        print(wip(#function))
    }

    func performShuffle() {
        print(wip(#function))
    }

    func performInfo() {
        print(wip(#function))
    }

    func performLink() {
        print(wip(#function))
    }

    func performFlag() {
        print(wip(#function))
    }

    func performTimer() {
        print(wip(#function))
    }
}

extension UInt {
    var localizedString: String {
        NumberFormatter.localizedString(from: .init(value: self), number: .decimal)
    }
}

struct ParquetViewer: View {
    @ObservedObject var document: ParquetteDocument
    @State var sql = wip("select * from data where first_name >= 'K'")
    @Environment(\.font) var font
    @State var rowCount: UInt = 0

    var body: some View {
        VStack {
            DataTableView(rowCount: $rowCount)

            HStack {
                TextField(NSLocalizedString("SELECT * FROM table", comment: ""), text: $sql, onCommit: performQuery)
                    .font(font?.monospacedDigit())
                Button(action: performQuery) {
                    Label(NSLocalizedString("Execute", comment: ""), systemImage: "play.fill")
                        .labelStyle(IconOnlyLabelStyle())
                }
                // .keyboardShortcut(KeyEquivalent.return)
                Text(rowCount.localizedString)
            }
            .padding()
        }
    }

    func performQuery() {
        print("performQuery")
        do {
            if let frame = try document.ctx.query(sql: sql) {
                self.rowCount = .init(try frame.collectionCount())
                print("received \(self.rowCount) results")
            }
        } catch {
            NSDocumentController.shared.currentDocument?.windowForSheet?.presentError(error)
        }
    }
}


extension ControlSize {
    var controlSize : NSControl.ControlSize {
        switch self {
        case .regular:
            return .regular
        case .small:
            return .small
        case .mini:
            return .mini
        case .large:
            return .large
        @unknown default:
            return .regular
        }
    }
}

struct DataTableView : NSViewRepresentable {
    @Environment(\.controlSize) var controlSize
    @Binding var rowCount: UInt

    typealias NSViewType = NSScrollView
    
    func makeNSView(context: Context) -> NSViewType {
        let tableView = NSTableView()
        tableView.dataSource = context.coordinator
        tableView.delegate = context.coordinator

        tableView.focusRingType = .none
        tableView.allowsMultipleSelection = true
        tableView.allowsTypeSelect = true
        tableView.allowsEmptySelection = true
        tableView.allowsColumnSelection = false
        tableView.allowsColumnResizing = true
        tableView.allowsColumnReordering = false
        tableView.controlSize = controlSize.controlSize

        let scrollView = NSScrollView()
        scrollView.documentView = tableView
        scrollView.focusRingType = .none
        scrollView.hasVerticalScroller = true
        scrollView.hasHorizontalScroller = true
        scrollView.autohidesScrollers = false
        scrollView.scrollerStyle = .legacy

        return scrollView
    }

    func updateNSView(_ view: NSViewType, context: Context) {
        let tableView = view.documentView as! NSTableView

        if tableView.tableColumns.isEmpty {
            for i in 1...colcount {
                let id = "C\(i)"
                let col = NSTableColumn(identifier: NSUserInterfaceItemIdentifier(id))
                col.title = "Column \(i)"
                col.sortDescriptorPrototype = NSSortDescriptor(key: wip(id), ascending: true)

                // work; TODO: make monospace digit
                // (col.dataCell as? NSCell)?.font = NSFont(name: "Zapfino", size: 12)

                tableView.addTableColumn(col)
            }
        }

        if context.coordinator.rowCount != self.rowCount {
            context.coordinator.rowCount = self.rowCount
            context.coordinator.tableView?.reloadData()
        }
    }

    static func dismantleNSView(_ view: NSViewType, coordinator: Coordinator) {
        let tableView = view.documentView as! NSTableView

        tableView.dataSource = nil
        tableView.delegate = nil
    }

    func makeCoordinator() -> Coordinator {
        Coordinator(rowCount: rowCount)
    }

    final class Coordinator : NSObject, NSTableViewDelegate, NSTableViewDataSource {
        weak var tableView: NSTableView?
        var rowCount: UInt

        init(tableView: NSTableView? = nil, rowCount: UInt = 0) {
            self.tableView = tableView
            self.rowCount = rowCount
        }

        func tableView(_ tableView: NSTableView, objectValueFor tableColumn: NSTableColumn?, row: Int) -> Any? {
            wip("\(tableColumn?.identifier.rawValue ?? "") \(row)")
        }

        func numberOfRows(in tableView: NSTableView) -> Int {
            wip(Int(rowCount))
        }

        func tableView(_ tableView: NSTableView, sortDescriptorsDidChange oldDescriptors: [NSSortDescriptor]) {
            print(wip("change sort descriptors"), tableView.sortDescriptors)
        }
    }
}
