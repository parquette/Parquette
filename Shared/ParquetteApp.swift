//
//  ParquetteApp.swift
//  Shared
//
//  Created by Marc Prud'hommeaux on 2/4/21.
//

import SwiftUI
import UniformTypeIdentifiers
import SwiftArrow

class AppDelegate: NSObject, NSApplicationDelegate {
    private let startTime = now()

    func applicationDidFinishLaunching(_ notification: Notification) {
        dbg("launched", notification.description, "in", startTime.millisFrom())
    }

    func applicationShouldOpenUntitledFile(_ sender: NSApplication) -> Bool {
        false // we do not support creating new files
    }
}

struct ContainerView : View, ParquetteCommands {
    @ObservedObject var document: ParquetteDocument
    @EnvironmentObject var appState: AppState
    @AppStorage("theme") private var theme = AppTheme.system
    @AppStorage("controlSize") private var controlScale = ControlScale.regular

    var body: some View {
        ContentView(document: document)
            .preferredColorScheme(theme.colorScheme)
            //.controlSize(controlScale.controlSize)
    }
}

@main
struct ParquetteApp: App {
    @NSApplicationDelegateAdaptor(AppDelegate.self) var appDelegate

    @SceneBuilder var body: some Scene {
        DocumentGroup(newDocument: { ParquetteDocument() }) { file in
            ContainerView(document: file.document)
                .environmentObject(AppState())
        }
        //.windowToolbarStyle(DefaultWindowToolbarStyle())
        //.windowToolbarStyle(ExpandedWindowToolbarStyle())
        .windowToolbarStyle(UnifiedWindowToolbarStyle())
        //.windowToolbarStyle(UnifiedCompactWindowToolbarStyle())
        .commands {
            SidebarCommands()

            // TextEditingCommands()
            // TextFormattingCommands()

            ToolbarCommands()

            //            CommandGroup(before: CommandGroupPlacement.newItem) {
            //                Button("before item") {
            //                    dbg("before item")
            //                }
            //            }
            //
            //            CommandGroup(replacing: CommandGroupPlacement.appInfo) {
            //                Button("Custom app info") {
            //                    // show custom app info
            //                }
            //            }

            //            performBookmarkButton()
            //                .keyboardShortcut("d", modifiers: [.command])
            //                .inCommandGroup(.pasteboard)

            //            CommandMenu("First menu") {
            //                Button("Print message") {
            //                    dbg("Hello World!")
            //                }.keyboardShortcut("p")
            //            }
        }

        Settings {
            SettingsView()
        }
    }
}

public enum ParquetteError : LocalizedError {
    case writeNotSupported
    case unknownContentType(UTType)

    public var failureReason: String? {
        switch self {
        case .writeNotSupported:
            return loc("Write is not supported")
        case .unknownContentType:
            return loc("Unknown content type")
        }
    }

    public var recoverySuggestion: String? {
        switch self {
        case .writeNotSupported:
            return loc("Parquette currently only supports reading files")
        case .unknownContentType:
            return loc("Save as a supported content type")
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

class AppState : ObservableObject {
    let operationQueue: OperationQueue = {
        let queue = OperationQueue()
        queue.qualityOfService = .userInitiated
        queue.maxConcurrentOperationCount = 1
        return queue
    }()

    @Published var result = QueryResult()

    init() {
    }
}

struct QueryResult : Equatable {
    var resultCount: UInt? = nil
    var resultTime: UInt? = nil
}

final class ParquetteDocument: ReferenceFileDocument {
    static var readableContentTypes: [UTType] { [.parquette_parquet, .parquette_csv] }
    static var writableContentTypes: [UTType] { [] }

    let ctx = DFExecutionContext()
    let tableName = "data"

    init() {
    }

    required init(configuration: ReadConfiguration) throws {

        guard let filename = configuration.file.filename else {
            throw CocoaError(.fileReadCorruptFile)
        }

        //        let url: URL! = nil

        dbg("opening file", filename)

        //        let doc = wip(NSDocumentController.shared.currentDirectory)
        //        dbg("### currentDocument", NSDocumentController.shared.currentDocument?.fileURL)
        //        dbg("### currentDirectory", NSDocumentController.shared.currentDirectory)
        //        dbg("### docs", NSDocumentController.shared.documents)

        let tmpFile = UUID().uuidString

        // we need to operate on a physical file, and NSFileWrapper doens't expose the underlying URL, so we cheat by copying the file into a temporary file and opening that one
        let tmpURL = URL(fileURLWithPath: tmpFile, relativeTo: URL(fileURLWithPath: NSTemporaryDirectory()))
            .appendingPathExtension("parquet")

        try configuration.file.write(to: tmpURL, options: .withNameUpdating, originalContentsURL: nil)

        dbg("### wrote to", tmpURL.absoluteString)

        switch configuration.contentType {
        case .parquette_parquet:
            try ctx.register(parquet: tmpURL, tableName: tableName)
        case .parquette_csv:
            try ctx.register(csv: tmpURL, tableName: tableName)
        default:
            throw ParquetteError.unknownContentType(configuration.contentType)
        }

        dbg("### opened", tmpURL.absoluteString)

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


struct StatusPanel : View, ParquetteCommands {
    @EnvironmentObject var appState: AppState

    var body: some View {
        HStack(alignment: .center) {
            Group {
                if self.appState.result.resultTime == 0 {
                    ProgressView()
                        .progressViewStyle(CircularProgressViewStyle())
                        .controlSize(.small)
                } else {
                    performReloadButton()
                        .keyboardShortcut("r")
                }
            }
            .frame(width: 50)

            Text(appState.statusText)
                .multilineTextAlignment(.leading)
                .lineLimit(1)
                .truncationMode(.tail)
                .frame(maxWidth: .infinity)

            Spacer()

            performCancelButton()
                .keyboardShortcut(".")
                .opacity(self.appState.result.resultTime == 0 ? 1.0 : 0.0)
        }
        .font(Font.callout.monospacedDigit())
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(Color.gray.cornerRadius(5).opacity(0.2))
        .frame(minWidth: 100, idealWidth: 450, maxWidth: 550) // note: seems to be fixed at the idealWidth
        .frame(height: 20)
        .layoutPriority(1)
    }
}

extension AppState {
    var statusText: String {
        if let resultTime = self.result.resultTime {
            if resultTime == 0 {
                return loc("Querying…")
            } else if let resultCount = self.result.resultCount {
                let countStr = NumberFormatter.localizedString(from: .init(value: resultCount), number: .decimal)
                let timeStr = NumberFormatter.localizedString(from: .init(value: resultTime), number: .scientific)

                return loc("\(countStr) results in \(timeStr) ms")
            } else {
                return loc("Querying…")
            }
        } else {
            return loc("Ready.")
        }
    }
}

/// A button that can appear in either a toolbar or a menu
struct ActionButton : View {
    let title: String
    let icon: String
    var hoverFill: Bool = false
    var render: Image.TemplateRenderingMode = .original

    let action: () -> ()

    @State var hovering: Bool = false

    var body: some View {
        Button(action: action) {
            Label(title: { Text(title) }) {
                Image(systemName: icon + (hovering && hoverFill ? ".fill" : ""))
                    .renderingMode(render)
                    .onHover(perform: { if hoverFill { self.hovering = $0 } })
            }
        }
    }
}

// MARK: Commands

protocol ParquetteCommands where Self : View {
    // var document: ParquetteDocument { get }
    var appState: AppState { get }
}

extension ParquetteCommands {
    func performBookmark() {
        dbg(wip(#function))
    }

    func performBookmarkButton() -> some View {
        ActionButton(title: loc("Bookmark"), icon: "bookmark.fill", action: performBookmark)
    }
}

extension ParquetteCommands {
    func performCancel() {
        dbg(wip(#function))
        appState.operationQueue.cancelAllOperations()
    }

    func performCancelButton() -> some View {
        ActionButton(title: loc("Cancel"), icon: "xmark.circle", hoverFill: true, render: .template) {
            performCancel()
        }
    }
}

extension ParquetteCommands {
    func performReload() {
        dbg(wip(#function))
    }

    func performReloadButton() -> some View {
        ActionButton(title: loc("Reload"), icon: "arrow.clockwise.circle", hoverFill: true, render: .template) {
            performReload()
        }
    }
}

extension ParquetteCommands {
    func performInfo() {
        dbg(wip(#function))
    }

    func performInfoButton() -> some View {
        ActionButton(title: loc("info"), icon: "info.circle.fill", action: performInfo)
    }
}

extension ParquetteCommands {
    func performLink() {
        dbg(wip(#function))
    }

    func performLinkButton() -> some View {
        ActionButton(title: loc("Link"), icon: "link.badge.plus", action: performLink)
    }
}

extension ParquetteCommands {
    func performFlag() {
        dbg(wip(#function))
    }

    func performFlagButton() -> some View {
        ActionButton(title: loc("Flag"), icon: "flag.fill", action: performFlag)
    }
}

extension ParquetteCommands {
    func performTimer() {
        dbg(wip(#function))
    }

    func performTimerButton() -> some View {
        ActionButton(title: loc("Timer"), icon: "timer", action: performTimer)
    }
}

extension View {
    func inCommandGroup(after: Bool? = true, _ placement: CommandGroupPlacement) -> some Commands {
        Group {
            CommandGroup(after: placement) {
                self // .labelStyle(TitleOnlyLabelStyle())
            }

            // “Closure containing control flow statement cannot be used with function builder 'CommandsBuilder'”
            //            switch after {
            //            case .none:
            //                CommandGroup(replacing: placement) {
            //                    self.labelStyle(TitleOnlyLabelStyle())
            //                }
            //            case false:
            //                CommandGroup(before: placement) {
            //                    self.labelStyle(TitleOnlyLabelStyle())
            //                }
            //            case true:
            //                CommandGroup(after: placement) {
            //                    self.labelStyle(TitleOnlyLabelStyle())
            //                }
            //            }
        }
    }
}


struct SettingsView : View {
    var body: some View {
        TabView {
            DocumentSettingsView()
                .padding()
                .tabItem({
                    Label(loc("General"), systemImage: "gearshape.2.fill")
                })
        }
        .tabViewStyle(DefaultTabViewStyle())
    }
}


enum AppTheme : String, CaseIterable, Hashable {
    case system
    case light
    case dark

    var localizedTitle: String {
        switch self {
        case .system:
            return loc("System Default")
        case .light:
            return loc("Light")
        case .dark:
            return loc("Dark")
        }
    }

    var colorScheme: ColorScheme? {
        switch self {
        case .system:
            return nil
        case .light:
            return .light
        case .dark:
            return .dark
        }
    }
}

enum ControlScale : String, CaseIterable, Hashable {
    case mini
    case small
    case regular
    case large

    var localizedTitle: String {
        switch self {
        case .mini: return loc("Mini")
        case .small: return loc("Small")
        case .regular: return loc("Regular")
        case .large: return loc("Large")
        }
    }

    var controlSize: ControlSize {
        switch self {
        case .mini: return .mini
        case .small: return .small
        case .regular: return .regular
        case .large: return .large
        }
    }

}

struct DocumentSettingsView : View {
    @AppStorage("reopenDocuments") private var reopenDocuments = true
    @AppStorage("theme") private var theme = AppTheme.system
    @AppStorage("controlSize") private var controlScale = ControlScale.regular

    var body: some View {
        Form {
            // Text(loc("Document"))
            Toggle(loc("Re-Open Last Document:"), isOn: $reopenDocuments)
            // Divider()

            Picker(loc("Theme:"), selection: $theme) {
                ForEach(AppTheme.allCases, id: \.self) { theme in
                    Text(theme.localizedTitle)
                }
            }
            .pickerStyle(RadioGroupPickerStyle())

            //Divider()

            Picker(loc("Control Size:"), selection: $controlScale) {
                ForEach(ControlScale.allCases, id: \.self) { scale in
                    Text(scale.localizedTitle)
                }
            }
            .pickerStyle(RadioGroupPickerStyle())

        }
    }
}

@available(*, deprecated, message: "make columns dynamic")
let colcount = 7


struct ContentView: View, ParquetteCommands {
    @ObservedObject var document: ParquetteDocument
    @EnvironmentObject var appState: AppState

    var body: some View {
        contentBody
    }

    var contentBody: some View {
        NavigationView {
            List() {
                Section(header: Text(wip("") /* appState.fileTitle */)) {
                    ForEach(0..<colcount) { i in
                        Text("Colum \(i + 1)")
                    }
                }
            }
            .listStyle(SidebarListStyle())
            .frame(minWidth: 180)
            .toolbar {
                ToolbarItem {
                    ActionButton(title: loc("Toggle Sidebar"), icon: "sidebar.leading", render: .template, action: {
                        NSApplication.shared.sendAction(#selector(NSSplitViewController.toggleSidebar(_:)), to: NSApp.currentEvent?.window, from: nil)
                    })
                }
            }

            ParquetViewer(document: document)
                .toolbar {
                    Group {
                        ToolbarItem(id: "statusPanel", placement: ToolbarItemPlacement.principal) {
                            StatusPanel()
                        }
                    }

                    Group {
                        ToolbarItem(id: "performInfo", placement: ToolbarItemPlacement.primaryAction) {
                            performInfoButton()
                        }

                        ToolbarItem(id: "performBookmark", placement: ToolbarItemPlacement.primaryAction) {
                            performBookmarkButton()
                        }
                    }

                    ToolbarItem() { Spacer() }

                    Group {
                        ToolbarItem(id: "performLink") {
                            performLinkButton()
                        }
                    }

                    Group {
                        ToolbarItem(id: "performFlag") {
                            performFlagButton()
                        }

                        ToolbarItem(id: "performTimer") {
                            performTimerButton()
                        }
                    }
                }
        }
        .navigationViewStyle(DoubleColumnNavigationViewStyle())
    }

}

extension UInt {
    var localizedString: String {
        NumberFormatter.localizedString(from: .init(value: self), number: .decimal)
    }
}

//extension ParquetteCommands {
//    @discardableResult
//    func trying<T>(_ block: () -> T) throws -> T? {
//        do {
//            return block()
//        } catch {
//            DispatchQueue.main.async {
//                win?.presentError(error)
//            }
//            return nil
//        }
//
//    }
//}

struct ParquetViewer: View {
    @ObservedObject var document: ParquetteDocument
    @EnvironmentObject var appState: AppState

    @State var sql = wip("select * from data where first_name >= 'Q'")
    @Environment(\.font) var font
    @State var rowCount: UInt = 0

    var body: some View {
        VStack {
            DataTableView()

            HStack {
                TextField("SELECT * FROM table", text: $sql, onCommit: performQuery)
                    .font(Font.custom("Menlo", size: 15, relativeTo: .body))

                Button(action: performQuery) {
                    Label(loc("Execute"), systemImage: "play.fill")
                        .labelStyle(IconOnlyLabelStyle())
                }
                .keyboardShortcut(.return, modifiers: [.command])
            }
            .padding()
        }
    }

    func performQuery() {
        dbg("performQuery")
        let start = now()
        let ctx = document.ctx
        let win = NSDocumentController.shared.currentDocument?.windowForSheet

        appState.result.resultCount = nil
        appState.result.resultTime = 0

        appState.operationQueue.addOperation {
            do {
                if let frame = try ctx.query(sql: sql) {
                    let results = try frame.collectionCount()

                    DispatchQueue.main.async {
                        self.rowCount = .init(results)
                        let duration = start.millisFrom()
                        dbg("received \(self.rowCount) results in \(duration)ms")
                        appState.result.resultCount = self.rowCount
                        appState.result.resultTime = duration
                    }
                }
            } catch {
                DispatchQueue.main.async {
                    win?.presentError(error)
                }
            }
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

extension NSControl.ControlSize {
    var systemFontSize: CGFloat {
        NSFont.systemFontSize(for: self)
    }
}

struct DataTableView : NSViewRepresentable {
    @Environment(\.controlSize) var controlSize
    @EnvironmentObject var appState: AppState

    typealias NSViewType = NSScrollView

    func makeNSView(context: Context) -> NSViewType {
        let tableView = context.coordinator.tableView
        
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
        scrollView.translatesAutoresizingMaskIntoConstraints = false
        scrollView.documentView = tableView
        scrollView.drawsBackground = false
        scrollView.borderType = .noBorder
        scrollView.hasVerticalScroller = true
        scrollView.hasHorizontalScroller = true
        scrollView.autohidesScrollers = true
        scrollView.allowsMagnification = true
        scrollView.horizontalScrollElasticity = .none
        scrollView.verticalScrollElasticity = .allowed
        scrollView.usesPredominantAxisScrolling = true

        // scrollView.scrollerStyle = .legacy // always shows scrollers

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

                col.isEditable = false
                col.isHidden = false
                col.headerCell.isEnabled = true

                (col.dataCell as? NSCell)?.font = NSFont.monospacedDigitSystemFont(ofSize: controlSize.controlSize.systemFontSize, weight: .light)

                tableView.addTableColumn(col)
            }
        }

        if context.coordinator.rowCount != self.appState.result.resultCount {
            context.coordinator.rowCount = self.appState.result.resultCount
            dbg("reloading", context.coordinator.rowCount)
            context.coordinator.tableView.reloadData()
        }
    }

    static func dismantleNSView(_ view: NSViewType, coordinator: Coordinator) {
        coordinator.tableView.dataSource = nil
        coordinator.tableView.delegate = nil
    }

    func makeCoordinator() -> Coordinator {
        Coordinator()
    }

    final class Coordinator : NSObject, NSTableViewDelegate, NSTableViewDataSource {
        let tableView: NSTableView = NSTableView()
        var rowCount: UInt? = nil

        func tableView(_ tableView: NSTableView, objectValueFor tableColumn: NSTableColumn?, row: Int) -> Any? {
            wip("\(tableColumn?.identifier.rawValue ?? "") \(row)")
        }

        func numberOfRows(in tableView: NSTableView) -> Int {
            wip(Int(rowCount ?? 0))
        }

        func tableView(_ tableView: NSTableView, sortDescriptorsDidChange oldDescriptors: [NSSortDescriptor]) {
            dbg(wip("change sort descriptors"), tableView.sortDescriptors)
        }
    }
}


// MARK: Utilities

@available(*, deprecated, message: "work in progress")
@inlinable func wip<T>(_ value: T) -> T {
    value
}

/// Localization
@inlinable func loc<S: ExpressibleByStringInterpolation>(_ key: S, comment: String = "") -> String {
    NSLocalizedString("\(key)", comment: comment)
}

@inlinable func now() -> CFAbsoluteTime {
    CFAbsoluteTimeGetCurrent()
}

extension CFAbsoluteTime {
    @inlinable func millisFrom() -> UInt {
        UInt(max(0, (CFAbsoluteTimeGetCurrent() - self) * 1_000))
    }

}

