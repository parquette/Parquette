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
    case unsupportedContentType(UTType)
    case readNoFilename


    public var failureReason: String? {
        switch self {
        case .writeNotSupported:
            return loc("Write is not supported")
        case .unsupportedContentType:
            return loc("Unsupported content type")
        case .readNoFilename:
            return loc("No filename for reading")
        }
    }

    public var recoverySuggestion: String? {
        switch self {
        case .writeNotSupported:
            return loc("Parquette currently only supports reading files")
        case .unsupportedContentType:
            return loc("Parquette supports reading csv and parquet files")
        case .readNoFilename:
            return loc("Parquette can only read local files")
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

    /// For types that we natively support, return the file extension
    var parquetteExtension: String? {
        switch self {
        case .parquette_csv: return "csv"
        case .parquette_parquet: return "parquet"
        default: return nil
        }
    }
}

let asyncQueryDefault = "asyncQuery"
let asyncQueryDefaultValue = false

class AppState : ObservableObject {

    var useAsyncQuery: Bool {
        UserDefaults.standard.object(forKey: asyncQueryDefault) == nil ? asyncQueryDefaultValue : UserDefaults.standard.bool(forKey: asyncQueryDefault)
    }

    let operationQueue: OperationQueue = {
        let queue = OperationQueue()
        queue.qualityOfService = .userInitiated
        queue.maxConcurrentOperationCount = 1
        return queue
    }()

    @Published var result = QueryResult()

    /// The current stack of errors to present to the user
    //@Published var errors = Array<LocalError>()

    init() {
    }

    /// Performs the operation async or sync, depending on the default `asyncQuery` boolean
    func performOperation(async: Bool, _ block: @escaping () -> ()) {
        if async {
            operationQueue.addOperation {
                block()
            }
        } else {
            block()
        }
    }

    func attempt(async: Bool = false, _ block: @escaping () throws -> ()) {
        let win = NSApp.currentEvent?.window ?? NSDocumentController.shared.currentDocument?.windowForSheet

        performOperation(async: async) {
            do {
                try block()
            } catch {
                onmain {
                    dbg("presenting error", error.localizedDescription, "\(error)")
                    let err = LocalError(id: UUID(), error: error)
                    // self.errors.append(err)
                    win?.presentError(err)
                }
            }
        }
    }
}


struct LocalError : LocalizedError, Identifiable {
    let id: UUID
    let error: Error

    /// A localized message describing what error occurred.
    var errorDescription: String? {
        (error as NSError).localizedDescription
    }

    /// A localized message describing the reason for the failure.
    var failureReason: String? {
        (error as NSError).localizedFailureReason
    }

    /// A localized message describing how one might recover from the failure.
    var recoverySuggestion: String? {
        (error as NSError).localizedRecoverySuggestion
    }

    /// A localized message providing "help" text if the user requests help.
    var helpAnchor: String? {
        (error as NSError).helpAnchor
    }
}

struct QueryResult {
    /// The current unique ID of the results
    var resultID: UUID? = nil
    var resultCount: Int? = nil
    var resultTime: UInt? = nil
    var vectors: [ArrowVector] = []
}

final class ParquetteDocument: ReferenceFileDocument {
    static var readableContentTypes: [UTType] { [.parquette_parquet, .parquette_csv] }
    static var writableContentTypes: [UTType] { [] }

    let ctx = DFExecutionContext()
    let tableName = "data"

    init() {
    }

    required init(configuration: ReadConfiguration) throws {

        guard let ext = configuration.contentType.parquetteExtension else {
            throw ParquetteError.unsupportedContentType(configuration.contentType)
        }

        guard let filename = configuration.file.filename else {
            throw ParquetteError.readNoFilename
        }

        dbg("opening file", filename)

        //        let doc = wip(NSDocumentController.shared.currentDirectory)
        //        dbg("### currentDocument", NSDocumentController.shared.currentDocument?.fileURL)
        //        dbg("### currentDirectory", NSDocumentController.shared.currentDirectory)
        //        dbg("### docs", NSDocumentController.shared.documents)

        let tmpFile = UUID().uuidString

        // we need to operate on a physical file, and NSFileWrapper doens't expose the underlying URL, so we cheat by copying the file into a temporary file and opening that one
        let tmpURL = URL(fileURLWithPath: tmpFile, relativeTo: URL(fileURLWithPath: NSTemporaryDirectory()))
            .appendingPathExtension(ext)

        try configuration.file.write(to: tmpURL, options: .withNameUpdating, originalContentsURL: nil)

        dbg("intermediate temporary file", tmpURL.description)

        switch configuration.contentType {
        case .parquette_parquet:
            try ctx.register(parquet: tmpURL, tableName: tableName)
        case .parquette_csv:
            try ctx.register(csv: tmpURL, tableName: tableName)
        default:
            throw ParquetteError.unsupportedContentType(configuration.contentType)
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
                let timeStr = NumberFormatter.localizedString(from: .init(value: resultTime), number: .decimal)

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
        dbg(wip("TODO"))
    }

    func performBookmarkButton() -> some View {
        ActionButton(title: loc("Bookmark"), icon: "bookmark.fill", action: performBookmark)
    }
}

extension ParquetteCommands {
    func performCancel() {
        dbg(wip("TODO"))
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
        dbg(wip("TODO"))
    }

    func performReloadButton() -> some View {
        ActionButton(title: loc("Reload"), icon: "arrow.clockwise.circle", hoverFill: true, render: .template) {
            performReload()
        }
    }
}

extension ParquetteCommands {
    func performInfo() {
        dbg(wip("TODO"))
    }

    func performInfoButton() -> some View {
        ActionButton(title: loc("info"), icon: "info.circle.fill", action: performInfo)
    }
}

extension ParquetteCommands {
    func performLink() {
        dbg(wip("TODO"))
    }

    func performLinkButton() -> some View {
        ActionButton(title: loc("Link"), icon: "link.badge.plus", action: performLink)
    }
}

extension ParquetteCommands {
    func performFlag() {
        dbg(wip("TODO"))
    }

    func performFlagButton() -> some View {
        ActionButton(title: loc("Flag"), icon: "flag.fill", action: performFlag)
    }
}

extension ParquetteCommands {
    func performTimer() {
        dbg(wip("TODO"))
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
    @AppStorage("reopenDocuments") private var reopenDocuments = true
    @AppStorage(asyncQueryDefault) private var asyncQuery = asyncQueryDefaultValue
    @AppStorage("theme") private var theme = AppTheme.system
    @AppStorage("controlSize") private var controlScale = ControlScale.regular

    var body: some View {
        TabView {
            VStack {
                documentSettingsView()
                Divider().frame(width: 250)
                appearanceSettingsView()
            }
            .padding()
            .tabItem({
                Label(loc("General"), systemImage: "gearshape.2.fill")
            })

            /* crashes when switching tabs and then back to general
             documentSettingsView()
             .padding()
             .tabItem({
             Label(loc("General"), systemImage: "gearshape.2.fill")
             })

             appearanceSettingsView()
             .padding()
             .tabItem({
             Label(loc("Appearance"), systemImage: "eye.fill")
             })
             */
        }
        .tabViewStyle(DefaultTabViewStyle())
    }

    func documentSettingsView() -> some View {

        Form {
            // Text(loc("Document"))
            Toggle(loc("Re-Open Last Document"), isOn: $reopenDocuments)
            Toggle(loc("Query Asynchronously"), isOn: $asyncQuery)
            // Divider()
        }
    }

    func appearanceSettingsView() -> some View {
        Form {
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
//        .sheet(item: .constant(appState.errors.first), onDismiss: { appState.errors.removeFirst() }) { error in
//            ErrorSheet(error: error)
//        }
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
//            onmain {
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

    // registration_dttm, birthdate: thread '<unnamed>' panicked at 'called `Result::unwrap()` on an `Err` value: CDataInterface("The datatype \"Timestamp(Nanosecond, None)\" is still not supported in Rust implementation")', src/arrowz.rs:614:79

    @SceneStorage("sql") var sql = "select 1"
    @Environment(\.font) var font

    var body: some View {
        VStack {
            DataTableView()

            HStack {
                MenuButton(loc("SQL:")) {
                    ActionButton(title: loc("Execute"), icon: "play", action: performQuery)
                }
                .menuButtonStyle(PullDownMenuButtonStyle())
                .frame(idealWidth: 80)

                TextField(loc("SQL"), text: $sql, onCommit: performQuery)
                    .font(Font.custom("Menlo", size: 15, relativeTo: .body))
                    .layoutPriority(1)

                ActionButton(title: loc("Execute"), icon: "play.fill", action: performQuery)
                    .keyboardShortcut(.return, modifiers: [.command])
                    .labelStyle(IconOnlyLabelStyle()) // TODO: change to menu comment shortcut
            }
            .padding()
        }
    }

    func performQuery() {
        dbg("performQuery")
        let start = now()
        let ctx = document.ctx

        appState.result.resultCount = nil
        appState.result.resultTime = 0

        appState.attempt(async: appState.useAsyncQuery) {
            if let frame = try ctx.query(sql: sql) {
                let results = try frame.collectVector(index: wip(0)) // need a way to get multiple columns

                onmain {
                    let duration = start.millisFrom()
                    appState.result.resultCount = results.bufferLength
                    appState.result.vectors = [results]
                    appState.result.resultTime = duration
                    appState.result.resultID = .init()
                    dbg("received \(appState.result.vectors.count) columns with \(appState.result.resultCount ?? -1) elements in \(appState.result.resultTime ?? 0)ms")
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

private final class ArrowTableColumn : NSTableColumn {
    let vector: ArrowVector

    init(id: String, vector: ArrowVector) {
        self.vector = vector
        super.init(identifier: .init(id))
    }

    required init(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    func objectValue(at index: Int) -> NSObject? {
        do {
            switch vector.dataType {
            case .utf8:
                return try String.BufferView(vector: vector)[index] as NSString?
            case .int8:
                return try Int8.BufferView(vector: vector)[index] as NSNumber?
            case .int16:
                return try Int16.BufferView(vector: vector)[index] as NSNumber?
            case .int32:
                return try Int32.BufferView(vector: vector)[index] as NSNumber?
            case .int64:
                return try Int64.BufferView(vector: vector)[index] as NSNumber?
            case .uint8:
                return try UInt8.BufferView(vector: vector)[index] as NSNumber?
            case .uint16:
                return try UInt16.BufferView(vector: vector)[index] as NSNumber?
            case .uint32:
                return try UInt32.BufferView(vector: vector)[index] as NSNumber?
            case .uint64:
                return try UInt64.BufferView(vector: vector)[index] as NSNumber?

            default:
                throw SwiftArrowError.unsupportedDataType(vector.dataType)
            }
        } catch {
            dbg("error accessing index", index, error.localizedDescription)
            return loc("ERROR") as NSString
        }
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
        guard let tableView = view.documentView as? NSTableView else {
            fatalError("document view should have been a table")
        }

        func reloadColumns() {
            let results = self.appState.result
            let colCount = results.vectors.count

            let font = NSFont.monospacedDigitSystemFont(ofSize: controlSize.controlSize.systemFontSize, weight: .light)

            // clear and re-load; we could alternatively diff it and move columns around for similar queries…
            for col in tableView.tableColumns.reversed() {
                tableView.removeTableColumn(col)
            }

            for i in 0..<colCount {
                let id = "C\(i)"

                let vec = results.vectors[i]

                let col = ArrowTableColumn(id: id, vector: vec)
                col.title = wip("Column \(i)") // TODO: extract name from column

                // col.sortDescriptorPrototype = NSSortDescriptor(key: wip(id), ascending: true)

                col.isEditable = false
                col.isHidden = false
                col.headerCell.isEnabled = true

                if let dataCell = col.dataCell as? NSCell {
                    switch vec.dataType {
                    case .utf8, .utf8Large:
                        dataCell.formatter = nil // just strings
                        dataCell.alignment = .left
                    case .int8, .int16, .int32, .int64, .uint8, .uint16, .uint32, .uint64:
                        dataCell.formatter = NumberFormatter()
                        dataCell.alignment = .right
                    case .float16, .float32, .float64:
                        dataCell.formatter = NumberFormatter()
                        dataCell.alignment = .right
                    default:
                        dataCell.alignment = .center
                        ; // no formatter
                    }
                    dataCell.font = font
                }

                tableView.addTableColumn(col)
            }
        }

        if context.coordinator.result?.resultID != self.appState.result.resultID {
            reloadColumns()
            context.coordinator.result = self.appState.result
            dbg("reloading table with", context.coordinator.result?.resultCount, "rows")
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
        var result: QueryResult? = nil

        func tableView(_ tableView: NSTableView, objectValueFor tableColumn: NSTableColumn?, row: Int) -> Any? {
            (tableColumn as? ArrowTableColumn)?.objectValue(at: row)
        }

        func numberOfRows(in tableView: NSTableView) -> Int {
            result?.resultCount ?? 0
        }

        func tableView(_ tableView: NSTableView, sortDescriptorsDidChange oldDescriptors: [NSSortDescriptor]) {
            dbg("change sort descriptors", tableView.sortDescriptors)
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

/// Executes the given block on the main thread. If the current thread *is* the main thread, executed synchronously
@inlinable func onmain(_ block: @escaping () -> ()) {
    if Thread.isMainThread {
        block()
    } else {
        DispatchQueue.main.async(execute: block)
    }
}

extension CFAbsoluteTime {
    @inlinable func millisFrom() -> UInt {
        UInt(max(0, (CFAbsoluteTimeGetCurrent() - self) * 1_000))
    }
}
