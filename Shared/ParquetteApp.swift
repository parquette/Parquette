//
//  ParquetteApp.swift
//  Shared
//
//  Created by Marc Prud'hommeaux on 2/4/21.
//

import SwiftUI
import MiscKit
import HubOMatic
import SwiftArrow
import JavaScriptCore
import UniformTypeIdentifiers

@main
struct ParquetteApp: App {
    @NSApplicationDelegateAdaptor(AppDelegate.self) var appDelegate

    @SceneBuilder var body: some Scene {
        Group {
            DocumentGroup(viewing: ParquetteDocument.self, viewer: createAppContent)
            DocumentGroup(newDocument: ParquetteDocument(), editor: createAppContent)
        }
        .commands {
            SidebarCommands()
            ToolbarCommands()
        }


        Settings {
            SettingsView()
        }
    }

    func createAppContent(fileConfig: FileDocumentConfiguration<ParquetteDocument>) -> some View {
        ParquetteAppContentView(docState: DocState(config: fileConfig))
            .frame(idealWidth: 700, idealHeight: 700)
            .environmentObject(appDelegate.appState)
            .withFileExporter()
    }
}


class AppDelegate: NSObject, NSApplicationDelegate {
    let appState = AppState()
    private let startTime = now()

    func applicationShouldTerminateAfterLastWindowClosed(_ sender: NSApplication) -> Bool {
        true
    }

    func applicationShouldOpenUntitledFile(_ sender: NSApplication) -> Bool {
        false // we do not support creating new files
    }

    func applicationWillFinishLaunching(_ notification: Notification) {
        dbg(notification.debugDescription)
    }

    func applicationDidFinishLaunching(_ notification: Notification) {
        dbg("launched", notification.description, "in", startTime.millisFrom())
        appState.calculateMemoryUsage()
    }

    //    func applicationWillHide(_ notification: Notification) {
    //        dbg(notification.debugDescription)
    //    }

    //    func applicationDidHide(_ notification: Notification) {
    //        dbg(notification.debugDescription)
    //    }

    //    func applicationWillUnhide(_ notification: Notification) {
    //        dbg(notification.debugDescription)
    //    }

    func applicationDidUnhide(_ notification: Notification) {
        //        dbg(notification.debugDescription)
        appState.calculateMemoryUsage()
    }

    func applicationWillBecomeActive(_ notification: Notification) {
        //        dbg(notification.debugDescription)
    }

    func applicationDidBecomeActive(_ notification: Notification) {
        //        dbg(notification.debugDescription)
        appState.calculateMemoryUsage()
    }

    //    func applicationWillResignActive(_ notification: Notification) {
    //        dbg(notification.debugDescription)
    //    }

    func applicationDidResignActive(_ notification: Notification) {
        //        dbg(notification.debugDescription)
        appState.calculateMemoryUsage()
    }

    //    func applicationWillUpdate(_ notification: Notification) {
    //        dbg(notification.debugDescription)
    //    }

    //    func applicationDidUpdate(_ notification: Notification) {
    //        dbg(notification.debugDescription)
    //    }

    func applicationWillTerminate(_ notification: Notification) {
        dbg(notification.debugDescription)
    }

    func applicationDidChangeScreenParameters(_ notification: Notification) {
        dbg(notification.debugDescription)
    }

    //    func applicationDidChangeOcclusionState(_ notification: Notification) {
    //        dbg(notification.debugDescription)
    //    }

}

struct ParquetteAppContentView : View, ParquetteCommands {
    @StateObject var docState: DocState
    @AppStorage("theme") private var theme = AppTheme.system
    @AppStorage("controlSize") private var controlScale = ControlScale.regular
    @Environment(\.scenePhase) private var scenePhase
    @Environment(\.undoManager) private var undoManager

    var body: some View {
        ContentView()
            .preferredColorScheme(theme.colorScheme)
            .environmentObject(docState)
            //.controlSize(controlScale.controlSize)
            .onChange(of: scenePhase) { phase in
                dbg("scene phase", undoManager)
            }
            .onAppear {
                dbg("disabling undo", undoManager)
            }
    }
}

extension View {
    func withFileExporter() -> some View {
        wip(self)
        // TODO
        // .fileExporter(isPresented: T##Binding<Bool>, documents: T##Collection, contentType: T##UTType, onCompletion: T##(Result<[URL], Error>) -> Void)
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

/// The global application state
final class AppState : ObservableObject {
    /// The amount of memory usage by the app, as most recently polled
    @Published var memoryUsage: mach_vm_size_t? = nil

    init() {
    }

    var processMemoryText: String? {
        guard let footprint = memoryUsage else {
            return nil
        }

        return ByteCountFormatter.string(fromByteCount: .init(footprint), countStyle: .memory)

    }

    func calculateMemoryUsage() {
        self.memoryUsage = memoryFootprint()
    }
}

/// The per-document window state
final class DocState : ObservableObject {
    let ctx = DFExecutionContext()
    let jsc = JSContext()

    @Published var config: FileDocumentConfiguration<ParquetteDocument>
    @Published var result = QueryResult()

    init(config: FileDocumentConfiguration<ParquetteDocument>) {
        self.config = config
        do {
            try self.load(fileType: config.document.contentType, fileURL: config.fileURL)
        } catch {
            // how best to handle errors here?
            dbg("error loading from file: \(config) error: \(wip(error))")
        }
    }

    var useAsyncQuery: Bool {
        UserDefaults.standard.object(forKey: asyncQueryDefault) == nil ? asyncQueryDefaultValue : UserDefaults.standard.bool(forKey: asyncQueryDefault)
    }

    let operationQueue: OperationQueue = {
        let queue = OperationQueue()
        queue.qualityOfService = .userInitiated
        queue.maxConcurrentOperationCount = 1
        return queue
    }()

    /// The current stack of errors to present to the user
    //@Published var errors = Array<LocalError>()

    @discardableResult func load(fileType: UTType, fileURL: URL?, tableName: String = "data") throws -> Bool {
        dbg("creating with", config.fileURL?.absoluteString)
        guard let fileURL = config.fileURL else {
            return false
        }

        // not needed when sandboxed
        //        let accessing = fileURL.startAccessingSecurityScopedResource()
        //        defer { if accessing { fileURL.stopAccessingSecurityScopedResource() } }

        switch fileType {
        case .parquette_parquet:
            try ctx.register(parquet: fileURL, tableName: tableName)
        case .parquette_csv:
            try ctx.register(csv: fileURL, tableName: tableName)
        default:
            throw ParquetteError.unsupportedContentType(config.document.contentType)
        }

        return true
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


extension JSContext {
    @inlinable func checkException() throws {
        if let exception = self.exception {
            if let error = JSException(exception: exception.jsValueRef, ctx: self.jsGlobalContextRef) {
                throw error
            } else {
                dbg("unconvertable JS exception")
            }
        }
    }

    @inlinable func setProperty(_ name: String, _ val: JSValueRef) throws {
        let pname = JSStringCreateWithUTF8CString(name)
        defer { JSStringRelease(pname) }
        var ex: JSValueRef? = nil
        JSObjectSetProperty(jsGlobalContextRef, jsGlobalContextRef, pname, val, JSPropertyAttributes(kJSPropertyAttributeNone), &ex)
        if let ex = ex {
            if let error = JSException(exception: ex, ctx: jsGlobalContextRef) { throw error }
        }
    }

    @inlinable func getProperty(_ name: String) throws -> JSValueRef? {
        let pname = JSStringCreateWithUTF8CString(name)
        defer { JSStringRelease(pname) }
        var ex: JSValueRef? = nil
        let val = JSObjectGetProperty(jsGlobalContextRef, jsGlobalContextRef, pname, &ex)
        if let ex = ex {
            if let error = JSException(exception: ex, ctx: jsGlobalContextRef) { throw error }
        }
        return val
    }

//    /// Deletes a property from an object.
//    @inlinable func deleteProperty(inObject object: JSObjectRef!, propertyName: JSStringRef!) throws {
//        let _ = try withJSException {
//            JSObjectDeleteProperty(jsGlobalContextRef, object, propertyName, &$0)
//        }
//    }

    /// Returns `true` if the given object (defaults to the global object) has the specified property defined
    @inlinable func hasProperty(_ name: CFString, this: JSObjectRef? = nil) -> Bool {
        let propName: JSStringRef = JSStringCreateWithCFString(name)
        defer { JSStringRelease(propName) }
        return JSObjectHasProperty(jsGlobalContextRef, this ?? jsGlobalContextRef, propName)
    }

    /// Executes the JS against the context
    @discardableResult public func execute(script: String) throws -> JSValue? {
        let value = evaluateScript(" { " + script + "; };")
        try checkException()
        return value
    }

//    /// Passes the given data as the named property for the duration of the block closure.
//    /// - Note: Care must be taken to avoid retaining the data in the JavaScriptCore context, since the underlying pointer will not be valid beyond the scope of the block
//    @inlinable func withArrayBufferData<T>(data: Data, named name: String = JSContext.makeTemporaryPropName(), block: (String) throws -> T) rethrows -> T {
//        let count = data.count
//        var data = data
//
//        return try data.withUnsafeMutableBytes { ptr in
//            let deallocator: JSTypedArrayBytesDeallocator = { ptr, ctx in
//                // unnecessary, since the pointer will pass out
//                // ptr?.deallocate()
//            }
//
//            var ex : JSValueRef?
//            let obj = JSObjectMakeArrayBufferWithBytesNoCopy(jsGlobalContextRef, ptr.baseAddress, count, deallocator, nil, &ex)
//            if let ex = ex { if let error = JSException(exception: ex, ctx: jsGlobalContextRef) { throw error } }
//
////            guard let objValue = obj else {
////                throw err("unexpected null result in withArrayBufferData")
////            }
//
//            return try withPropertyValue(objValue, named: name, block: block)
//        }
//    }


    /// Retruns a random temporary variable name
    @inlinable static func makeTemporaryPropName() -> String {
        "_v" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
    }

//    /// Performs the given closure with the temporarily assigned propertyname
//    /// - Note: Care must be taken to avoid retaining the data in the JavaScriptCore context, since the underlying pointer will not be valid beyond the scope of the block
//    @inlinable func withPropertyValue<T>(_ obj: JSObjectRef, named name: String = JSContext.makeTemporaryPropName(), block: (String) throws -> T) throws -> T {
//        let pname = JSStringCreateWithUTF8CString(name)
//        defer { JSStringRelease(pname) }
//
//        try setProperty(inObject: jsGlobalContextRef, propertyName: pname, value: obj, attributes: .init(kJSPropertyAttributeReadOnly))
//
//        defer {
//            try? deleteProperty(inObject: jsGlobalContextRef, propertyName: pname) // cannot throw in defer
//        }
//
//        return try block(name)
//    }

    /// Validates the JS against the context
    @inlinable func validate(script: String) throws -> Bool {
        let (valid, exception) = script.withCString { script in
            let stringRef = JSStringCreateWithUTF8CString(script)
            defer { JSStringRelease(stringRef) }
            var exception: JSValueRef? = nil
            return (JSCheckScriptSyntax(jsGlobalContextRef, stringRef, nil, 0, &exception), exception)
        } as (Bool, JSValueRef?)

        try checkException()
        return valid
    }
}


/// Convert the JSStringRef to a String; note that the JSStringRef is not freed
@inlinable public func jsStringToString(_ jstr: JSStringRef?) -> String {
    // JSStringRef A UTF16 character buffer. The fundamental string representation in JavaScript
    let len = JSStringGetMaximumUTF8CStringSize(jstr)
    if len <= 0 { return "" }

    let buf = UnsafeMutablePointer<Int8>.allocate(capacity: len)
    let _: Int = JSStringGetUTF8CString(jstr, buf, len) // TODO: do we need to utilize the actual length?

    if let sstr = String(validatingUTF8: buf) {
        buf.deallocate()
        return sstr
    } else {
        return ""
    }
}

/// The result of a SQL query
struct QueryResult {
    /// The current unique ID of the results; new queries will always be assigned a new ID
    var resultID: UUID? = nil

    /// The amount of time the query took; -1 means a query is currently being performed
    var resultTime: Int? = nil

    /// The actual results
    var results: ArrowResultSet? = nil
}

extension QueryResult {
    /// The total number of results in the data set
    var resultCount: Int? {
        results?.columnSets.first?.count
    }
}

final class ParquetteDocument: FileDocument {
    let contentType: UTType
    static var readableContentTypes: [UTType] { [.parquette_parquet, .parquette_csv] }

    init() {
        self.contentType = .parquette_parquet
    }

    required init(configuration: ReadConfiguration) throws {
        guard let ext = configuration.contentType.parquetteExtension else {
            throw ParquetteError.unsupportedContentType(configuration.contentType)
        }
        self.contentType = configuration.contentType
    }

    // declaring this seems to enable autosave…
    // static var writableContentTypes: [UTType] { wip([]) } // TODO: export to CSV?

    func fileWrapper(configuration: WriteConfiguration) throws -> FileWrapper {
        // dbg(configuration.contentType.debugDescription)

        // return FileWrapper() // avoid showing a save error to the user; this however does wind up clobbering the file

        switch configuration.contentType {
        case .parquette_csv:
            throw ParquetteError.writeNotSupported
        case .parquette_parquet:
            throw ParquetteError.writeNotSupported
        default:
            throw ParquetteError.writeNotSupported
        }
    }
}


struct StatusPanel : View, ParquetteCommands {
    @EnvironmentObject var appState: AppState
    @EnvironmentObject var docState: DocState

    var body: some View {
        HStack(alignment: .center) {
            Group {
                if self.docState.result.resultTime == -1 {
                    ProgressView()
                        .progressViewStyle(CircularProgressViewStyle())
                        .controlSize(.small)
                } else {
                    performReloadButton()
                        .keyboardShortcut("r")
                }
            }

            Text(docState.queryText)
                .multilineTextAlignment(.leading)
                .lineLimit(1)
                .truncationMode(.tail)
                .frame(maxWidth: .infinity)

            Spacer()

            performCancelButton()
                .keyboardShortcut(".")
                .opacity(self.docState.result.resultTime == -1 ? 1.0 : 0.0)

            // the file/memory size labels
            VStack(alignment: .leading, spacing: 2) {
                HStack {
                    Image(systemName: "internaldrive.fill")
                    Text(docState.fileMemoryText ?? "?")
                }
                .help(loc("The size of the current input file"))
                HStack {
                    Image(systemName: "memorychip")
                    Text(appState.processMemoryText ?? "?")
                }
                .help(loc("The total amount of memory in use by the app"))
            }
            .font(Font.footnote.monospacedDigit())
            .foregroundColor(Color.secondary)
            .padding(.horizontal)
        }
        .font(Font.callout.monospacedDigit())
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(Color.gray.cornerRadius(5).opacity(0.2))
        .frame(minWidth: 100, idealWidth: 450, maxWidth: 550) // note: seems to be fixed at the idealWidth
        .frame(height: 20)
        //.layoutPriority(1)
    }
}

extension DocState {
    var queryText: String {
        if self.result.resultTime == -1 {
            return loc("Querying…")
        }

        if let resultTime = self.result.resultTime {
            if let resultCount = self.result.resultCount {
                let countStr = NumberFormatter.localizedString(from: .init(value: resultCount), number: .decimal)

                let timeStr = NumberFormatter.localizedString(from: .init(value: resultTime), number: .decimal)
                // let timeStr2 = DateComponentsFormatter.interval.string(from: .init(resultTime) / 1000.0) // interval is in ms
                // let timeStr = timeStr2 ?? ""

                return loc("\(countStr) \(resultCount == 1 ? "result" : "results") in \(timeStr) ms")
            } else {
                return loc("Querying…")
            }
        } else {
            return loc("Ready.")
        }
    }

    /// The current status is the size of the current file
    var fileMemoryText: String? {
        guard let size = try? config.fileURL?.resourceValues(forKeys: [.fileSizeKey]).fileSize else {
            return nil
        }
        return ByteCountFormatter.string(fromByteCount: .init(size), countStyle: .file)
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
    var docState: DocState { get }
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
        docState.operationQueue.cancelAllOperations()
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
    //@AppStorage("reopenDocuments") private var reopenDocuments = true
    @AppStorage(asyncQueryDefault) private var asyncQuery = asyncQueryDefaultValue
    @AppStorage("theme") private var theme = AppTheme.system
    @AppStorage("controlSize") private var controlScale = ControlScale.regular
    @AppStorage("NSQuitAlwaysKeepsWindows") private var quitAlwaysKeepsWindows = true



    private enum Tabs: Hashable {
        case general, appearance
    }

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

            // crashes when switching tabs a couple of times
            //            documentSettingsView()
            //                .padding()
            //                .tabItem({
            //                    Label(loc("General"), systemImage: "gearshape.2.fill")
            //                })
            //                .tag(Tabs.general)
            //
            //            appearanceSettingsView()
            //                .padding()
            //                .tabItem({
            //                    Label(loc("Appearance"), systemImage: "eye.fill")
            //                })
            //                .tag(Tabs.appearance)
        }
        .tabViewStyle(DefaultTabViewStyle())
    }

    func documentSettingsView() -> some View {

        Form {
            // Text(loc("Document"))
            Toggle(loc("Re-Open Last Document"), isOn: $quitAlwaysKeepsWindows)
            // Toggle(loc("Re-Open Last Document"), isOn: $reopenDocuments)
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

extension ArrowColumnSet {
    var icon: Image {
        switch self.batches.first?.dataType {
        case .none: return Image(systemName: "exclamationmark.square.fill")
        case .null: return Image(systemName: "exclamationmark.sheild.fill")
        case .boolean: return Image(systemName: "switch.2")
        case .int8: return Image(systemName: "8.circle")
        case .uint8: return Image(systemName: "8.circle.fill")
        case .int16: return Image(systemName: "16.circle")
        case .uint16: return Image(systemName: "16.circle.fill")
        case .int32: return Image(systemName: "32.circle")
        case .uint32: return Image(systemName: "32.circle.fill")
        case .int64: return Image(systemName: "46.circle")
        case .uint64: return Image(systemName: "46.circle.fill")
        case .float16: return Image(systemName: "16.square")
        case .float32: return Image(systemName: "32.square")
        case .float64: return Image(systemName: "46.square")
        case .binary: return Image(systemName: "seal")
        case .binaryLarge: return Image(systemName: "seal.fill")
        case .utf8: return Image(systemName: "textformat")
        case .utf8Large: return Image(systemName: "textformat.size")
        case .date32: return Image(systemName: "calendar.circle")
        case .date64: return Image(systemName: "calendar.circle.fill")
        case .time64: return Image(systemName: "timer.square")
        }
    }
}

struct ContentView: View, ParquetteCommands {
    @EnvironmentObject var docState: DocState

    var body: some View {
        contentBody
    }

    var contentBody: some View {
        NavigationView {
            List() {
                Section(header: Text("Vectors")) {
                    ForEach(self.docState.result.results?.columnSets ?? []) { columnSet in
                        Label(
                            title: { Text(columnSet.columnName ?? "???") },
                            icon: { columnSet.icon })
                    }
                }
            }
            .listStyle(SidebarListStyle())
            // .frame(minWidth: 180)
            // .frame(minWidth: docState.config.fileURL == nil ? nil : 180, maxWidth: docState.config.fileURL == nil ? 0 : nil) // hide when no file
            .toolbar {
                ToolbarItem {
                    ActionButton(title: loc("Toggle Sidebar"), icon: "sidebar.leading", render: .template, action: {
                        NSApplication.shared.sendAction(#selector(NSSplitViewController.toggleSidebar(_:)), to: NSApp.currentEvent?.window, from: nil)
                    })
                }
            }

            ParquetViewer()
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
        //        .sheet(item: .constant(docState.errors.first), onDismiss: { docState.errors.removeFirst() }) { error in
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


struct StringListStorage : Codable {
    var strings: [String] = []
}

extension StringListStorage : RawRepresentable {
    init(rawValue: String) {
        if let data = rawValue.data(using: .utf8) {
            do {
                strings = try JSONDecoder().decode([String].self, from: data)
            } catch {
                dbg("StringListStorage deserialize error", strings)
                self = .init(strings: [])
            }
        } else {
            self = .init(strings: [])
        }
    }

    var rawValue: String {
        (squelch(try String(data: JSONEncoder().encode(self.strings), encoding: .utf8)) ?? "[]") ?? "[]" // double optional: <Optional<OptionalString>>
    }
}


struct ParquetViewer: View {
    @EnvironmentObject var docState: DocState
    @EnvironmentObject var appState: AppState

    // registration_dttm, birthdate: thread '<unnamed>' panicked at 'called `Result::unwrap()` on an `Err` value: CDataInterface("The datatype \"Timestamp(Nanosecond, None)\" is still not supported in Rust implementation")', src/arrowz.rs:614:79

    @State var selectedTab = ConsoleTab.sql

    @SceneStorage("sql") var sql = "select count(*) from data" // "select 1 + CAST('2' as BIGINT)"
    @SceneStorage("script") var script = "1+2"
    @SceneStorage("sqlVisible") var sqlVisible = true

    @SceneStorage("sqlHistory") var sqlHistory = StringListStorage()
    @AppStorage("sqlHistoryCount") var sqlHistoryCount = 20

    @SceneStorage("scriptHistory") var scriptHistory = StringListStorage()
    @AppStorage("scriptHistoryCount") var scriptHistoryCount = 20

    @Environment(\.font) var font

    var body: some View {
        VSplitView {
            DataTableView()
            ConsoleTabView()
        }
    }
}

extension ParquetViewer {
    func ConsoleActionsBar() -> some View {
        HStack(alignment: .firstTextBaseline) {
            ActionButton(title: loc("Toggle Consoles"), icon: "rectangle.bottomthird.inset.fill", render: .template) {
                sqlVisible.toggle()
            }
            .foregroundColor(sqlVisible ? .accentColor : .secondary)
            .buttonStyle(PlainButtonStyle())
            .labelStyle(IconOnlyLabelStyle())

            MenuButton(selectedTab == .sql ? loc("SQL:") : loc("JavaScript:")) {
                ActionButton(title: loc("Execute"), icon: "play", action: performQuery)

                Group {
                    Divider()
                    Text(loc("Recent Queries:"))
                    ForEach(sqlHistory.strings, id: \.self) { previousQuery in
                        ActionButton(title: previousQuery, icon: "magnifyingglass.circle") {
                            sql = previousQuery
                        }
                    }
                }

                Group {
                    Divider()
                    Text(loc("Recent Scripts:"))
                    ForEach(scriptHistory.strings, id: \.self) { previousScript in
                        ActionButton(title: previousScript, icon: "magnifyingglass.circle") {
                            script = previousScript
                        }
                    }
                }

                Group {
                    Divider()

                    ActionButton(title: loc("Clear Script History"), icon: "trash.circle.fill") {
                        scriptHistory.strings.removeAll()
                    }

                    ActionButton(title: loc("Clear SQL History"), icon: "trash.circle") {
                        sqlHistory.strings.removeAll()
                    }
                }
            }
            .menuButtonStyle(PullDownMenuButtonStyle())
            .frame(width: 200)

            Spacer()
            Text(docState.ctx.validationMessage(sql: sql) ?? "")
            Spacer()

            ActionButton(title: loc("Execute"), icon: "play.fill", action: performConsoleCommand)
                .help(loc("Executed the console command (CMD-Return)"))
                .keyboardShortcut(.return, modifiers: [.command])
                .labelStyle(IconOnlyLabelStyle()) // TODO: change to menu comment shortcut
        }
    }


    func ConsoleTabView() -> some View {
        Group {
            ConsoleActionsBar()
            TabView(selection: $selectedTab) {
                SQLView()
                    .tabItem {

                        Label(loc("SQL"), systemImage: "ladybug.fill")
                            // .labelStyle(IconOnlyLabelStyle()) // icons never don't show up in tabs for some reason
                            .help(loc("SQL Console"))
                    }
                    .tag(ConsoleTab.sql)
                JSCView()
                    .tabItem {
                        Label(loc("JSC"), systemImage: "ant.fill")
                            .help(loc("JavaScript Console"))
                    }
                    .tag(ConsoleTab.jsc)
            }
        }
    }
}

enum ConsoleTab {
    case sql
    case jsc
}

extension ParquetViewer {

    func JSCView() -> some View {
        TextEditor(text: $script)
            .font(Font.custom("Menlo", size: 15, relativeTo: .body).bold())
            .foregroundColor((try? docState.jsc?.validate(script: script)) == nil ? Color.red : Color.primary)
            .cornerRadius(5)
            .padding()
    }

    func SQLView() -> some View {
        TextEditor(text: $sql)
            .font(Font.custom("Menlo", size: 15, relativeTo: .body).bold())
            .foregroundColor((try? docState.ctx.validate(sql: sql)) == nil ? Color.orange : Color.primary)
            .cornerRadius(5)
            .padding()
    }

    func performConsoleCommand() {
        dbg(selectedTab)
        switch selectedTab {
        case .sql: return performQuery()
        case .jsc: return performScript()
        }
    }

    func performScript() {
        dbg("performScript")
        let start = now()
        let dst = self.docState
        let ast = self.appState

        guard let jsc = dst.jsc else {
            return dbg("no jsc in doc")
        }

        let script = self.script

        dst.result.resultTime = -1
        dst.attempt(async: dst.useAsyncQuery) {
            defer {
                onmain { dst.result.resultTime = .init(start.millisFrom()) }
            }

            let result = try jsc.execute(script: script)

            // dbg("received \(dst.result.results?.columnSets.count ?? 0) columns with \(dst.result.resultCount ?? -1) elements in \(dst.result.resultTime ?? 0)ms")

            onmain {
                dst.result.resultTime = .init(start.millisFrom())
                addScriptHistory(script)

                dbg("recevied result", result)

                guard let result = result else { return dbg("no results") }

//                    dst.result.results = results
                dst.result.resultID = .init()

                self.script = self.script + "\n// \(result)\n"

                ast.calculateMemoryUsage() // re-calc usage
            }
        }
    }

    func performQuery() {
        dbg("performQuery")
        let start = now()

        let dst = self.docState
        let ast = self.appState

        let ctx = dst.ctx

        let sql = self.sql


        dst.result.resultTime = -1 // indicate that we are querying…
        dst.attempt(async: dst.useAsyncQuery) {
            let frame = try ctx.query(sql: sql)
            addQueryHistory(sql)
            let results = try frame?.collectResults()
            defer {
                onmain { dst.result.resultTime = .init(start.millisFrom()) }
            }

            onmain {
                guard let results = results else {
                    return dbg("no query results")
                }

                dst.result.results = results
                dst.result.resultID = .init()

                ast.calculateMemoryUsage() // re-calc usage

                dbg("received \(dst.result.results?.columnSets.count ?? 0) columns with \(dst.result.resultCount ?? -1) elements in \(dst.result.resultTime ?? 0)ms")
            }
        }
    }

    func addQueryHistory(_ sql: String) {
        // clear any previous history items with the same SQL
        sqlHistory.strings.removeAll { $0 == sql }
        sqlHistory.strings.insert(sql, at: 0)

        while sqlHistory.strings.count > sqlHistoryCount {
            sqlHistory.strings.removeLast()
        }

    }

    func addScriptHistory(_ script: String) {
        // clear any previous history items with the same SQL
        scriptHistory.strings.removeAll { $0 == sql }
        scriptHistory.strings.insert(sql, at: 0)

        while scriptHistory.strings.count > scriptHistoryCount {
            scriptHistory.strings.removeLast()
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

extension ArrowColumnSet {
    var columnName: String? {
        wip(batches.compactMap(\.name).first) // column name isn't always set

    }
}

private final class ArrowTableColumn : NSTableColumn {
    let columnSet: ArrowColumnSet

    init(id: String, columnSet: ArrowColumnSet) {
        self.columnSet = columnSet
        super.init(identifier: .init(id))
    }

    required init(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    /// The arrow data type for this column
    var dataType: ArrowDataType? {
        columnSet.batches.compactMap(\.dataType).first
    }

    /// The arrow column name for this
    var columnName: String? {
        columnSet.columnName
    }

    func objectValue(at index: Int) -> NSObject? {
        assert(index >= 0)
        assert(index < columnSet.count)

        guard let (i, vec) = columnSet.vectorIndex(forAbsoluteIndex: index) else {
            dbg("no vector chunk for index", index)
            return nil
        }

        do {
            switch vec.dataType {
            case .utf8:
                return try String.BufferView(vector: vec)[i] as NSString?
            case .int8:
                return try Int8.BufferView(vector: vec)[i] as NSNumber?
            case .int16:
                return try Int16.BufferView(vector: vec)[i] as NSNumber?
            case .int32:
                return try Int32.BufferView(vector: vec)[i] as NSNumber?
            case .int64:
                return try Int64.BufferView(vector: vec)[i] as NSNumber?
            case .uint8:
                return try UInt8.BufferView(vector: vec)[i] as NSNumber?
            case .uint16:
                return try UInt16.BufferView(vector: vec)[i] as NSNumber?
            case .uint32:
                return try UInt32.BufferView(vector: vec)[i] as NSNumber?
            case .uint64:
                return try UInt64.BufferView(vector: vec)[i] as NSNumber?

            default:
                throw SwiftArrowError.unsupportedDataType(vec.dataType)
            }
        } catch {
            // dbg("error accessing index", index, "\(error)")
            return "\(error)" as NSString
        }
    }
}


struct DataTableView : NSViewRepresentable {
    @Environment(\.controlSize) var controlSize
    @EnvironmentObject var docState: DocState

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
            guard let results = self.docState.result.results else {
                return
            }

            let colCount = results.columnSets.count

            let font = NSFont.monospacedDigitSystemFont(ofSize: controlSize.controlSize.systemFontSize, weight: .light)

            // clear and re-load; we could alternatively diff it and move columns around for similar queries…
            for col in tableView.tableColumns.reversed() {
                tableView.removeTableColumn(col)
            }

            for i in 0..<colCount {
                let id = "C\(i)"

                let columnSet = results.columnSets[i]

                let col = ArrowTableColumn(id: id, columnSet: columnSet)
                col.title = wip("Column \(i)") // TODO: extract name from column

                // col.sortDescriptorPrototype = NSSortDescriptor(key: wip(id), ascending: true)

                col.isEditable = false
                col.isHidden = false
                col.headerCell.isEnabled = true

                if let dataCell = col.dataCell as? NSCell {
                    switch col.dataType {
                    case .utf8, .utf8Large:
                        dataCell.formatter = nil // just strings
                        dataCell.alignment = .left
                    case .int8, .int16, .int32, .int64, .uint8, .uint16, .uint32, .uint64:
                        dataCell.formatter = NumberFormatter.integerFormatter
                        dataCell.alignment = .right
                    case .float16, .float32, .float64:
                        dataCell.formatter = NumberFormatter.decimalFormatter
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

        if context.coordinator.result?.resultID != self.docState.result.resultID {
            reloadColumns()
            context.coordinator.result = self.docState.result
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

/// Performs the given block, logging any errors that occur
func squelch<T>(_ block: @autoclosure () throws -> T) -> T? {
    do {
        return try block()
    } catch {
        dbg("caught error: \(error)")
        return nil
    }
}

extension CFAbsoluteTime {
    @inlinable func millisFrom() -> UInt {
        UInt(max(0, (CFAbsoluteTimeGetCurrent() - self) * 1_000))
    }
}

extension NumberFormatter {
    static let decimalFormatter: NumberFormatter = {
        let fmt = NumberFormatter()
        fmt.numberStyle = .decimal
        return fmt
    }()

    static let integerFormatter: NumberFormatter = {
        let fmt = NumberFormatter()
        fmt.numberStyle = .decimal
        return fmt
    }()
}

// MARK: Error Handling


public enum JSErrors : Error, CustomDebugStringConvertible {
    case valueNotObject(String)
    case objectNotFunction(String)

    @inlinable public var debugDescription: String {
        switch self {
        case .valueNotObject(let x): return "Value was not an object: '\(x)'"
        case .objectNotFunction(let x): return "Object was not a function: '\(x)'"
        }
    }
}

public struct JSException : LocalizedError {
    public let _domain: String = "JavaScriptContext"
    public let _code: Int = 0
    public let name: String
    public let message: String
    public let line: Int
    public let file: String
    public let stack: String

    public init?(exception: JSValueRef?, ctx: JSContextRef) {
        // extact "line" and "sourceURL" standard exception properties
        let pline = JSStringCreateWithUTF8CString("line")
        if pline != nil {
            defer { JSStringRelease(pline) }
            let jline = JSObjectGetProperty(ctx, exception, pline, nil)
            if jline != nil {
                let line = JSValueToNumber(ctx, jline, nil)
                if !line.isNaN {
                    self.line = Int(line)
                } else {
                    self.line = 0
                }
            } else {
                self.line = 0
            }
        } else {
            self.line = 0
        }

        func getStringProperty(_ prop: String, obj: JSObjectRef?) -> String? {
            let jprop = JSStringCreateWithUTF8CString(prop)
            if jprop != nil {
                defer { JSStringRelease(jprop) }
                let jvalue = JSObjectGetProperty(ctx, obj, jprop, nil)
                let jstring = JSValueToStringCopy(ctx, jvalue, nil)
                defer { JSStringRelease(jstring) }
                return jsStringToString(jstring!)
            } else {
                return nil
            }
        }

        guard let name = getStringProperty("name", obj: exception) else { return nil }
        self.name = name
        guard let message = getStringProperty("message", obj: exception) else { return nil }
        self.message = message
        guard let sourceURL = getStringProperty("sourceURL", obj: exception) else { return nil }
        self.file = sourceURL
        guard let stack = getStringProperty("stack", obj: exception) else { return nil }
        self.stack = stack
    }

    public var errorDescription: String? {
        message
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
