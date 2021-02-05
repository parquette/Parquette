//
//  ParquetteApp.swift
//  Shared
//
//  Created by Marc Prud'hommeaux on 2/4/21.
//

import SwiftUI

@main
struct ParquetteApp: App {
    @SceneBuilder var body: some Scene {
        DocumentGroup(newDocument: { ParquetteDocument() }) { file in
            ContentView(document: file.document)
        }
        // .windowToolbarStyle(UnifiedWindowToolbarStyle())
        // .windowToolbarStyle(ExpandedWindowToolbarStyle())
        .windowToolbarStyle(UnifiedCompactWindowToolbarStyle())
        Settings {
            SettingsView()
        }
    }
}


struct SettingsView : View {
    var body: some View {
        DocumentSettingsView()
            .padding()
//        TabView {
//            DocumentSettingsView()
//        }
//        .tabViewStyle(DefaultTabViewStyle())
    }
}


enum AppTheme : String, CaseIterable, Hashable {
    case system
    case light
    case dark

    var localizedTitle: String {
        switch self {
        case .system:
            return NSLocalizedString("System Default", comment: "")
        case .light:
            return NSLocalizedString("Light", comment: "")
        case .dark:
            return NSLocalizedString("Dark", comment: "")
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

struct DocumentSettingsView : View {
    @AppStorage("reopenDocuments") private var reopenDocuments = true
    @AppStorage("theme") private var theme = AppTheme.system

    var body: some View {
        Form {
            Text("Docuemnt")
            Toggle(NSLocalizedString("Re-Open Last Document", comment: ""), isOn: $reopenDocuments)

            Picker(NSLocalizedString("Theme", comment: ""), selection: $theme) {
                ForEach(AppTheme.allCases, id: \.self) { theme in
                    Text(theme.localizedTitle)
                }
            }
            .pickerStyle(RadioGroupPickerStyle())
        }
    }
}
