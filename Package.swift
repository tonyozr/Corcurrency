// swift-tools-version:5.9
//
// C++ Corcurrency — Swift Concurrency for C++20, built on libdispatch.
//
// The library is pure C++. SwiftPM is used only as the build system, and the
// libdispatch shipped with the Swift toolchain is the only runtime dependency.
import PackageDescription

// Pure C++ executables built by SwiftPM on Windows still get `swiftrt.obj`
// linked in, which references the Swift runtime.
let cxxExecutableLinkerSettings: [LinkerSetting] = [
    .linkedLibrary("swiftCore", .when(platforms: [.windows])),
]

let package = Package(
    name: "Corcurrency",
    products: [
        .library(name: "Corcurrency", targets: ["Corcurrency"]),
    ],
    targets: [
        .target(
            name: "Corcurrency",
            linkerSettings: [
                .linkedLibrary("dispatch", .when(platforms: [.windows, .linux, .android, .openbsd])),
            ]
        ),
        .executableTarget(
            name: "CorcurrencyTests",
            dependencies: ["Corcurrency"],
            linkerSettings: cxxExecutableLinkerSettings
        ),
        .executableTarget(
            name: "Example",
            dependencies: ["Corcurrency"],
            linkerSettings: cxxExecutableLinkerSettings
        ),
        .executableTarget(
            name: "ThreadsDemo",
            dependencies: ["Corcurrency"],
            linkerSettings: cxxExecutableLinkerSettings
        ),
        .executableTarget(
            name: "SyncExample",
            dependencies: ["Corcurrency"],
            linkerSettings: cxxExecutableLinkerSettings
        ),
        // Using Corcurrency from Swift: a C++ component behind a C API...
        .target(
            name: "InventoryBridge",
            dependencies: ["Corcurrency"]
        ),
        // ...driven by native Swift concurrency.
        .executableTarget(
            name: "SwiftExample",
            dependencies: ["InventoryBridge"]
        ),
    ],
    cxxLanguageStandard: .cxx20
)
