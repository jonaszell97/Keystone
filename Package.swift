// swift-tools-version: 5.7
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "Keystone",
    platforms: [
        .iOS(.v15),
        .macOS(.v12),
    ],
    products: [
        .library(
            name: "Keystone",
            targets: ["Keystone"]),
    ],
    dependencies: [
    ],
    targets: [
        .target(
            name: "Keystone",
            dependencies: []),
        .testTarget(
            name: "KeystoneTests",
            dependencies: ["Keystone"],
            swiftSettings: [
                .define("TESTING"),
            ]),
    ]
)
