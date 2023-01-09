
import Foundation

@MainActor public protocol KeystoneDelegate {
    // MARK: Loading analytics
    
    /// Called whenever the status of the analyzer changes.
    func statusChanged(to: AnalyzerStatus) async
    
    // MARK: Cache
    
    /// Handle a request to persist a value.
    func persist<Value: Codable>(_ value: Value?, withKey: String) async
    
    /// Try to load a persisted value.
    func load<Value: Codable>(_ type: Value.Type, withKey: String) async -> Value?
}

extension KeystoneDelegate {
    /// Called whenever the status of the analyzer changes.
    public func statusChanged(to: AnalyzerStatus) async { }

    /// Handle a request to persist a value.
    public func persist<Value: Codable>(_ value: Value?, withKey: String) async { }
    
    /// Try to load a persisted value.
    public func load<Value: Codable>(_ type: Value.Type, withKey: String) async -> Value? { nil }
}
