
/// Delegate object used by ``KeystoneAnalyzer`` in order to persist its state
/// and be informed of status changes.
@MainActor public protocol KeystoneDelegate {
    /// Called whenever the status of the analyzer changes.
    ///
    /// This method is always called on the main actor and can be used to update the UI in your App,
    /// for example to inform the user about the current status.
    ///
    /// - Parameter state: The new state.
    func statusChanged(to state: AnalyzerStatus) async
    
    /// Handle a request to persist a value.
    ///
    /// - Parameters:
    ///   - value: The value that should be persisted.
    ///   - withKey: The key used to uniquely identify and retrieve the persisted value.
    func persist<Value: Codable>(_ value: Value?, withKey: String) async
    
    /// Try to load a persisted value.
    ///
    /// - Parameters:
    ///   - type: The type of value that should be loaded.
    ///   - withKey: The key used to uniquely identify and retrieve the persisted value.
    /// - Returns: The loaded value, or `nil` if no value with the given key was present.
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
