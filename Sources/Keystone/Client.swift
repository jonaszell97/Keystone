
import Foundation

@MainActor public final class KeystoneClient {
    /// The configuration object.
    let config: KeystoneConfig
    
    /// The persistence API.
    let backend: KeystoneBackend
    
    /// Initialize the analytics state.
    public init(config: KeystoneConfig, backend: KeystoneBackend) {
        self.config = config
        self.backend = backend
    }
}

extension KeystoneClient {
    /// Submit an event.
    public func submitEvent(category: String, data: [String: KeystoneEventData]) async throws {
        try await backend.persist(event: createEvent(category: category, data: data))
    }
    
    /// Create an event without submitting it.
    public func createEvent(category: String, data: [String: KeystoneEventData]) -> KeystoneEvent {
        KeystoneEvent(id: UUID(),
                      userId: config.userIdentifier,
                      category: category, date: KeystoneAnalyzer.now, data: data)
    }
    
    /// Submit an event.
    public func submitEvent(_ event: KeystoneEvent) async throws {
        try await backend.persist(event: event)
    }
}
