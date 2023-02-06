
import Foundation

/// Responsible for the creation and submission of events.
@MainActor public final class KeystoneClient {
    /// The configuration object.
    let config: KeystoneConfig
    
    /// The persistence API.
    let backend: KeystoneBackend
    
    /// Create a keystone client.
    ///
    /// - Parameters:
    ///   - config: The configuration.
    ///   - backend: The backend used for persisting events.
    public init(config: KeystoneConfig, backend: KeystoneBackend) {
        self.config = config
        self.backend = backend
    }
}

extension KeystoneClient {
    /// Create and submit an event with the current date and user ID.
    ///
    /// - Parameters:
    ///   - category: The category of the event.
    ///   - data: The event specific data.
    @discardableResult public func submitEvent(category: String, data: [String: KeystoneEventData]) async throws -> KeystoneEvent {
        let event = createEvent(category: category, data: data)
        try await backend.persist(event: event)
        
        return event
    }
    
    /// Create an event without submitting it.
    ///
    /// - Parameters:
    ///   - category: The category of the event.
    ///   - data: The event specific data.
    public func createEvent(category: String, data: [String: KeystoneEventData]) -> KeystoneEvent {
        KeystoneEvent(id: UUID(),
                      userId: config.userIdentifier,
                      category: category, date: KeystoneAnalyzer.now, data: data)
    }
    
    /// Submit an event.
    ///
    /// - Parameter event: The event to submit.
    public func submitEvent(_ event: KeystoneEvent) async throws {
        try await backend.persist(event: event)
    }
    
    /// Submit multiple events at once.
    ///
    /// - Parameter events: The events to submit.
    public func submitEvents(_ events: [KeystoneEvent]) async throws {
        try await backend.persist(events: events)
    }
}
