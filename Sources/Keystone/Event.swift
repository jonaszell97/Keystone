
import Foundation

/// An event can represent anything happening in your App that you want to keep track of.
///
/// Events are created and persisted by ``KeystoneClient``. They feature a unique ID, a creation date, a user ID,
/// an event category, and event-specific data.
///
/// Events are passed to instances of ``EventAggregator`` in order to process them. ``KeystoneAnalyzer`` provides an
/// interface to query events and aggregators in specific date intervals.
public struct KeystoneEvent {
    /// The ID of the event.
    public let id: UUID
    
    /// The ID of the user that generated the event.
    public let userId: String
    
    /// The event category.
    public let category: String
    
    /// The creation date of this event.
    public let date: Date
    
    /// The data associated with this event.
    public let data: [String: KeystoneEventData]
    
    /// Create an event.
    ///
    /// - Parameters:
    ///   - id: The unique ID of the event.
    ///   - userId: The ID of the user that generated the event.
    ///   - category: The event category.
    ///   - date: The creation date of this event.
    ///   - data: The specific data associated with this event.
    public init(id: UUID,
                userId: String,
                category: String,
                date: Date,
                data: Dictionary<String, KeystoneEventData>) {
        self.id = id
        self.userId = userId
        self.category = category
        self.date = date
        self.data = data
    }
}

public extension KeystoneEvent {
    /// Create a copy of this event with some of its data replaced.
    ///
    /// - Parameters:
    ///   - id: The unique ID of the event.
    ///   - userId: The ID of the user that generated the event.
    ///   - category: The event category.
    ///   - date: The creation date of this event.
    ///   - data: The specific data associated with this event.
    /// - Returns: A new event with either the values of `self` or of the non-`nil` parameters.
    func copy(id: UUID? = nil, userId: String? = nil, category: String? = nil, date: Date? = nil,
              data: [String: KeystoneEventData]? = nil) -> KeystoneEvent {
        .init(id: id ?? self.id, userId: userId ?? self.userId,
              category: category ?? self.category, date: date ?? self.date,
              data: data ?? self.data)
    }
}

// MARK: Conformances

extension KeystoneEvent: Codable {
    enum CodingKeys: String, CodingKey {
        case id, userId, category, date, data,
             // FIXME: REMOVE
             analyticsId
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(id, forKey: .id)
        try container.encode(userId, forKey: .userId)
        try container.encode(category, forKey: .category)
        try container.encode(date, forKey: .date)
        try container.encode(data, forKey: .data)
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        
        let id: String
        do {
            id = try container.decode(String.self, forKey: .userId)
        }
        catch {
            id = try container.decode(String.self, forKey: .analyticsId)
        }
        
        self.init(
            id: try container.decode(UUID.self, forKey: .id),
            userId: id,
            category: try container.decode(String.self, forKey: .category),
            date: try container.decode(Date.self, forKey: .date),
            data: try container.decode(Dictionary<String, KeystoneEventData>.self, forKey: .data)
        )
    }
}

extension KeystoneEvent: Equatable {
    public static func ==(lhs: Self, rhs: Self) -> Bool {
        return (
            lhs.id == rhs.id
            && lhs.userId == rhs.userId
            && lhs.category == rhs.category
            && lhs.date == rhs.date
            && lhs.data == rhs.data
        )
    }
}

extension KeystoneEvent: Hashable {
    public func hash(into hasher: inout Hasher) {
        hasher.combine(id)
        hasher.combine(userId)
        hasher.combine(category)
        hasher.combine(date)
        hasher.combine(data)
    }
}
