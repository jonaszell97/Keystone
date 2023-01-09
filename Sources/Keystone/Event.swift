
import Foundation

public struct KeystoneEvent {
    /// The ID of the event.
    public let id: UUID
    
    /// The analytics ID of the device that generated the event.
    public let analyticsId: String
    
    /// The event category.
    public let category: String
    
    /// The creation date of this event.
    public let date: Date
    
    /// The data associated with this event.
    public let data: [String: KeystoneEventData]
    
    /// Memberwise initializer.
    public init(id: UUID,
                analyticsId: String,
                category: String,
                date: Date,
                data: Dictionary<String, KeystoneEventData>) {
        self.id = id
        self.analyticsId = analyticsId
        self.category = category
        self.date = date
        self.data = data
    }
}

public extension KeystoneEvent {
    /// Create a copy of this event with a new date.
    func copy(id: UUID? = nil, analyticsId: String? = nil, category: String? = nil, date: Date? = nil,
              data: [String: KeystoneEventData]? = nil) -> KeystoneEvent {
        .init(id: id ?? self.id, analyticsId: analyticsId ?? self.analyticsId,
              category: category ?? self.category, date: date ?? self.date,
              data: data ?? self.data)
    }
}

// MARK: Conformances

extension KeystoneEvent: Codable {
    enum CodingKeys: String, CodingKey {
        case id, analyticsId, category, date, data
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(id, forKey: .id)
        try container.encode(analyticsId, forKey: .analyticsId)
        try container.encode(category, forKey: .category)
        try container.encode(date, forKey: .date)
        try container.encode(data, forKey: .data)
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.init(
            id: try container.decode(UUID.self, forKey: .id),
            analyticsId: try container.decode(String.self, forKey: .analyticsId),
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
            && lhs.analyticsId == rhs.analyticsId
            && lhs.category == rhs.category
            && lhs.date == rhs.date
            && lhs.data == rhs.data
        )
    }
}

extension KeystoneEvent: Hashable {
    public func hash(into hasher: inout Hasher) {
        hasher.combine(id)
        hasher.combine(analyticsId)
        hasher.combine(category)
        hasher.combine(date)
        hasher.combine(data)
    }
}
