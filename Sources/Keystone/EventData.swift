
import Foundation

/// Represents the data associated with an event. Events can have multiple columns with different types of data.
public enum KeystoneEventData {
    /// Numeric data.
    case number(value: Double)
    
    /// Text data.
    case text(value: String)
    
    /// Dates.
    case date(value: Date)
    
    /// Boolean data.
    case bool(value: Bool)
    
    /// Generic codable data.
    case codable(value: Data)
    
    /// The absence of a value.
    case noValue
}

public extension KeystoneEventData {
    /// Create codable data.
    static func codable<Value: Encodable>(_ value: Value) throws -> KeystoneEventData {
        let encoder = JSONEncoder()
        return .codable(value: try encoder.encode(value))
    }
    
    /// Try to decode this datum as a value of a given type.
    ///
    /// - Parameter type: The type to attempt to decode.
    /// - Returns: The decoded value, if decoding was successful.
    func decode<T: Decodable>(as type: T.Type) throws -> T {
        guard case .codable(let data) = self else {
            throw DecodingError.valueNotFound(
                T.self,
                .init(codingPath: [], debugDescription: "primitive value cannot be decoded"))
        }
        
        return try JSONDecoder().decode(T.self, from: data)
    }
    
    /// Numeric data.
    static func number(value: Int) -> KeystoneEventData {
        .number(value: Double(value))
    }
    
    /// Numeric data.
    static func number(value: Double?) -> KeystoneEventData {
        guard let value else { return .noValue }
        return .number(value: value)
    }
    
    /// Text data.
    static func text(value: String?) -> KeystoneEventData {
        guard let value else { return .noValue }
        return .text(value: value)
    }
    
    /// Dates.
    static func date(value: Date?) -> KeystoneEventData {
        guard let value else { return .noValue }
        return .date(value: value)
    }
    
    /// Boolean data.
    static func bool(value: Bool?) -> KeystoneEventData {
        guard let value else { return .noValue }
        return .bool(value: value)
    }
    
    /// Generic codable data.
    static func codable(value: Data?) -> KeystoneEventData {
        guard let value else { return .noValue }
        return .codable(value: value)
    }
}

public extension KeystoneEventData {
    /// The numeric value of this datum.
    var numericValue: Double? {
        guard case .number(let value) = self else {
            return nil
        }
        
        return value
    }
    
    /// The numeric value of this datum.
    var integerValue: Int? {
        guard case .number(let value) = self else {
            return nil
        }
        
        return Int(value)
    }
    
    /// The boolean value of this datum.
    var booleanValue: Bool? {
        guard case .bool(let value) = self else {
            return nil
        }
        
        return value
    }
    
    /// The string value of this datum.
    var stringValue: String? {
        guard case .text(let value) = self else {
            return nil
        }
        
        return value
    }
}

internal extension KeystoneEventData {
    /// Convert this value to an NSObject that can be persisted with CloudKit.
    var nsObject: NSObject {
        switch self {
        case .number(let value):
            return value as NSNumber
        case .text(let value):
            return value as NSString
        case .date(let value):
            return value as NSDate
        case .bool(let value):
            return value as NSNumber
        case .codable(let value):
            return value as NSData
        case .noValue:
            return 0 as NSNumber
        }
    }
}

// MARK: Conformances

extension KeystoneEventData: Codable {
    enum CodingKeys: String, CodingKey {
        case number, text, date, bool, codable, noValue
    }
    
    var codingKey: CodingKeys {
        switch self {
        case .number: return .number
        case .text: return .text
        case .date: return .date
        case .bool: return .bool
        case .codable: return .codable
        case .noValue: return .noValue
        }
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        switch self {
        case .number(let value):
            try container.encode(value, forKey: .number)
        case .text(let value):
            try container.encode(value, forKey: .text)
        case .date(let value):
            try container.encode(value, forKey: .date)
        case .bool(let value):
            try container.encode(value, forKey: .bool)
        case .codable(let value):
            try container.encode(value, forKey: .codable)
        case .noValue:
            try container.encodeNil(forKey: .noValue)
        }
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        switch container.allKeys.first {
        case .number:
            let value = try container.decode(Double.self, forKey: .number)
            self = .number(value: value)
        case .text:
            let value = try container.decode(String.self, forKey: .text)
            self = .text(value: value)
        case .date:
            let value = try container.decode(Date.self, forKey: .date)
            self = .date(value: value)
        case .bool:
            let value = try container.decode(Bool.self, forKey: .bool)
            self = .bool(value: value)
        case .codable:
            let value = try container.decode(Data.self, forKey: .codable)
            self = .codable(value: value)
        case .noValue:
            _ = try container.decodeNil(forKey: .noValue)
            self = .noValue
        default:
            throw DecodingError.dataCorrupted(
                DecodingError.Context(
                    codingPath: container.codingPath,
                    debugDescription: "Unabled to decode enum."
                )
            )
        }
    }
}

extension KeystoneEventData: Equatable {
    public static func ==(lhs: KeystoneEventData, rhs: KeystoneEventData) -> Bool {
        guard lhs.codingKey == rhs.codingKey else {
            return false
        }
        
        switch lhs {
        case .number(let value):
            guard case .number(let value_) = rhs else { return false }
            guard value == value_ else { return false }
        case .text(let value):
            guard case .text(let value_) = rhs else { return false }
            guard value == value_ else { return false }
        case .date(let value):
            guard case .date(let value_) = rhs else { return false }
            guard value == value_ else { return false }
        case .bool(let value):
            guard case .bool(let value_) = rhs else { return false }
            guard value == value_ else { return false }
        case .codable(let value):
            guard case .codable(let value_) = rhs else { return false }
            guard value == value_ else { return false }
        default: break
        }
        
        return true
    }
}

extension KeystoneEventData: Hashable {
    public func hash(into hasher: inout Hasher) {
        hasher.combine(self.codingKey.rawValue)
        switch self {
        case .number(let value):
            hasher.combine(value)
        case .text(let value):
            hasher.combine(value)
        case .date(let value):
            hasher.combine(value)
        case .bool(let value):
            hasher.combine(value)
        case .codable(let value):
            hasher.combine(value)
        default: break
        }
    }
}


