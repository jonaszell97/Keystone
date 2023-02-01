
import Foundation
@testable import Keystone

internal final class MockDelegate: KeystoneDelegate {
    /// In-memory storage for the duration of the test session.
    var mockStorage: [String: Data]
    
    func persist<Value: Codable>(_ value: Value?, withKey key: String) async {
        guard let value else {
            self.mockStorage[key] = nil
            return
        }
        
        let data = try! JSONEncoder().encode(value)
        self.mockStorage[key] = data
    }
    
    func load<Value: Codable>(_ type: Value.Type, withKey key: String) async -> Value? {
        guard let data = self.mockStorage[key] else {
            return nil
        }
        
        return try? JSONDecoder().decode(Value.self, from: data)
    }
    
    init() {
        self.mockStorage = [:]
    }
}

internal final class MockBackend: KeystoneBackend {
    /// The mocked events.
    var mockEvents: [KeystoneEvent] = []
    
    func persist(event: KeystoneEvent) async throws {
        self.mockEvents.append(event)
    }
    
    func loadEvents(in interval: DateInterval, updateStatus: @escaping (BackendStatus) -> Void) async throws -> [KeystoneEvent] {
        self.mockEvents.filter { interval.contains($0.date) }.sorted { $0.date < $1.date }
    }
}

internal final class MockEventBuilder {
    /// The user IDs.
    var users: Set<String>
    
    /// The event categories.
    let eventCategories: [String: [String: KeystoneEventData.CodingKeys]]
    
    init(userCount: Int, eventCategories: [String: [String: KeystoneEventData.CodingKeys]], using rng: inout ARC4RandomNumberGenerator) {
        self.users = []
        self.eventCategories = eventCategories
        
        while self.users.count < userCount {
            self.users.insert("\(rng.random(in: Int.min...Int.max))")
        }
    }
    
    func randomValue(type: KeystoneEventData.CodingKeys, using rng: inout ARC4RandomNumberGenerator) -> KeystoneEventData {
        switch type {
        case .noValue:
            return .noValue
        case .date:
            return .date(value: .now)
        case .number:
            return .number(value: rng.random(in: 0..<100))
        case .bool:
            return .bool(value: Bool.random(using: &rng))
        case .text:
            return .text(value: String.random(length: 3, using: &rng))
        case .codable:
            return .codable(value: Data())
        }
    }
    
    func generateRandomEvent(on date: Date, for userId: String, using rng: inout ARC4RandomNumberGenerator) -> KeystoneEvent {
        let category = eventCategories.keys.randomElement(using: &rng)!
        let columns = eventCategories[category]!
        
        var data = [String: KeystoneEventData]()
        for (column, dataType) in columns {
            data[column] = randomValue(type: dataType, using: &rng)
        }
        
        return KeystoneEvent(id: UUID(), userId: userId, category: category, date: date, data: data)
    }
    
    func generateEvents(count: Int, in interval: DateInterval, using rng: inout ARC4RandomNumberGenerator) -> [KeystoneEvent] {
        let step = interval.duration / TimeInterval(count)
        var currentDate = interval.start
        
        var events = [KeystoneEvent]()
        for _ in 0..<count {
            let userId = users.randomElement(using: &rng)!
            events.append(self.generateRandomEvent(on: currentDate, for: userId, using: &rng))
            
            currentDate = currentDate.addingTimeInterval(step)
        }
        
        return events
    }
}

internal final class TestDataBackend: KeystoneBackend {
    /// The loaded events.
    var events: [KeystoneEvent]
    
    init() {
        self.events = []
    }
    
    func initialize() async throws {
        guard let data: Data = Data(base64Encoded: _testEvents) else {
            fatalError("decoding test events failed")
        }
        
        self.events = try JSONDecoder().decode([KeystoneEvent].self, from: data)
    }
    
    func persist(event: KeystoneEvent) async throws { }
    
    func loadEvents(in interval: DateInterval, updateStatus: @escaping (BackendStatus) -> Void) async throws
    -> [KeystoneEvent]
    {
        self.events.filter { interval.contains($0.date) }
    }
}

// Adapted from Swift.Tensorflow
internal struct ARC4RandomNumberGenerator: RandomNumberGenerator, Codable {
    var state: [UInt8] = Array(0...255)
    var iPos: UInt8 = 0
    var jPos: UInt8 = 0
    let seed: [UInt8]
    
    /// Initialize ARC4RandomNumberGenerator using an array of UInt8. The array
    /// must have length between 1 and 256 inclusive.
    init(seed: [UInt8]) {
        precondition(seed.count > 0, "Length of seed must be positive")
        precondition(seed.count <= 256, "Length of seed must be at most 256")
        
        self.seed = seed
        
        var j: UInt8 = 0
        for i: UInt8 in 0...255 {
            j &+= S(i) &+ seed[Int(i) % seed.count]
            swapAt(i, j)
        }
    }
    
    init(seed seedValue: UInt64) {
        var seed = [UInt8](repeating: 0, count: 8)
        for i in 0..<8 {
            seed[i] = UInt8((seedValue >> (UInt64(i) * 8)) & 0xFF)
        }
        
        self.init(seed: seed)
    }
    
    init() {
        self.init(seed: UInt64.random(in: UInt64.min...UInt64.max))
    }
    
    mutating func reset() {
        self = .init(seed: seed)
    }
    
    /// Produce the next random UInt64 from the stream, and advance the internal state.
    mutating func next() -> UInt64 {
        var result: UInt64 = 0
        for _ in 0..<UInt64.bitWidth / UInt8.bitWidth {
            result <<= UInt8.bitWidth
            result += UInt64(nextByte())
        }
        
        return result
    }
    
    /// Helper to access the state.
    private func S(_ index: UInt8) -> UInt8 {
        return state[Int(index)]
    }
    
    /// Helper to swap elements of the state.
    private mutating func swapAt(_ i: UInt8, _ j: UInt8) {
        state.swapAt(Int(i), Int(j))
    }
    
    /// Generates the next byte in the keystream.
    private mutating func nextByte() -> UInt8 {
        iPos &+= 1
        jPos &+= S(iPos)
        swapAt(iPos, jPos)
        return S(S(iPos) &+ S(jPos))
    }
}

extension ARC4RandomNumberGenerator {
    /// Generate a random integer in a range .
    mutating func random(in range: Range<Int>) -> Int {
        Int.random(in: range, using: &self)
    }
    
    /// Generate a random integer in a range .
    mutating func random(in range: ClosedRange<Int>) -> Int {
        Int.random(in: range, using: &self)
    }
}

extension String {
    /// - returns: A random alphanumeric string of length `length` using the system RNG.
    static func random(length: Int) -> String {
        let letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        return String((0..<length).map{ _ in letters.randomElement()! })
    }
    
    /// - returns: A random alphanumeric string of length `length` using a given RNG.
    static func random(length: Int, using rng: inout ARC4RandomNumberGenerator) -> String {
        let letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        return String((0..<length).map{ _ in letters.randomElement(using: &rng)! })
    }
}
