
import Foundation

public enum EventProcessingResult {
    /// The event should be kept as-is.
    case keep
    
    /// The event should be discarded.
    case discard
    
    /// The event should be replaced with a different one.
    case replace(with: KeystoneEvent)
}

public protocol EventAggregator: AnyObject, CustomDebugStringConvertible {
    /// The ID of this aggregator.
    var id: String { get }
    
    /// Add a new event to this aggregator.
    @MainActor func addEvent(_ event: KeystoneEvent, column: EventColumn) -> EventProcessingResult
    
    /// Encode this aggregator's state.
    func encode() throws -> Data?
    
    /// Decode this aggregator's state from a given Data value.
    func decode(from data: Data) throws
    
    /// Reset this aggregator's state.
    func reset()
    
    /// If this is a chaining aggregator, returns the next aggragator in the chain. Otherwise nil.
    var next: EventAggregator? { get }
    
    /// If this is a chaining aggregator, returns the final aggragator in the chain. Otherwise the aggregator itself.
    var final: EventAggregator { get }
}

// MARK: Chaining

extension EventAggregator {
    /// If this is a chaining aggregator, returns the next aggragator in the chain. Otherwise nil.
    public var next: EventAggregator? { nil }
    
    /// If this is a chaining aggregator, returns the final aggragator in the chain. Otherwise the aggregator itself.
    public var final: EventAggregator {
        var result: any EventAggregator = self
        while let next = result.next {
            result = next
        }
        
        return result
    }
    
    /// Chain another aggregator to operate on the results of this one.
    public func then<Aggregator: EventAggregator>(_ builder: () -> Aggregator) -> some EventAggregator {
        ChainingAggregator(input: self, output: builder())
    }
    
    /// Find an aggregator of the given type within this aggregator.
    public func findAggregator(withId id: String) -> (any EventAggregator)? {
        if self.id == id {
            return self
        }
        
        return self.next?.findAggregator(withId: id)
    }
}

// MARK: ChainingAggregator

public final class ChainingAggregator {
    /// The ID of this aggregator.
    public var id: String { self.final.id }
    
    /// The aggregator the event is first passed to.
    public var input: any EventAggregator
    
    /// The aggregator the filtered events are then passed to.
    public var output: any EventAggregator
    
    /// Memberwise initializer.
    public init(input: any EventAggregator, output: any EventAggregator) {
        self.input = input
        self.output = output
    }
}

extension ChainingAggregator: EventAggregator {
    /// The next aggregator in the chain.
    public var next: EventAggregator? { output }
    
    public func addEvent(_ event: KeystoneEvent, column: EventColumn) -> EventProcessingResult {
        let result = self.input.addEvent(event, column: column)
        switch result {
        case .discard:
            return .discard
        case .keep:
            return self.output.addEvent(event, column: column)
        case .replace(let replacement):
            return self.output.addEvent(replacement, column: column)
        }
    }
    
    public func reset() {
        final.reset()
    }
    
    public var debugDescription: String {
        "ChainingAggregator(\(self.input.debugDescription) -> \(self.output.debugDescription))"
    }
}

extension ChainingAggregator {
    public func encode() throws -> Data? {
        nil
    }
    
    /// Decode this aggregator's state from a given Data value.
    public func decode(from data: Data) throws {
        
    }
}

// MARK: NumericStatsAggregator

open class NumericStatsAggregator {
    /// The ID of this aggregator.
    public var id: String
    
    /// The number of encountered values.
    public var valueCount: Int
    
    /// The sum of the values.
    public var sum: Double
    
    /// The running mean.
    public var runningAverage: Double
    
    /// The running variance.
    public var runningVariance: Double
    
    /// Memberwise initializer.
    public init(id: String,
                valueCount: Int = 0,
                sum: Double = 0,
                runningAverage: Double = 0,
                runningVariance: Double = 0) {
        self.id = id
        self.valueCount = valueCount
        self.sum = sum
        self.runningAverage = runningAverage
        self.runningVariance = runningVariance
    }
}

extension NumericStatsAggregator: EventAggregator {
    public func addEvent(_ event: KeystoneEvent, column: EventColumn) -> EventProcessingResult {
        guard let value = event.data[column.name], case .number(let number) = value else {
            return .keep
        }
        
        let previousAverage = self.runningAverage
        self.valueCount += 1
        self.sum += number
        
        // https://www.johndcook.com/blog/standard_deviation/
        // Update running mean
        // Mk = Mk-1 + (xk – Mk-1)/k
        self.runningAverage += (number - self.runningAverage) / Double(self.valueCount)
        
        // Update running variance
        // Sk = Sk-1 + (xk – Mk-1)*(xk – Mk)
        self.runningVariance += (number - previousAverage) * (number - self.runningAverage)
        
        return .keep
    }
    
    public func reset() {
        self.valueCount = 0
        self.sum = 0
        self.runningAverage = 0
        self.runningVariance = 0
    }
    
    public var debugDescription: String {
        "NumericStatsAggregator(valueCount: \(valueCount), sum: \(sum), runningAverage: \(runningAverage), runningVariance: \(runningVariance))"
    }
}

extension NumericStatsAggregator {
    public func encode() throws -> Data? {
        try JSONEncoder().encode([
            Double(self.valueCount),
            self.sum,
            self.runningAverage,
            self.runningVariance,
        ])
    }
    
    /// Decode this aggregator's state from a given Data value.
    public func decode(from data: Data) throws {
        let data = try JSONDecoder().decode([Double].self, from: data)
        self.valueCount = Int(data[0])
        self.sum = data[1]
        self.runningAverage = data[2]
        self.runningVariance = data[3]
    }
}

public extension NumericStatsAggregator {
    /// The running standard deviation.
    var runningStdDev: Double {
        sqrt(runningVariance)
    }
}

// MARK: CountingAggregator

open class CountingAggregator {
    /// The ID of this aggregator.
    public var id: String
    
    /// The number of encountered values.
    public var valueCount: Int
    
    /// Memberwise initializer.
    public init(id: String, valueCount: Int = 0) {
        self.id = id
        self.valueCount = valueCount
    }
}

extension CountingAggregator: EventAggregator {
    public func addEvent(_ event: KeystoneEvent, column: EventColumn) -> EventProcessingResult {
        self.valueCount += 1
        return .keep
    }
    
    public func reset() {
        valueCount = 0
    }
    
    public var debugDescription: String {
        "CountingAggregator(\(valueCount))"
    }
}

extension CountingAggregator {
    public func encode() throws -> Data? {
        try JSONEncoder().encode(self.valueCount)
    }
    
    /// Decode this aggregator's state from a given Data value.
    public func decode(from data: Data) throws {
        self.valueCount = try JSONDecoder().decode(Int.self, from: data)
    }
}

// MARK: LatestValueAggregator

open class LatestEventAggregator {
    /// The ID of this aggregator.
    public var id: String
    
    /// The latest event by each user.
    var latestEvents: [String: KeystoneEvent]
    
    /// Memberwise initializer.
    public init(id: String, latestEvents: [String: KeystoneEvent] = [:]) {
        self.id = id
        self.latestEvents = latestEvents
    }
}

extension LatestEventAggregator: EventAggregator {
    public func addEvent(_ event: KeystoneEvent, column: EventColumn) -> EventProcessingResult {
        self.latestEvents[event.analyticsId] = event
        return .keep
    }
    
    public func reset() {
        latestEvents = [:]
    }
    
    public var debugDescription: String {
        "LatestEventAggregator()"
    }
    
    public func encode() throws -> Data? {
        try JSONEncoder().encode(self.latestEvents)
    }
    
    public func decode(from data: Data) throws {
        self.latestEvents = try JSONDecoder().decode([String: KeystoneEvent].self, from: data)
    }
}

// MARK: FilteringAggregator

public final class FilteringAggregator {
    /// The ID of this aggregator.
    public let id: String
    
    /// The filter predicate.
    let filter: (KeystoneEventData) -> Bool
    
    /// Memberwise initializer.
    public init(filter: @escaping (KeystoneEventData) -> Bool) {
        self.id = UUID().uuidString
        self.filter = filter
    }
}

extension FilteringAggregator: EventAggregator {
    public func addEvent(_ event: KeystoneEvent, column: EventColumn) -> EventProcessingResult {
        guard let datum = event.data[column.name] else {
            return .discard
        }
        guard self.filter(datum) else {
            return .discard
        }
        
        return .keep
    }
    
    public func reset() {
        
    }
    
    public var debugDescription: String {
        "FilteringAggregator()"
    }
    
    public func encode() throws -> Data? {
       nil
    }
    
    /// Decode this aggregator's state from a given Data value.
    public func decode(from data: Data) throws {
        
    }
}

// MARK: MetaFilteringAggregator

public final class MetaFilteringAggregator {
    /// The ID of this aggregator.
    public let id: String
    
    /// The filter predicate.
    let filter: (KeystoneEvent) -> Bool
    
    /// Memberwise initializer.
    public init(filter: @escaping (KeystoneEvent) -> Bool) {
        self.filter = filter
        self.id = UUID().uuidString
    }
}

extension MetaFilteringAggregator: EventAggregator {
    public func addEvent(_ event: KeystoneEvent, column: EventColumn) -> EventProcessingResult {
        guard self.filter(event) else {
            return .discard
        }
        
        return .keep
    }
    
    public func reset() {
        
    }
    
    public var debugDescription: String {
        "MetaFilteringAggregator()"
    }
    
    public func encode() throws -> Data? {
        nil
    }
    
    /// Decode this aggregator's state from a given Data value.
    public func decode(from data: Data) throws {
        
    }
}

// MARK: MappingAggregator

public final class MappingAggregator {
    /// The ID of this aggregator.
    public let id: String
    
    /// The mapping function.
    let map: (KeystoneEventData) -> KeystoneEventData?
    
    /// Memberwise initializer.
    public init(map: @escaping (KeystoneEventData) -> Optional<KeystoneEventData>) {
        self.id = UUID().uuidString
        self.map = map
    }
}

extension MappingAggregator: EventAggregator {
    public func addEvent(_ event: KeystoneEvent, column: EventColumn) -> EventProcessingResult {
        guard let datum = event.data[column.name] else {
            return .discard
        }
        guard let mappedValue = self.map(datum) else {
            return .discard
        }
        
        var dataCopy = event.data
        dataCopy[column.name] = mappedValue
        
        let mappedEvent = event.copy(data: dataCopy)
        return .replace(with: mappedEvent)
    }
    
    public func reset() {
        
    }
    
    public var debugDescription: String {
        "MappingAggregator()"
    }
}

extension MappingAggregator {
    public func encode() throws -> Data? {
        nil
    }
    
    /// Decode this aggregator's state from a given Data value.
    public func decode(from data: Data) throws {
        
    }
}

// MARK: GroupingAggregator

open class GroupingAggregator: EventAggregator {
    /// The ID of this aggregator.
    public let id: String
    
    /// The grouped values.
    public var groupedValues: [KeystoneEventData: [KeystoneEvent]]
    
    /// Default initializer.
    public init(id: String, groupedValues: [KeystoneEventData: [KeystoneEvent]] = [:]) {
        self.id = id
        self.groupedValues = groupedValues
    }
    
    open func reset() {
        groupedValues = [:]
    }
    
    open func addEvent(_ event: KeystoneEvent, column: EventColumn) -> EventProcessingResult {
        guard let datum = event.data[column.name] else {
            return .keep
        }
        
        self.groupedValues.modify(key: datum, defaultValue: []) { $0.append(event) }
        return .keep
    }
    
    open var debugDescription: String {
        "GroupingAggregator(\(groupedValues))"
    }
    
    open func encode() throws -> Data? {
        try JSONEncoder().encode(self.groupedValues)
    }
    
    /// Decode this aggregator's state from a given Data value.
    open func decode(from data: Data) throws {
        self.groupedValues = try JSONDecoder().decode([KeystoneEventData: [KeystoneEvent]].self, from: data)
    }
    
    public var valueCount: Int { self.groupedValues.values.reduce(0) { $0 + $1.count } }
}

// MARK: CountingByGroupAggregator

open class CountingByGroupAggregator: EventAggregator {
    /// The ID of this aggregator.
    public let id: String
    
    /// The grouped values.
    public var groupedValues: [KeystoneEventData: Int]
    
    /// Default initializer.
    public init(id: String, groupedValues: [KeystoneEventData: Int] = [:]) {
        self.id = id
        self.groupedValues = groupedValues
    }
    
    open func reset() {
        groupedValues = [:]
    }
    
    open func addEvent(_ event: KeystoneEvent, column: EventColumn) -> EventProcessingResult {
        guard let datum = event.data[column.name] else {
            return .keep
        }
        
        self.groupedValues.modify(key: datum, defaultValue: 0) { $0 += 1 }
        return .keep
    }
    
    open var debugDescription: String {
        "CountingByGroupAggregator(\(groupedValues))"
    }
    
    open func encode() throws -> Data? {
        try JSONEncoder().encode(self.groupedValues)
    }
    
    /// Decode this aggregator's state from a given Data value.
    open func decode(from data: Data) throws {
        self.groupedValues = try JSONDecoder().decode([KeystoneEventData: Int].self, from: data)
    }
    
    public var valueCount: Int { self.groupedValues.values.reduce(0) { $0 + $1 } }
}

// MARK: DateAggregator

open class DateAggregator: EventAggregator {
    /// The ID of this aggregator.
    public let id: String
    
    /// The date components kept for keying.
    public let components: Set<Calendar.Component>
    
    /// The grouped values.
    public var groupedValues: [DateComponents: [KeystoneEvent]]
    
    /// Default initializer.
    public init(id: String, components: Set<Calendar.Component>, groupedValues: [DateComponents: [KeystoneEvent]] = [:]) {
        self.id = id
        self.components = components
        self.groupedValues = groupedValues
    }
    
    open func addEvent(_ event: KeystoneEvent, column: EventColumn) -> EventProcessingResult {
        let key = Calendar.gregorian.dateComponents(components, from: event.date)
        self.groupedValues.modify(key: key, defaultValue: []) { $0.append(event) }
        
        return .keep
    }
    
    open func reset() {
        groupedValues = [:]
    }
    
    open var debugDescription: String {
        "DateAggregator(\(groupedValues))"
    }
    
    open func encode() throws -> Data? {
        try JSONEncoder().encode(self.groupedValues)
    }
    
    /// Decode this aggregator's state from a given Data value.
    open func decode(from data: Data) throws {
        self.groupedValues = try JSONDecoder().decode([DateComponents: [KeystoneEvent]].self, from: data)
    }
    
    public var valueCount: Int { self.groupedValues.values.reduce(0) { $0 + $1.count } }
}

// MARK: CountingByDateAggregator

open class CountingByDateAggregator: EventAggregator {
    /// The ID of this aggregator.
    public let id: String
    
    /// The date components kept for keying.
    public let components: Set<Calendar.Component>
    
    /// The grouped values.
    public var groupedValues: [DateComponents: Int]
    
    /// Default initializer.
    public init(id: String, components: Set<Calendar.Component>, groupedValues: [DateComponents: Int] = [:]) {
        self.id = id
        self.components = components
        self.groupedValues = groupedValues
    }
    
    open func addEvent(_ event: KeystoneEvent, column: EventColumn) -> EventProcessingResult {
        let key = Calendar.gregorian.dateComponents(components, from: event.date)
        self.groupedValues.modify(key: key, defaultValue: 0) { $0 += 1 }
        
        return .keep
    }
    
    open func reset() {
        groupedValues = [:]
    }
    
    open var debugDescription: String {
        "CountingByDateAggregator(\(groupedValues))"
    }
    
    open func encode() throws -> Data? {
        try JSONEncoder().encode(self.groupedValues)
    }
    
    /// Decode this aggregator's state from a given Data value.
    open func decode(from data: Data) throws {
        self.groupedValues = try JSONDecoder().decode([DateComponents: Int].self, from: data)
    }
    
    public var valueCount: Int { self.groupedValues.values.reduce(0) { $0 + $1 } }
}

// MARK: Compound aggregators

public func PredicateAggregator(id: String, predicate: @escaping (KeystoneEventData) -> Bool) -> some EventAggregator {
    FilteringAggregator(filter: predicate).then { CountingAggregator(id: id) }
}
