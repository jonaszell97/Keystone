
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
    /// Add a new event to this aggregator.
    @MainActor func addEvent(_ event: KeystoneEvent, column: EventColumn?) -> EventProcessingResult
    
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
}

// MARK: ChainingAggregator

public final class ChainingAggregator {
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
    
    public func addEvent(_ event: KeystoneEvent, column: EventColumn?) -> EventProcessingResult {
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
    /// The number of encountered values.
    public var valueCount: Int
    
    /// The sum of the values.
    public var sum: Double
    
    /// The running mean.
    public var runningAverage: Double
    
    /// The running variance.
    public var runningVariance: Double
    
    /// Memberwise initializer.
    public init(valueCount: Int = 0,
                sum: Double = 0,
                runningAverage: Double = 0,
                runningVariance: Double = 0) {
        self.valueCount = valueCount
        self.sum = sum
        self.runningAverage = runningAverage
        self.runningVariance = runningVariance
    }
}

extension NumericStatsAggregator: EventAggregator {
    public func addEvent(_ event: KeystoneEvent, column: EventColumn?) -> EventProcessingResult {
        guard let column else {
            return .discard
        }
        guard let value = event.data[column.name], case .number(let number) = value else {
            return .discard
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
    /// The number of encountered values.
    public var valueCount: Int
    
    /// Memberwise initializer.
    public init(valueCount: Int = 0) {
        self.valueCount = valueCount
    }
}

extension CountingAggregator: EventAggregator {
    public func addEvent(_ event: KeystoneEvent, column: EventColumn?) -> EventProcessingResult {
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
    /// The latest event by each user.
    var latestEvents: [String: KeystoneEvent]
    
    /// Memberwise initializer.
    public init(latestEvents: [String: KeystoneEvent] = [:]) {
        self.latestEvents = latestEvents
    }
}

extension LatestEventAggregator: EventAggregator {
    public func addEvent(_ event: KeystoneEvent, column: EventColumn?) -> EventProcessingResult {
        self.latestEvents[event.userId] = event
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
    /// The filter predicate.
    let filter: (KeystoneEventData) -> Bool
    
    /// Memberwise initializer.
    public init(filter: @escaping (KeystoneEventData) -> Bool) {
        self.filter = filter
    }
}

extension FilteringAggregator: EventAggregator {
    public func addEvent(_ event: KeystoneEvent, column: EventColumn?) -> EventProcessingResult {
        guard let column else {
            return .discard
        }
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
    /// The filter predicate.
    let filter: (KeystoneEvent) -> Bool
    
    /// Memberwise initializer.
    public init(filter: @escaping (KeystoneEvent) -> Bool) {
        self.filter = filter
    }
}

extension MetaFilteringAggregator: EventAggregator {
    public func addEvent(_ event: KeystoneEvent, column: EventColumn?) -> EventProcessingResult {
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
    /// The mapping function.
    let map: (KeystoneEventData) -> KeystoneEventData?
    
    /// Memberwise initializer.
    public init(map: @escaping (KeystoneEventData) -> Optional<KeystoneEventData>) {
        self.map = map
    }
}

extension MappingAggregator: EventAggregator {
    public func addEvent(_ event: KeystoneEvent, column: EventColumn?) -> EventProcessingResult {
        guard let column else {
            return .discard
        }
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
    /// The grouped values.
    public var groupedValues: [KeystoneEventData: [KeystoneEvent]]
    
    /// The total event count.
    public var totalEventCount: Int { groupedValues.values.reduce(0) { $0 + $1.count } }
    
    /// Default initializer.
    public init(groupedValues: [KeystoneEventData: [KeystoneEvent]] = [:]) {
        self.groupedValues = groupedValues
    }
    
    open func reset() {
        groupedValues = [:]
    }
    
    open func addEvent(_ event: KeystoneEvent, column: EventColumn?) -> EventProcessingResult {
        guard let column else {
            return .discard
        }
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
    /// The grouped values.
    public var groupedValues: [KeystoneEventData: Int]
    
    /// The total event count.
    public var totalEventCount: Int { groupedValues.values.reduce(0) { $0 + $1 } }
    
    /// Default initializer.
    public init(groupedValues: [KeystoneEventData: Int] = [:]) {
        self.groupedValues = groupedValues
    }
    
    open func reset() {
        groupedValues = [:]
    }
    
    open func addEvent(_ event: KeystoneEvent, column: EventColumn?) -> EventProcessingResult {
        guard let column else {
            return .discard
        }
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

public enum DateAggregatorScope: String {
    /// Group values by hour.
    case hour
    
    /// Group values by day.
    case day
    
    /// Group values by week.
    case week
    
    /// Group values by month.
    case month
    
    /// Group values by year.
    case year
}

internal extension DateAggregatorScope {
    func scopeStartDate(from date: Date) -> Date {
        switch self {
        case .hour:
            let components = Calendar.reference.dateComponents([.year,.month,.day,.hour], from: date)
            return Calendar.reference.date(from: components)!
        case .day:
            return date.startOfDay
        case .week:
            return date.startOfWeek(weekStartsOn: .monday)
        case .month:
            return date.startOfMonth
        case .year:
            return date.startOfYear
        }
    }
}

open class DateAggregator: EventAggregator {
    /// The date components kept for keying.
    public let scope: DateAggregatorScope
    
    /// The grouped values.
    public var groupedValues: [Date: [KeystoneEvent]]
    
    /// The total event count.
    public var totalEventCount: Int { groupedValues.values.reduce(0) { $0 + $1.count } }
    
    /// Default initializer.
    public init(scope: DateAggregatorScope, groupedValues: [Date: [KeystoneEvent]] = [:]) {
        self.scope = scope
        self.groupedValues = groupedValues
    }
    
    open func addEvent(_ event: KeystoneEvent, column: EventColumn?) -> EventProcessingResult {
        let key = scope.scopeStartDate(from: event.date)
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
        self.groupedValues = try JSONDecoder().decode([Date: [KeystoneEvent]].self, from: data)
    }
    
    public var valueCount: Int { self.groupedValues.values.reduce(0) { $0 + $1.count } }
}

// MARK: CountingByDateAggregator

open class CountingByDateAggregator: EventAggregator {
    /// The date components kept for keying.
    public let scope: DateAggregatorScope
    
    /// The grouped values.
    public var groupedValues: [Date: Int]
    
    /// The total event count.
    public var totalEventCount: Int { groupedValues.values.reduce(0) { $0 + $1 } }
    
    /// Default initializer.
    public init(scope: DateAggregatorScope, groupedValues: [Date: Int] = [:]) {
        self.scope = scope
        self.groupedValues = groupedValues
    }
    
    open func addEvent(_ event: KeystoneEvent, column: EventColumn?) -> EventProcessingResult {
        let key = scope.scopeStartDate(from: event.date)
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
        self.groupedValues = try JSONDecoder().decode([Date: Int].self, from: data)
    }
    
    public var valueCount: Int { self.groupedValues.values.reduce(0) { $0 + $1 } }
}

// MARK: DuplicateEventChecker

public final class DuplicateEventChecker {
    /// The IDs of all events that were encountered so far by this aggregator.
    var encounteredEvents: Set<UUID> = []
    
    /// The number of duplicates that were found.
    var encounteredDuplicates: Int = 0
    
    /// Default initializer.
    public init() { }
}

extension DuplicateEventChecker: EventAggregator {
    public func addEvent(_ event: KeystoneEvent, column: EventColumn?) -> EventProcessingResult {
        guard !encounteredEvents.insert(event.id).inserted else {
            return .keep
        }
        
        encounteredDuplicates += 1
        return .keep
    }
    
    private struct CodableState: Codable {
        let encounteredEvents: Set<UUID>
        let encounteredDuplicates: Int
    }
    
    public func encode() throws -> Data? {
        try JSONEncoder().encode(CodableState(encounteredEvents: encounteredEvents, encounteredDuplicates: encounteredDuplicates))
    }
    
    public func decode(from data: Data) throws {
        let state = try JSONDecoder().decode(CodableState.self, from: data)
        self.encounteredEvents = state.encounteredEvents
        self.encounteredDuplicates = state.encounteredDuplicates
    }
    
    public func reset() {
        encounteredEvents.removeAll()
        encounteredDuplicates = 0
    }
    
    public var debugDescription: String { "DuplicateEventChecker(\(encounteredDuplicates) duplicates)" }
}

// MARK: Compound aggregators

public func PredicateAggregator(predicate: @escaping (KeystoneEventData) -> Bool) -> some EventAggregator {
    FilteringAggregator(filter: predicate).then { CountingAggregator() }
}
