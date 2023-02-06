
import Foundation

/// The result of processing an event by the ``EventAggregator/addEvent(_:column:)`` function.
///
/// This value is only relevant for chaining aggregators, where it determines the event data that is forwarded
/// to the next aggregator in the chain, if any.
public enum EventProcessingResult {
    /// The event should be kept as-is.
    case keep
    
    /// The event should be discarded.
    case discard
    
    /// The event should be replaced with a different one.
    case replace(with: KeystoneEvent)
}

/// Aggregators analyze and process ``KeystoneEvent`` instances.
///
/// Aggregators are the way in which you analyze and process events in `Keystone`. An aggregator can be registered on all events,
/// an entire event category, or a single column of an event category.
///
/// The ``KeystoneAnalyzer`` calls the aggregators ``EventAggregator/addEvent(_:column:)`` method for all events that
/// match the scope in which the aggregator was registered.
///
/// Additionally, aggregators should support encoding and decoding their state to avoid having to reprocess all events whenever the App relaunches.
/// Aggregator state persistence is handled by the ``KeystoneAnalyzer``, which persists its state using a ``KeystoneDelegate``.
///
/// Aggregators can be chained, which allows an aggregator to receive events that were filtered or modified by a different aggregator earlier in the chain.
/// As a result, if you want to query an aggregator's state, you should make sure that you are accessing the ``EventAggregator/final-7ge2n`` instance,
/// which represents the last aggregator in a chain.
public protocol EventAggregator: AnyObject, CustomDebugStringConvertible {
    /// Add a new event to this aggregator.
    /// - Parameters:
    ///   - event: The event that was added.
    ///   - column: The column this aggregator was installed on, or `nil` if this aggregator does not belong to a column.
    /// - Returns: The result of processing the event.
    @MainActor func addEvent(_ event: KeystoneEvent, column: EventColumn?) -> EventProcessingResult
    
    /// Encode this aggregator's state.
    ///
    /// - Returns: The encoded aggregator state, or `nil` if this aggregator is stateless.
    func encode() throws -> Data?
    
    /// Decode this aggregator's state from a given Data value.
    ///
    /// - Parameter data: The encoded aggregator state.
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
    ///
    /// - Parameter builder: Builds the aggregator that the event processing result of `self` is forwarded to.
    /// - Returns: A `ChainingAggregator`that forwards events from `self` to the output of `builder`.
    public func then<Aggregator: EventAggregator>(_ builder: () -> Aggregator) -> some EventAggregator {
        ChainingAggregator(input: self, output: builder())
    }
}

// MARK: ChainingAggregator

/// An aggregator that forwards the event processing result of one aggregator to another aggregator.
///
/// You can easily create aggregator chains using the ``EventAggregator/then(_:)`` method.
/// ```swift
/// // An aggregator that filters events by a predicate and then counts them
/// FilteringAggregator(predicate: somePredicate).then { CountingAggregator() }
/// ```
public final class ChainingAggregator {
    /// The aggregator the event is first passed to.
    public var input: any EventAggregator
    
    /// The aggregator the filtered events are then passed to.
    public var output: any EventAggregator
    
    /// Create a chaining aggregator.
    ///
    /// - Parameters:
    ///   - input: The aggregator that processes the event.
    ///   - output: The aggregator to which the result of the first aggregator is forwarded to.
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

/// Collects statistics (count, sum, average, and variance) about a numeric event column.
///
/// The following example assumes that a `NumericStatsAggregator` is installed on the `amount` column of an
/// exemplary `moneySpent` event.
///
/// ```swift
///  client.submitEvent(category: "moneySpent", data: ["amount": .number(10)])
///  client.submitEvent(category: "moneySpent", data: ["amount": .number(15)])
///  client.submitEvent(category: "moneySpent", data: ["amount": .number(5)])
///  client.submitEvent(category: "moneySpent", data: ["amount": .number(50)])
///  client.submitEvent(category: "moneySpent", data: ["amount": .number(15)])
/// ```
/// The data collected by the `NumericStatsAggregator` can then be accessed as follows:
///
/// ```swift
/// let amountSpentStats = analyzer.findAggregator(/* ... */)
/// print(amountSpentStats.valueCount)      // prints "5"
/// print(amountSpentStats.sum)             // prints "95.0"
/// print(amountSpentStats.runningAverage)  // prints "19.0"
/// ```
open class NumericStatsAggregator {
    /// The number of encountered values.
    public var valueCount: Int
    
    /// The sum of the values.
    public var sum: Double
    
    /// The running mean.
    public var runningAverage: Double
    
    /// The running variance.
    public var runningVariance: Double
    
    /// Create a numeric stats aggregator with initial values.
    ///
    /// - Parameters:
    ///   - valueCount: The initial value count.
    ///   - sum: The initial sum.
    ///   - runningAverage: The initial mean.
    ///   - runningVariance: The initial variance.
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

/// Counts the number of events it encounters.
///
/// Example usage:
/// ```swift
/// builder.registerCategory("sessionStart") { category in
///     category.registerAggregator("Sessions") { CountingAggregator() }
/// }
/// ```
///
/// ```swift
/// client.submitEvent(category: "sessionStart")
/// client.submitEvent(category: "sessionStart")
/// client.submitEvent(category: "sessionStart")
///
/// // later...
/// print(sessionsAggregator.valueCount) // prints "3"
/// ```
open class CountingAggregator {
    /// The number of encountered events.
    public var valueCount: Int
    
    /// Create a counting aggregator with an  initial value count.
    ///
    /// - Parameter valueCount: The initial value count.
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

/// Keeps only the latest event for each user.
open class LatestEventAggregator {
    /// The latest event by each user.
    var latestEvents: [String: KeystoneEvent]
    
    /// Create a latest value aggregator.
    ///
    /// - Parameter latestEvents: The initial latest event dictionary.
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

/// Filters events by applying some predicate to the column value.
public final class FilteringAggregator {
    /// The filter predicate.
    let filter: (KeystoneEventData) -> Bool
    
    /// Create a filtering aggregator.
    ///
    /// - Parameter filter: The filter predicate.
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

/// Filters events by applying some predicate to the event itself.
public final class MetaFilteringAggregator {
    /// The filter predicate.
    let filter: (KeystoneEvent) -> Bool
    
    /// Create a filtering aggregator.
    ///
    /// - Parameter filter: The filter predicate.
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

/// Maps the value of an event column to a different value using a closure parameter.
public final class MappingAggregator {
    /// The mapping function.
    let map: (KeystoneEventData) -> KeystoneEventData?
    
    /// Create a mapping aggregator.
    ///
    /// - Parameter map: The mapping closure.
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

/// Groups events that share the same event data for the column this aggregator is installed on.
open class GroupingAggregator: EventAggregator {
    /// The grouped values.
    public var groupedValues: [KeystoneEventData: [KeystoneEvent]]
    
    /// The total event count.
    public var totalEventCount: Int { groupedValues.values.reduce(0) { $0 + $1.count } }
    
    /// Create a grouping aggregator.
    ///
    /// - Parameter groupedValues: The initial grouped values.
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

/// Counts events that share the same event data for the column this aggregator is installed on.
open class CountingByGroupAggregator: EventAggregator {
    /// The grouped values.
    public var groupedValues: [KeystoneEventData: Int]
    
    /// The total event count.
    public var totalEventCount: Int { groupedValues.values.reduce(0) { $0 + $1 } }
    
    /// Create a grouping aggregator.
    ///
    /// - Parameter groupedValues: The initial grouped value counts.
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

/// The granularity with which a DateAggregator groups its events.
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

/// Groups events that share the same date with a given granularity for the column this aggregator is installed on.
open class DateAggregator: EventAggregator {
    /// The date components kept for keying.
    public let scope: DateAggregatorScope
    
    /// The grouped values.
    public var groupedValues: [Date: [KeystoneEvent]]
    
    /// The total event count.
    public var totalEventCount: Int { groupedValues.values.reduce(0) { $0 + $1.count } }
    
    /// Create a grouping aggregator.
    ///
    /// - Parameters:
    ///   - scope: The time granularity within which to group values.
    ///   - groupedValues: The initial grouped values.
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

/// Counts events that share the same date with a given granularity for the column this aggregator is installed on.
open class CountingByDateAggregator: EventAggregator {
    /// The date components kept for keying.
    public let scope: DateAggregatorScope
    
    /// The grouped values.
    public var groupedValues: [Date: Int]
    
    /// The total event count.
    public var totalEventCount: Int { groupedValues.values.reduce(0) { $0 + $1 } }
    
    /// Create a grouping aggregator.
    ///
    /// - Parameters:
    ///   - scope: The time granularity within which to group values.
    ///   - groupedValues: The initial grouped values.
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

/// Verifies that an aggregator does not receive duplicate events.
public final class DuplicateEventChecker {
    /// The IDs of all events that were encountered so far by this aggregator.
    var encounteredEvents: Set<UUID> = []
    
    /// The number of duplicates that were found.
    var encounteredDuplicates: Int = 0
    
    /// Create a duplicate event checker.
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

/// Create an aggregator that counts the events that pass a given predicate.
///
/// - Parameter predicate: The predicate to filter values with.
/// - Returns: An aggregator that counts the events that pass the given `predicate`.
public func PredicateAggregator(predicate: @escaping (KeystoneEventData) -> Bool) -> some EventAggregator {
    FilteringAggregator(filter: predicate).then { CountingAggregator() }
}
