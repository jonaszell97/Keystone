
import Foundation

@MainActor internal final class KeystoneAnalyzerState {
    /// The current analyzer state.
    var currentState: IntervalAggregatorState
    
    /// The accumulated state of all aggregators.
    var accumulatedState: IntervalAggregatorState
    
    /// The state for each historical processed time interval.
    var historicalStates: [DateInterval: IntervalAggregatorState]
    
    /// The complete interval of processed events.
    var processedEventInterval: DateInterval
    
    /// Memberwise initializer.
    init(currentState: IntervalAggregatorState,
         accumulatedState: IntervalAggregatorState,
         historicalStates: Dictionary<DateInterval, IntervalAggregatorState>,
         processedEventInterval: DateInterval) {
        self.currentState = currentState
        self.accumulatedState = accumulatedState
        self.historicalStates = historicalStates
        self.processedEventInterval = processedEventInterval
    }
    
    /// Reset the state.
    func reset() {
        self.processedEventInterval = .init(start: self.currentState.interval.start, duration: 0)
        self.currentState.reset()
        self.accumulatedState.reset()
        self.historicalStates = [:]
    }
}

@MainActor internal final class IntervalAggregatorState {
    /// The date interval this state covers.
    let interval: DateInterval
    
    /// The interval of events in this state.
    var processedEventInterval: DateInterval
    
    /// The number of processed events.
    var eventCount: Int
    
    /// The state of the aggregators within this interval.
    var aggregators: [String: EventAggregator]
    
    /// IDs of known aggregators.
    var knownAggregators: Set<String>
    
    /// Memberwise initializer.
    init(interval: DateInterval,
         processedEventInterval: DateInterval? = nil,
         eventCount: Int = 0,
         knownAggregators: Set<String> = [],
         aggregators: [String: EventAggregator]) {
        self.interval = interval
        self.processedEventInterval = processedEventInterval ?? .init(start: interval.start, duration: 0)
        self.eventCount = eventCount
        self.aggregators = aggregators
        self.knownAggregators = knownAggregators
    }
    
    /// Reset the state.
    func reset() {
        self.processedEventInterval = .init(start: interval.start, duration: 0)
        self.eventCount = 0
        self.aggregators.forEach { $0.value.reset() }
        self.knownAggregators = []
    }
}

extension IntervalAggregatorState {
    /// The opaque encodable state object.
    func codableState() throws -> _KeystoneAggregatorState {
        _KeystoneAggregatorState(interval: interval,
                                processedEventInterval: self.processedEventInterval,
                                eventCount: self.eventCount,
                                knownAggregators: Set(aggregators.keys),
                                aggregators: try aggregators.map { .init(id: $0.key, data: try $0.value.encode()) })
    }
    
    /// Initialize from a codable state.
    convenience init(from codableState: _KeystoneAggregatorState, aggregators: [String: EventAggregator]) throws {
        self.init(interval: codableState.interval,
                  processedEventInterval: codableState.processedEventInterval,
                  knownAggregators: codableState.knownAggregators,
                  aggregators: aggregators)
        
        for aggregatorState in codableState.aggregators {
            guard
                let data = aggregatorState.data,
                let aggregator = aggregators[aggregatorState.id]
            else {
                continue
            }
            
            try aggregator.decode(from: data)
        }
    }
}

internal struct AggregatorState {
    /// The ID of the aggregator.
    let id: String
    
    /// The encoded state data.
    let data: Data?
}

/// Represents the internal state of an aggregator that is persisted across App launches.
public struct _KeystoneAggregatorState {
    /// The date interval this state covers.
    let interval: DateInterval
    
    /// The interval of events in this state.
    let processedEventInterval: DateInterval
    
    /// The number of processed events.
    let eventCount: Int
    
    /// IDs of known aggregators.
    let knownAggregators: Set<String>
    
    /// The state of the aggregators within this interval.
    let aggregators: [AggregatorState]
    
    /// The unique key for this state.
    var key: String {
        Self.key(for: interval)
    }
    
    /// The unique key for this state.
    static func key(for interval: DateInterval) -> String {
        "state-\(self.formatDate(interval.start))-\(self.formatDate(interval.end))"
    }
    
    /// Memberwise initializer.
    init(interval: DateInterval,
         processedEventInterval: DateInterval,
         eventCount: Int,
         knownAggregators: Set<String>,
         aggregators: [AggregatorState]) {
        self.interval = interval
        self.processedEventInterval = processedEventInterval
        self.eventCount = eventCount
        self.knownAggregators = knownAggregators
        self.aggregators = aggregators
    }
    
    private static func formatDate(_ date: Date) -> String {
        let components = Calendar.reference.dateComponents([.day, .month, .year], from: date)
        let format: (Int?, Int) -> String = { "\($0!)".leftPadding(toMinimumLength: $1, withPad: "0") }
        return "\(format(components.year, 4))\(format(components.month, 2))\(format(components.day, 2))"
    }
}

/// Represents a searchable and filterable list of events.
@MainActor public final class KeystoneEventList {
    /// The date interval this cache covers.
    public let interval: DateInterval
    
    /// The cached events.
    public let events: [KeystoneEvent]
    
    /// The event search index.
    let searchIndex: KeystoneSearchIndex?
    
    /// Create an event list.
    init(interval: DateInterval, events: [KeystoneEvent], searchIndex: KeystoneSearchIndex?) {
        self.interval = interval
        self.events = events
        self.searchIndex = searchIndex
    }
}

extension KeystoneEventList {
    /// Create an event list by combining several existing event lists.
    static func create(combining lists: [KeystoneEventList], in interval: DateInterval, updateStatus: (AnalyzerStatus) async -> Void) async -> KeystoneEventList {
        let sortedLists = lists.sorted { $0.events.first!.date < $1.events.first!.date }
        
        let totalEventCount = Double(lists.reduce(0) { $0 + $1.events.count })
        var processedEventCount: Double = 0
        
        var events = [KeystoneEvent]()
        var keywordsMap = [String: Set<UUID>]()
        
        for list in sortedLists {
            // Update status
            let progress = processedEventCount / totalEventCount
            await updateStatus(.updatingSearchIndex(progress: progress))
            defer { processedEventCount += Double(list.events.count) }
            
            // Find events in interval
            guard
                let firstIndex = (list.events.firstIndex { interval.contains($0.date) }),
                let lastIndex = (list.events.lastIndex { interval.contains($0.date) })
            else {
                continue
            }
            
            events.append(contentsOf: list.events[firstIndex...lastIndex])
            
            // Update search index
            if let searchIndex = list.searchIndex {
                for (keyword, ids) in searchIndex.keywordMap {
                    keywordsMap.modify(key: keyword, defaultValue: []) {
                        $0.insert(contentsOf: ids)
                    }
                }
            }
        }
        
        return KeystoneEventList(interval: interval, events: events,
                                 searchIndex: KeystoneSearchIndex(interval: interval, keywordMap: keywordsMap))
    }
}

extension _KeystoneAggregatorState: Codable {
    enum CodingKeys: String, CodingKey {
        case interval, processedEventInterval, eventCount, knownAggregators, aggregators
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(interval, forKey: .interval)
        try container.encode(processedEventInterval, forKey: .processedEventInterval)
        try container.encode(eventCount, forKey: .eventCount)
        try container.encode(knownAggregators, forKey: .knownAggregators)
        try container.encode(aggregators, forKey: .aggregators)
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.init(
            interval: try container.decode(DateInterval.self, forKey: .interval),
            processedEventInterval: try container.decode(DateInterval.self, forKey: .processedEventInterval),
            eventCount: try container.decode(Int.self, forKey: .eventCount),
            knownAggregators: try container.decode(Set<String>.self, forKey: .knownAggregators),
            aggregators: try container.decode(Array<AggregatorState>.self, forKey: .aggregators)
        )
    }
}

extension IntervalAggregatorState: Hashable {
    public static func ==(lhs: IntervalAggregatorState, rhs: IntervalAggregatorState) -> Bool {
        ObjectIdentifier(lhs) == ObjectIdentifier(rhs)
    }
    
    public func hash(into hasher: inout Hasher) {
        hasher.combine(ObjectIdentifier(self))
    }
}

extension AggregatorState: Codable {
    enum CodingKeys: String, CodingKey {
        case id, data
    }
    
    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(id, forKey: .id)
        try container.encode(data, forKey: .data)
    }
    
    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.init(
            id: try container.decode(String.self, forKey: .id),
            data: try container.decode(Optional<Data>.self, forKey: .data)
        )
    }
}

extension AggregatorState: Equatable {
    static func ==(lhs: Self, rhs: Self) -> Bool {
        return (
            lhs.id == rhs.id
            && lhs.data == rhs.data
        )
    }
}

extension AggregatorState: Hashable {
    func hash(into hasher: inout Hasher) {
        hasher.combine(id)
        hasher.combine(data)
    }
}

extension KeystoneEventList: Codable {
    enum CodingKeys: String, CodingKey {
        case interval, events, searchIndex
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(interval, forKey: .interval)
        try container.encode(events, forKey: .events)
        try container.encode(searchIndex, forKey: .searchIndex)
    }
    
    public convenience init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.init(
            interval: try container.decode(DateInterval.self, forKey: .interval),
            events: try container.decode(Array<KeystoneEvent>.self, forKey: .events),
            searchIndex: try? container.decode(KeystoneSearchIndex?.self, forKey: .searchIndex)
        )
    }
}

extension KeystoneEventList: Equatable {
    public static func ==(lhs: KeystoneEventList, rhs: KeystoneEventList) -> Bool {
        return (
            lhs.interval == rhs.interval
            && lhs.events == rhs.events
            && lhs.searchIndex == rhs.searchIndex
        )
    }
}

extension KeystoneEventList: Hashable {
    public func hash(into hasher: inout Hasher) {
        hasher.combine(interval)
        hasher.combine(events)
        hasher.combine(searchIndex)
    }
}
