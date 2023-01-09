
//import AppUtilities
import Foundation

public enum AnalyzerStatus {
    /// The analyzer is ready.
    case ready
    
    /// The analyzer is initalizing its state.
    case initializingState
    
    /// Events are being persisted.
    case persistingEvents(progress: Double)
    
    /// The state is being persisted.
    case persistingState(progress: Double)
    
    /// The analyzer is fetching events.
    case fetchingEvents(count: Int)
    
    /// The analyzer is decoding events.
    case decodingEvents(progress: Double)
    
    /// The analyzer is processing events.
    case processingEvents(progress: Double)
}

@MainActor public final class KeystoneAnalyzer {
    /// The current status.
    var status: AnalyzerStatus
    
    /// The configuration object.
    let config: KeystoneConfig
    
    /// The persistence API.
    let backend: KeystoneBackend
    
    /// The delegate object.
    let delegate: KeystoneDelegate
    
    /// The analyzer state.
    let state: KeystoneAnalyzerState
    
    /// The known event categories.
    public let eventCategories: [EventCategory]
    
    /// Map form aggregator IDs to the columns that contain the respective aggregator.
    let aggregatorColumns: [String: [EventColumn]]
    
    /// Whether the state is ready to submit events.
    public var isReady: Bool {
        guard case .ready = status else {
            return false
        }
        
        return true
    }
    
    /// Initialize the analytics state.
    internal init(config: KeystoneConfig, delegate: KeystoneDelegate, backend: KeystoneBackend,
                  eventCategories: [EventCategory]) async throws {
        self.status = .initializingState
        self.config = config
        self.delegate = delegate
        self.backend = backend
        self.eventCategories = eventCategories
        
        var aggregatorColumns: [String: [EventColumn]] = [:]
        let currentStateAggregators = Self.instantiateAggregators(eventCategories: eventCategories, aggregatorColumns: &aggregatorColumns)
        
        self.aggregatorColumns = aggregatorColumns
        
        let currentInterval = Self.currentEventInterval
        let currentState = try await Self.state(in: currentInterval, delegate: delegate, eventCategories: eventCategories, aggregators: currentStateAggregators)
        let accumulatedState = try await Self.state(in: Self.allEncompassingDateInterval, delegate: delegate, eventCategories: eventCategories)
        
        self.state = .init(currentState: currentState,
                           accumulatedState: accumulatedState,
                           historicalStates: [:],
                           processedEventInterval: accumulatedState.processedEventInterval)
        
        // Check if the current state needs to be updated
        try await self.ensureCurrentStateValidity()
        
        // Check for new aggregators and reset if there are any
        try await self.checkForNewAggregators()
        
        // Initialize with complete history
        if accumulatedState.processedEventInterval.duration.isZero {
            try await self.loadAndProcessAllHistory()
        }
        // Update with the latest events
        else {
            try await self.loadAndProcessNewEvents()
        }
        
        await updateStatus(.ready)
    }
    
    /// Update the analyzer status.
    func updateStatus(_ status: AnalyzerStatus) async {
        guard status.isSignificantlyDifferentFrom(self.status) else {
            return
        }
        
        self.status = status
        await self.delegate.statusChanged(to: status)
    }
    
    /// Reset the analytics state.
    public func reset() async throws {
        self.state.reset()
        
        // Check if the current state needs to be updated
        try await self.ensureCurrentStateValidity()
        
        // Initialize with complete history
        try await self.loadAndProcessAllHistory()
    }
}

// MARK: Data access

public extension KeystoneAnalyzer {
    /// Find an aggregator with the given ID in the selected time interval.
    func findAggregator(withId id: String, in interval: DateInterval) async -> EventAggregator? {
        guard let state = try? await self.state(in: interval) else {
            return nil
        }
        
        return state.aggregators.first { $0.id == id }
    }
    
    /// Find all aggregators belonging to a column in the given interval.
    func findAggregators(for category: String, in interval: DateInterval) async -> [EventAggregator] {
        guard let state = try? await self.state(in: interval) else {
            return []
        }
        
        return state.aggregators.filter { self.aggregatorColumns[$0.id]?.contains { $0.categoryName == category } ?? false }
    }
    
    /// Load new events.
    func loadNewEvents() async throws {
        try await self.loadAndProcessNewEvents()
    }
}

// MARK: Date intervals

public extension KeystoneAnalyzer {
    /// Get the date interval covering all time.
    static let allEncompassingDateInterval: DateInterval = {
        DateInterval(start: Date(timeIntervalSinceReferenceDate: 0), duration: 300 * 365 * 24 * 60 * 60)
    }()
    
    /// Get the current date interval.
    static var currentEventInterval: DateInterval {
        let now = Date.now
        return .init(start: now.startOfMonth, end: now.endOfMonth)
    }
    
    /// Get the current date interval.
    static func interval(before: DateInterval) -> DateInterval {
        let previous = before.start.addingTimeInterval(-24*60*60)
        return .init(start: previous.startOfMonth, end: previous.endOfMonth)
    }
    
    /// Get the current date interval.
    static func interval(after: DateInterval) -> DateInterval {
        let next = after.end.addingTimeInterval(24*60*60)
        return .init(start: next.startOfMonth, end: next.endOfMonth)
    }
    
    /// Get the current date interval.
    static func interval(containing date: Date) -> DateInterval {
        .init(start: date.startOfMonth, end: date.endOfMonth)
    }
}

// MARK: Event processing

extension KeystoneAnalyzer {
    /// Register a list of events.
    func processEvents(_ events: [KeystoneEvent]) async throws {
        guard let first = events.first, let last = events.last else {
            return
        }
        
        let now = Date.now
        let previousStatus = self.status
        
        var processedEvents = 0
        let totalEvents = events.count
        
        // Update historical and accumulated states for each event
        var modifiedStates = Set<IntervalAggregatorState>()
        for event in events {
            assert(event.date < now, "encountered an event from the future")
            
            await updateStatus(.processingEvents(progress: Double(processedEvents) / Double(totalEvents)))
            processedEvents += 1
            
            guard !self.state.processedEventInterval.contains(event.date) else {
                continue
            }
            
            let interval = Self.interval(containing: event.date)
            let state = try await self.state(in: interval)
            
            try await state.processEvent(event, aggregatorColumns: self.aggregatorColumns)
            try await self.state.accumulatedState.processEvent(event, aggregatorColumns: self.aggregatorColumns)
            
            modifiedStates.insert(state)
        }
        
        // Expand the interval of processed events
        self.state.processedEventInterval.expand(toContain: first.date)
        self.state.processedEventInterval.expand(toContain: last.date)
        
        await updateStatus(.persistingState(progress: 0))
        
        // Persist the events
        await persistEvents(events)
        
        var persistedStates = 0
        let totalStatesToPersist = modifiedStates.count + 1
        
        // Persist the modified states
        try await self.persistState(self.state.accumulatedState)
        for state in modifiedStates {
            await updateStatus(.persistingState(progress: Double(persistedStates + 1) / Double(totalStatesToPersist)))
            persistedStates += 1
            
            try await self.persistState(state)
        }
        
        await updateStatus(previousStatus)
    }
    
    /// Load and process all events.
    func loadAndProcessAllHistory() async throws {
        try await self.loadAndProcessEvents(in: .init(start: .distantPast, end: .now))
    }
    
    /// Load and process new events.
    func loadAndProcessNewEvents() async throws {
        let processedEventInterval = self.state.processedEventInterval
        let newEventInterval = DateInterval(start: processedEventInterval.end, end: .now)
        
        try await self.loadAndProcessEvents(in: newEventInterval)
    }
    
    /// Load and process new events.
    func loadAndProcessEvents(in interval: DateInterval) async throws {
        let events = try await backend.loadEvents(in: interval) { status in
            switch status {
            case .ready:
                break
            case .fetchedRecords(let count):
                Task {
                    await self.updateStatus(.fetchingEvents(count: count))
                }
            case .processingRecords(let progress):
                Task {
                    await self.updateStatus(.decodingEvents(progress: progress))
                }
            }
        }
        
        try await self.processEvents(events)
    }
    
    /// Check if there are new, uninitialized aggregators.
    func checkForNewAggregators() async throws {
        let uninitializedAggregators = state.accumulatedState.aggregators.filter { !state.accumulatedState.knownAggregators.contains($0.id) }
        guard !uninitializedAggregators.isEmpty else {
            return
        }
        
        config.log?(.debug, "resetting state because of new aggregators: [\(uninitializedAggregators.map { $0.id }.joined(separator: ", "))]")
        
        // No need to fetch the events from the backend again
        var intervals = self.state.historicalStates.keys.map { $0 }
        intervals.append(state.currentState.interval)
        
        self.state.reset()
        
        // Register the events we saved from before the reset
        for interval in intervals {
            async let events = await self.loadEvents(in: interval)
            guard let events = await events else {
                continue
            }
            
            try await self.processEvents(events)
        }
    }
}

// MARK: Initialization

extension KeystoneAnalyzer {
    /// Persist a time interval state.
    func persistState(_ state: IntervalAggregatorState) async throws {
        try await Self.persistState(state, delegate: delegate)
    }
    
    /// Persist a time interval state.
    static func persistState(_ state: IntervalAggregatorState, delegate: KeystoneDelegate) async throws {
        // Update known aggregators
        state.knownAggregators.insert(contentsOf: state.aggregators.map { $0.id })
        
        // Persist
        let key = KeystoneAggregatorState.key(for: state.interval)
        await delegate.persist(try state.codableState(), withKey: key)
    }
    
    /// Check if the current state needs to be updated.
    func ensureCurrentStateValidity() async throws {
        let currentInterval = Self.currentEventInterval
        guard state.currentState.interval != currentInterval else {
            return
        }
        
        state.historicalStates[state.currentState.interval] = state.currentState
        state.currentState = .init(interval: currentInterval,
                                   aggregators: Self.instantiateAggregators(eventCategories: eventCategories))
        
        try await self.persistState(state.currentState)
    }
    
    /// Fetch or create the state within a given interval.
    static func state(in interval: DateInterval, delegate: KeystoneDelegate,
                      eventCategories: [EventCategory],
                      aggregators: [EventAggregator]? = nil) async throws
        -> IntervalAggregatorState
    {
        let aggregators = aggregators ?? Self.instantiateAggregators(eventCategories: eventCategories)
        
        if let state = await delegate.load(KeystoneAggregatorState.self, withKey: KeystoneAggregatorState.key(for: interval)) {
            return try IntervalAggregatorState(from: state, aggregators: aggregators)
        }
        
        let state = IntervalAggregatorState(interval: interval, aggregators: aggregators)
        try await Self.persistState(state, delegate: delegate)
        
        return state
    }
    
    /// Fetch or create the state within a given interval.
    func state(in interval: DateInterval) async throws -> IntervalAggregatorState {
        if let cachedState = self.state.historicalStates[interval] {
            return cachedState
        }
        
        if interval == Self.currentEventInterval {
            return self.state.currentState
        }
        
        let state = try await Self.state(in: interval, delegate: delegate, eventCategories: eventCategories)
        self.state.historicalStates[interval] = state
        
        return state
    }
    
    /// Instantiate the aggregators for a state.
    static func instantiateAggregators(eventCategories: [EventCategory]) -> [EventAggregator] {
        var aggregators = [EventAggregator]()
        for category in eventCategories {
            for column in category.columns {
                aggregators.append(contentsOf: column.instantiateAggregators())
            }
        }
        
        return aggregators
    }
    
    /// Instantiate the aggregators for a state and remember the columns they belong to.
    static func instantiateAggregators(eventCategories: [EventCategory], aggregatorColumns: inout [String: [EventColumn]]) -> [EventAggregator] {
        var aggregators = [EventAggregator]()
        for category in eventCategories {
            for column in category.columns {
                let instantiatedAggregators = column.instantiateAggregators()
                aggregators.append(contentsOf: instantiatedAggregators)
                
                for inst in instantiatedAggregators {
                    aggregatorColumns.modify(key: inst.id, defaultValue: []) { $0.append(column) }
                }
            }
        }
        
        return aggregators
    }
}

// MARK: Events

extension KeystoneAnalyzer {
    private static func formatDate(_ date: Date) -> String {
        let components = Calendar.gregorian.dateComponents([.day, .month, .year], from: date)
        let format: (Int?, Int) -> String = { "\($0!)".leftPadding(toMinimumLength: $1, withPad: "0") }
        return "\(format(components.year, 4))\(format(components.month, 2))\(format(components.day, 2))"
    }
    
    private static func eventsKey(for interval: DateInterval) -> String {
        "analytics-events-\(formatDate(interval.start))-\(formatDate(interval.end))"
    }
    
    /// Persist the given events.
    func persistEvents(_ events: [KeystoneEvent]) async {
        guard !events.isEmpty else {
            return
        }
        
        var processedEventCount = 0
        let totalEventCount = events.count
        
        var currentInterval: DateInterval = Self.interval(containing: events[0].date)
        var currentIntervalEvents = await self.loadEvents(in: currentInterval) ?? []
        
        for event in events {
            await updateStatus(.persistingEvents(progress: Double(processedEventCount) / Double(totalEventCount)))
            processedEventCount += 1
            
            let eventInterval = Self.interval(containing: event.date)
            if eventInterval != currentInterval {
                await delegate.persist(currentIntervalEvents, withKey: Self.eventsKey(for: currentInterval))
                
                currentInterval = eventInterval
                currentIntervalEvents = await self.loadEvents(in: currentInterval) ?? []
            }
            
            currentIntervalEvents.append(event)
        }
        
        await delegate.persist(currentIntervalEvents, withKey: Self.eventsKey(for: currentInterval))
    }
    
    /// Load the given events in the given interval.
    public func loadEvents(in interval: DateInterval) async -> [KeystoneEvent]? {
        await delegate.load([KeystoneEvent].self, withKey: Self.eventsKey(for: interval))
    }
}

// MARK: Builder

public struct KeystoneAnalyzerBuilder {
    /// The configuration object.
    let config: KeystoneConfig
    
    /// The persistence API.
    let backend: KeystoneBackend
    
    /// The delegate object.
    let delegate: KeystoneDelegate
    
    /// The known event categories.
    var eventCategories: [EventCategory]
    
    /// Memberwise initializer.
    public init(config: KeystoneConfig,
                backend: KeystoneBackend,
                delegate: KeystoneDelegate) {
        self.config = config
        self.backend = backend
        self.delegate = delegate
        self.eventCategories = []
    }
}

public extension KeystoneAnalyzerBuilder {
    /// Register an event category.
    @discardableResult mutating func registerCategory(name: String, modify: (inout EventCategoryBuilder) -> Void) -> KeystoneAnalyzerBuilder {
        var builder = EventCategoryBuilder(name: name)
        modify(&builder)
        
        self.eventCategories.append(builder.build())
        return self
    }
    
    /// Build the analyzer.
    func build() async throws -> KeystoneAnalyzer {
        try await KeystoneAnalyzer(config: config, delegate: delegate, backend: backend,
                                    eventCategories: eventCategories)
    }
}

// MARK: Conformances

extension AnalyzerStatus {
    func isSignificantlyDifferentFrom(_ rhs: AnalyzerStatus) -> Bool {
        switch self {
        case .fetchingEvents(let count):
            guard case .fetchingEvents(let count_) = rhs else { return true }
            // > 1% difference
            guard abs(1 - (Double(count) / Double(count_))) >= 0.01 else { return false }
        case .decodingEvents(let progress):
            guard case .decodingEvents(let progress_) = rhs else { return true }
            guard abs(progress - progress_) >= 0.01 else { return false }
        case .processingEvents(let progress):
            guard case .processingEvents(let progress_) = rhs else { return true }
            guard abs(progress - progress_) >= 0.01 else { return false }
        case .persistingState(let progress):
            guard case .persistingState(let progress_) = rhs else { return true }
            guard abs(progress - progress_) >= 0.01 else { return false }
        case .persistingEvents(let progress):
            guard case .persistingEvents(let progress_) = rhs else { return true }
            guard abs(progress - progress_) >= 0.01 else { return false }
        case .initializingState:
            guard case .initializingState = rhs else {
                return true
            }
            
            return false
        case .ready:
            guard case .ready = rhs else {
                return true
            }
            
            return false
        }
        
        return true
    }
}

extension AnalyzerStatus {
    enum CodingKeys: String {
        case ready, initializingState, persistingEvents, persistingState, fetchingEvents, decodingEvents, processingEvents
    }
    
    var codingKey: CodingKeys {
        switch self {
        case .ready: return .ready
        case .initializingState: return .initializingState
        case .persistingEvents: return .persistingEvents
        case .persistingState: return .persistingState
        case .fetchingEvents: return .fetchingEvents
        case .decodingEvents: return .decodingEvents
        case .processingEvents: return .processingEvents
        }
    }
}

extension AnalyzerStatus: Equatable {
    public static func ==(lhs: AnalyzerStatus, rhs: AnalyzerStatus) -> Bool {
        guard lhs.codingKey == rhs.codingKey else {
            return false
        }
        
        switch lhs {
        case .persistingEvents(let progress):
            guard case .persistingEvents(let progress_) = rhs else { return false }
            guard progress == progress_ else { return false }
        case .persistingState(let progress):
            guard case .persistingState(let progress_) = rhs else { return false }
            guard progress == progress_ else { return false }
        case .fetchingEvents(let count):
            guard case .fetchingEvents(let count_) = rhs else { return false }
            guard count == count_ else { return false }
        case .decodingEvents(let progress):
            guard case .decodingEvents(let progress_) = rhs else { return false }
            guard progress == progress_ else { return false }
        case .processingEvents(let progress):
            guard case .processingEvents(let progress_) = rhs else { return false }
            guard progress == progress_ else { return false }
        default: break
        }
        
        return true
    }
}

extension AnalyzerStatus: Hashable {
    public func hash(into hasher: inout Hasher) {
        hasher.combine(self.codingKey.rawValue)
        switch self {
        case .persistingEvents(let progress):
            hasher.combine(progress)
        case .persistingState(let progress):
            hasher.combine(progress)
        case .fetchingEvents(let count):
            hasher.combine(count)
        case .decodingEvents(let progress):
            hasher.combine(progress)
        case .processingEvents(let progress):
            hasher.combine(progress)
        default: break
        }
    }
}




