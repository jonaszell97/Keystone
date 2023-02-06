
import Foundation

/// The status of a ``KeystoneAnalyzer``.
public enum AnalyzerStatus {
    /// The analyzer is ready.
    case ready
    
    /// The analyzer is initalizing its state.
    case initializingState
    
    /// The analyzer is persisting events.
    case persistingEvents(progress: Double)
    
    /// The analyzer is persisting its state.
    case persistingState(progress: Double)
    
    /// The analyzer is fetching events.
    case fetchingEvents(count: Int, source: String)
    
    /// The analyzer is decoding events.
    case decodingEvents(progress: Double, source: String)
    
    /// The analyzer is processing events.
    case processingEvents(progress: Double, detail: String? = nil)
}

fileprivate struct AggregatorProcessingInfo {
    /// The columns this aggregator belongs to.
    let columns: [EventColumn?]
    
    /// The interval this aggregator receives events in.
    let interval: DateInterval?
}

/// Responsible for event processing, persistence, and aggregator state management.
///
/// A `KeystoneAnalyzer` serves as the interface to the ``EventAggregator`` instances used to process your events.
/// You can create a `KeystoneAnalyzer` with the ``KeystoneAnalyzerBuilder`` helper struct. It allows you to
/// configure the event categories, columns, and aggregators relevant to your App.
///
/// ```swift
/// var builder = KeystoneAnalyzerBuilder(config: config, backend: backend, delegate: delegate)
/// builder.registerCategory(/* ... */)
/// builder.registerAllEventAggregator(/* ... */)
///
/// let analyzer = try await builder.build()
/// ```
///
/// `KeystoneAnalyzer` provides methods for finding aggregators within a given time interval. It automatically creates the necessary
/// aggregator instances and feeds them all events in the given time interval.
///
/// ```swift
/// // Get the number of total sessions throughout the lifetime of the App
/// let allTimeSessionCount = try await analyzer.findAggregator(
///     withId: "Sessions",
///     in: KeystoneAnalyzer.allEncompassingDateInterval)
///
/// // Get the number of sessions today
/// let todaySessionCount = try await analyzer.findAggregator(
///     withId: "Sessions",
///     in: KeystoneAnalyzer.dayInterval(containing: .now))
///
/// // Fetch all aggregators installed for the `sessionStartEvent`
/// // event category with the data from this month
/// let thisMonthSessionAggregators = try await analyzer.findAggregators(
///     category: "sessionStartEvent",
///     in: KeystoneAnalyzer.currentEventInterval)
/// ```
///
/// You can also fetch a list of events that were submitted within a given time interval.
///
/// ```swift
/// let todaysEvents = try await analyzer.getProcessedEvents(
///     in: KeystoneAnalyzer.dayInterval(containing: .now))
/// ```
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
    
    /// The data aggregators for all events.
    let allEventAggregators: [AggregatorMeta]
    
    /// The known event categories.
    public let eventCategories: [EventCategory]
    
    /// Map form aggregator IDs to the columns that contain the respective aggregator.
    fileprivate let aggregatorInfo: [String: AggregatorProcessingInfo]
    
    /// States with non-normal intervals that have been queried.
    var nonNormalStates: [DateInterval: IntervalAggregatorState]
    
    /// Whether the state is ready to submit events.
    public var isReady: Bool {
        guard case .ready = status else {
            return false
        }
        
        return true
    }
    
    /// Create an analyzer.
    internal init(config: KeystoneConfig, delegate: KeystoneDelegate, backend: KeystoneBackend,
                  eventCategories: [EventCategory], allEventAggregators: [AggregatorMeta]) async throws {
        self.status = .initializingState
        self.config = config
        self.delegate = delegate
        self.backend = backend
        self.eventCategories = eventCategories
        self.allEventAggregators = allEventAggregators
        self.nonNormalStates = [:]
        
        var aggregatorIntervals: [String: DateInterval] = [:]
        var aggregatorColumns: [String: [EventColumn?]] = [:]
        var aggregatorIds = Set<String>()
        
        for category in eventCategories {
            for column in category.columns {
                for meta in column.aggregators {
                    aggregatorColumns.modify(key: meta.id, defaultValue: []) { $0.append(column) }
                    aggregatorIntervals[meta.id] = meta.interval
                    aggregatorIds.insert(meta.id)
                }
            }
        }
        
        for meta in allEventAggregators {
            aggregatorColumns.modify(key: meta.id, defaultValue: []) { $0.append(nil) }
            aggregatorIntervals[meta.id] = meta.interval
            aggregatorIds.insert(meta.id)
        }
        
        var aggregatorInfo = [String: AggregatorProcessingInfo]()
        for id in aggregatorIds {
            aggregatorInfo[id] = .init(columns: aggregatorColumns[id] ?? [], interval: aggregatorIntervals[id])
        }
        
        self.aggregatorInfo = aggregatorInfo
        
        let currentInterval = Self.currentEventInterval
        let currentState = try await Self.state(in: currentInterval, delegate: delegate, eventCategories: eventCategories,
                                                allEventAggregators: allEventAggregators)
        let accumulatedState = try await Self.state(in: Self.allEncompassingDateInterval, delegate: delegate, eventCategories: eventCategories,
                                                    allEventAggregators: allEventAggregators)
        
        self.state = .init(currentState: currentState,
                           accumulatedState: accumulatedState,
                           historicalStates: [:],
                           processedEventInterval: accumulatedState.processedEventInterval)
        
        // Check if the current state needs to be updated
        try await self.ensureCurrentStateValidity()
        
        // Initialize with complete history
        if accumulatedState.processedEventInterval.duration.isZero {
            try await self.loadAndProcessAllHistory()
        }
        else {
            // Check for new aggregators and reset if there are any
            try await self.checkForNewAggregators()
            
            // Update with the latest events
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
    
    /// Reset the state of the analyzer.
    public func reset() async throws {
        for (_, state) in state.historicalStates {
            await self.removeState(state)
        }
        
        await self.removeState(state.currentState)
        await self.removeState(state.accumulatedState)
        
        self.nonNormalStates = [:]
        self.state.reset()
        
        // Check if the current state needs to be updated
        try await self.ensureCurrentStateValidity()
        
        // Initialize with complete history
        try await self.loadAndProcessAllHistory()
    }
}

// MARK: Data access

extension KeystoneAnalyzer {
    /// Find an aggregator with the given ID in the selected time interval.
    ///
    /// - Parameters:
    ///   - id: The ID of the aggregator to search for.
    ///   - interval: The interval within which to aggregate events for this aggregator.
    /// - Returns: An aggregator with the given ID that has processed all events in the given interval, or `nil` if none was found.
    public func findAggregator(withId id: String, in interval: DateInterval) async throws -> EventAggregator? {
        guard Self.isNormalized(interval) else {
            return try await self.findAggregator(withId: id, inNonNormalInterval: interval)
        }
        
        let state = try await self.state(in: interval)
        return state.aggregators.first { $0.key == id }?.value
    }
    
    /// Find an aggregator with the given ID in the selected time interval.
    func findAggregator(withId id: String, inNonNormalInterval interval: DateInterval) async throws -> EventAggregator? {
        let state: IntervalAggregatorState
        if let existingState = self.nonNormalStates[interval] {
            state = existingState
        }
        else {
            state = try await self.createNonNormalAggregatorState(in: interval)
        }
        
        return state.aggregators.first { $0.key == id }?.value
    }
    
    /// Find all aggregators belonging to a category in the given interval.
    ///
    /// - Parameters:
    ///   - category: The name of the category.
    ///   - interval: The interval within which to aggregate events for this aggregator.
    /// - Returns: All aggregators belonging to the category that have processed all events in the given interval.
    public func findAggregators(for category: String, in interval: DateInterval) async throws -> [String: EventAggregator] {
        guard Self.isNormalized(interval) else {
            return try await self.findAggregators(for: category, inNonNormalInterval: interval)
        }
        
        let state = try await self.state(in: interval)
        return state.aggregators.filter { self.aggregatorInfo[$0.key]?.columns.contains { $0?.categoryName == category } ?? false }
    }
    
    /// Find all aggregators belonging to a column in the given non-normal interval.
    func findAggregators(for category: String, inNonNormalInterval interval: DateInterval) async throws -> [String: EventAggregator] {
        let state: IntervalAggregatorState
        if let existingState = self.nonNormalStates[interval] {
            state = existingState
        }
        else {
            state = try await self.createNonNormalAggregatorState(in: interval)
        }
        
        return state.aggregators.filter { self.aggregatorInfo[$0.key]?.columns.contains { $0?.categoryName == category } ?? false }
    }
    
    /// Load and process new events.
    public func loadNewEvents() async throws {
        try await self.loadAndProcessNewEvents()
    }
}

// MARK: Date intervals

public extension KeystoneAnalyzer {
    // MARK: Yearly
    
    /// Get the yearly interval containing the given date.
    nonisolated static func yearInterval(containing date: Date) -> DateInterval {
        .init(start: date.startOfYear, end: date.endOfYear)
    }
    
    /// Get the yearly interval before the given one.
    nonisolated static func yearInterval(before: DateInterval) -> DateInterval {
        let previous = before.start.addingTimeInterval(-24*60*60)
        return .init(start: previous.startOfYear, end: previous.endOfYear)
    }
    
    /// Get the yearly interval after the given one.
    nonisolated static func yearInterval(after: DateInterval) -> DateInterval {
        let next = after.end.addingTimeInterval(24*60*60)
        return .init(start: next.startOfYear, end: next.endOfYear)
    }
    
    // MARK: Monthly (normalized)
    
    /// Get the date interval covering all time.
    nonisolated static let allEncompassingDateInterval: DateInterval = {
        DateInterval(start: Date(timeIntervalSinceReferenceDate: 0), duration: 300 * 365 * 24 * 60 * 60)
    }()
    
    /// Get the monthly interval containing the current date.
    nonisolated static var currentEventInterval: DateInterval {
        let now = KeystoneAnalyzer.now
        return .init(start: now.startOfMonth, end: now.endOfMonth)
    }
    
    /// Get the monthly interval before the given one.
    nonisolated static func interval(before: DateInterval) -> DateInterval {
        let previous = before.start.addingTimeInterval(-24*60*60)
        return .init(start: previous.startOfMonth, end: previous.endOfMonth)
    }
    
    /// Get the monthly interval after the given one.
    nonisolated static func interval(after: DateInterval) -> DateInterval {
        let next = after.end.addingTimeInterval(24*60*60)
        return .init(start: next.startOfMonth, end: next.endOfMonth)
    }
    
    /// Get the monthly interval containing the given date.
    nonisolated static func interval(containing date: Date) -> DateInterval {
        .init(start: date.startOfMonth, end: date.endOfMonth)
    }
    
    /// Whether a given interval is normalized, i.e. monthly.
    nonisolated static func isNormalized(_ interval: DateInterval) -> Bool {
        interval == Self.interval(containing: interval.start) || interval == allEncompassingDateInterval
    }
    
    // MARK: Weekly
    
    /// Get the weekly interval containing the given date.
    nonisolated static func weekInterval(containing date: Date, weekStartsOnMonday: Bool) -> DateInterval {
        .init(start: date.startOfWeek(weekStartsOn: weekStartsOnMonday ? .monday : .sunday),
              end: date.endOfWeek(weekStartsOn: weekStartsOnMonday ? .monday : .sunday))
    }
    
    /// Get the weekly interval before the given one.
    nonisolated static func weekInterval(before: DateInterval, weekStartsOnMonday: Bool) -> DateInterval {
        let previous = before.start.addingTimeInterval(-24*60*60)
        return .init(start: previous.startOfWeek(weekStartsOn: weekStartsOnMonday ? .monday : .sunday),
                     end: previous.endOfWeek(weekStartsOn: weekStartsOnMonday ? .monday : .sunday))
    }
    
    /// Get the weekly interval after the given one.
    nonisolated static func weekInterval(after: DateInterval, weekStartsOnMonday: Bool) -> DateInterval {
        let next = after.end.addingTimeInterval(24*60*60)
        return .init(start: next.startOfWeek(weekStartsOn: weekStartsOnMonday ? .monday : .sunday),
                     end: next.endOfWeek(weekStartsOn: weekStartsOnMonday ? .monday : .sunday))
    }
    
    // MARK: Daily
    
    /// Get the daily interval containing the given date.
    nonisolated static func dayInterval(containing date: Date) -> DateInterval {
        .init(start: date.startOfDay, end: date.endOfDay)
    }
    
    /// Get the daily interval before the given one.
    nonisolated static func dayInterval(before: DateInterval) -> DateInterval {
        let previous = before.start.addingTimeInterval(-24*60*60)
        return .init(start: previous.startOfDay, end: previous.endOfDay)
    }
    
    /// Get the daily interval after the given one.
    nonisolated static func dayInterval(after: DateInterval) -> DateInterval {
        let next = after.end.addingTimeInterval(24*60*60)
        return .init(start: next.startOfDay, end: next.endOfDay)
    }
}

// MARK: Event processing

extension KeystoneAnalyzer {
    /// Register a list of events.
    func processEvents(_ events: [KeystoneEvent]) async throws {
        guard let first = events.first, let last = events.last else {
            return
        }
        
        let now = KeystoneAnalyzer.now
        let previousStatus = self.status
        
        var processedEvents = 0
        let totalEvents = events.count
        
        // Update historical and accumulated states for each event
        var modifiedStates: Set<IntervalAggregatorState> = [self.state.accumulatedState]
        for event in events {
            assert(event.date < now, "encountered an event from the future")
            
            await updateStatus(.processingEvents(progress: Double(processedEvents) / Double(totalEvents)))
            processedEvents += 1
            
            guard !self.state.processedEventInterval.contains(event.date) else {
                continue
            }
            
            // Update normalized intervals
            
            let interval = Self.interval(containing: event.date)
            let state = try await self.state(in: interval)
            
            try await state.processEvent(event, aggregatorColumns: self.aggregatorInfo, isNewEvent: true)
            try await self.state.accumulatedState.processEvent(event, aggregatorColumns: self.aggregatorInfo,
                                                               isNewEvent: true)
            
            modifiedStates.insert(state)
            
            // Update non-normalized intervals
            for (interval, state) in nonNormalStates {
                guard interval.contains(event.date) else {
                    continue
                }
                
                try await state.processEvent(event, aggregatorColumns: self.aggregatorInfo, isNewEvent: true)
            }
        }
        
        // Expand the interval of processed events
        self.state.processedEventInterval.expand(toContain: first.date)
        self.state.processedEventInterval.expand(toContain: last.date)
        
        await updateStatus(.persistingState(progress: 0))
        
        // Persist the modified states
        try await self.persistAggregatorStates(modifiedStates)
        
        await updateStatus(previousStatus)
    }
    
    /// Initialize new aggregators with all historical events.
    func processHistoricalEvents(_ events: [KeystoneEvent], forAggregatorIds ids: Set<String>) async throws {
        let aggregatorColumns = self.aggregatorInfo.filter { ids.contains($0.key) }
        
        let now = KeystoneAnalyzer.now
        let previousStatus = self.status
        
        var processedEvents = 0
        let totalEvents = events.count
        
        // Update historical and accumulated states for each event
        var modifiedStates: Set<IntervalAggregatorState> = [self.state.accumulatedState]
        for event in events {
            assert(event.date < now, "encountered an event from the future")
            
            await updateStatus(.processingEvents(progress: Double(processedEvents) / Double(totalEvents),
                                                 detail: "New Aggregators"))
            processedEvents += 1
            
            let interval = Self.interval(containing: event.date)
            let state = try await self.state(in: interval)
            
            try await state.processEvent(event, aggregatorColumns: aggregatorColumns, isNewEvent: false)
            try await self.state.accumulatedState.processEvent(event, aggregatorColumns: aggregatorColumns,
                                                               isNewEvent: false)
            
            modifiedStates.insert(state)
        }
        
        await updateStatus(.persistingState(progress: 0))
        
        // Persist the modified states
        try await self.persistAggregatorStates(modifiedStates)
        
        await updateStatus(previousStatus)
    }
    
    /// Persist the states of modified aggregators.
    func persistAggregatorStates(_ states: Set<IntervalAggregatorState>) async throws {
        var persistedStates = 0
        let totalStatesToPersist = states.count
        
        for state in states {
            await updateStatus(.persistingState(progress: Double(persistedStates + 1) / Double(totalStatesToPersist)))
            persistedStates += 1
            
            try await self.persistState(state)
        }
    }
    
    /// Load and process all events.
    func loadAndProcessAllHistory() async throws {
        try await self.loadAndProcessEvents(in: .init(start: .distantPast, end: Self.now))
    }
    
    /// Load and process new events.
    func loadAndProcessNewEvents() async throws {
        let processedEventInterval = self.state.processedEventInterval
        let newEventInterval = DateInterval(start: processedEventInterval.end, end: Self.now)
        
        try await self.loadAndProcessEvents(in: newEventInterval)
    }
    
    /// Load and process new events.
    func loadAndProcessEvents(in interval: DateInterval) async throws {
        let updateStatus: (BackendStatus) -> Void = { status in
            switch status {
            case .ready:
                break
            case .fetchedRecords(let count):
                Task {
                    await self.updateStatus(.fetchingEvents(count: count, source: "backend"))
                }
            case .processingRecords(let progress):
                Task {
                    await self.updateStatus(.decodingEvents(progress: progress, source: "backend"))
                }
            }
        }
        
        // Use as many events from cache as possible
        if var cachedEvents = await self.getProcessedEvents(in: interval), !cachedEvents.isEmpty {
            config.log?(.debug, "loaded \(cachedEvents.count) events from cache")
            
            let cachedEventsInterval = DateInterval(start: cachedEvents.first!.date, end: cachedEvents.last!.date)
            
            // Load earlier events
            if interval.start < cachedEventsInterval.start {
                let earlierEvents = try await backend.loadEvents(in: .init(start: interval.start, end: cachedEventsInterval.start),
                                                                 updateStatus: updateStatus)
                
                cachedEvents.insert(contentsOf: earlierEvents, at: cachedEvents.startIndex)
                
                // Persist the events
                await persistEvents(earlierEvents)
                config.log?(.debug, "loaded \(earlierEvents.count) earlier events")
            }
            
            // Load later events
            if interval.end > cachedEventsInterval.end {
                let laterEvents = try await backend.loadEvents(in: .init(start: cachedEventsInterval.end, end: interval.end),
                                                               updateStatus: updateStatus)
                
                cachedEvents.append(contentsOf: laterEvents)
                
                // Persist the events
                await persistEvents(laterEvents)
                config.log?(.debug, "loaded \(laterEvents.count) later events")
            }
            
            try await self.processEvents(cachedEvents)
        }
        else {
            let events = try await backend.loadEvents(in: interval, updateStatus: updateStatus)
            try await self.processEvents(events)
            
            // Persist the events
            await persistEvents(events)
        }
    }
    
    /// Check if there are new, uninitialized aggregators.
    func checkForNewAggregators() async throws {
        let uninitializedAggregators = Set(state.accumulatedState.aggregators.keys.filter { !state.accumulatedState.knownAggregators.contains($0) })
        guard !uninitializedAggregators.isEmpty else {
            return
        }
        
        config.log?(.debug, "updating new aggregators: [\(uninitializedAggregators.joined(separator: ", "))]")
        
        // No need to fetch the events from the backend again
        var intervals = self.state.historicalStates.keys.map { $0 }
        intervals.append(state.currentState.interval)
        
        // Register the events we saved from before the reset
        guard let allEvents = await self.getProcessedEvents(in: Self.allEncompassingDateInterval) else {
            config.log?(.debug, "updating new aggregators: no events found")
            return
        }
        
        config.log?(.debug, "updating new aggregators: \(allEvents.count) events found")
        try await self.processHistoricalEvents(allEvents, forAggregatorIds: uninitializedAggregators)
    }
    
    /// Create and initialize a non-normal aggregator state.
    func createNonNormalAggregatorState(in interval: DateInterval) async throws -> IntervalAggregatorState {
        let aggregators = Self.instantiateAggregators(eventCategories: eventCategories,
                                                      interval: interval,
                                                      allEventsAggregators: allEventAggregators)
        
        let state = IntervalAggregatorState(interval: interval, aggregators: aggregators)
        guard let events = await self.getProcessedEvents(in: interval) else {
            return state
        }
        
        let now = KeystoneAnalyzer.now
        let previousStatus = self.status
        
        var processedEvents = 0
        let totalEvents = events.count
        
        // Update state for each event
        for event in events {
            assert(event.date < now, "encountered an event from the future")
            
            await updateStatus(.processingEvents(progress: Double(processedEvents) / Double(totalEvents),
                                                 detail: "New Interval"))
            processedEvents += 1
            
            try await state.processEvent(event, aggregatorColumns: self.aggregatorInfo, isNewEvent: true)
        }
        
        self.nonNormalStates[interval] = state
        
        await updateStatus(previousStatus)
        return state
    }
}

fileprivate extension IntervalAggregatorState {
    /// Process an event.
    func processEvent(_ event: KeystoneEvent, aggregatorColumns: [String: AggregatorProcessingInfo], isNewEvent: Bool) async throws {
        // Update aggregators
        for (id, aggregator) in self.aggregators {
            guard let info = aggregatorColumns[id] else {
                continue
            }
            
            for column in info.columns {
                if let column {
                    guard event.category == column.categoryName else {
                        continue
                    }
                }
                
                _ = aggregator.addEvent(event, column: column)
            }
        }
        
        if isNewEvent {
            // Add to the event list
            self.eventCount += 1
            
            // Update event interval
            self.processedEventInterval.expand(toContain: event.date)
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
        state.knownAggregators.insert(contentsOf: state.aggregators.map { $0.key })
        
        // Persist
        let key = _KeystoneAggregatorState.key(for: state.interval)
        await delegate.persist(try state.codableState(), withKey: key)
    }
    
    /// Persist a time interval state.
    func removeState(_ state: IntervalAggregatorState) async {
        let key = _KeystoneAggregatorState.key(for: state.interval)
        await delegate.persist(Optional<_KeystoneAggregatorState>.none, withKey: key)
    }
    
    /// Check if the current state needs to be updated.
    func ensureCurrentStateValidity() async throws {
        let currentInterval = Self.currentEventInterval
        guard state.currentState.interval != currentInterval else {
            return
        }
        
        state.historicalStates[state.currentState.interval] = state.currentState
        state.currentState = .init(interval: currentInterval,
                                   aggregators: Self.instantiateAggregators(eventCategories: eventCategories,
                                                                            interval: currentInterval,
                                                                            allEventsAggregators: allEventAggregators))
        
        try await self.persistState(state.currentState)
    }
    
    /// Fetch or create the state within a given interval.
    static func state(in interval: DateInterval,
                      delegate: KeystoneDelegate,
                      eventCategories: [EventCategory],
                      allEventAggregators: [AggregatorMeta]) async throws
        -> IntervalAggregatorState
    {
        let aggregators = Self.instantiateAggregators(eventCategories: eventCategories,
                                                      interval: interval,
                                                      allEventsAggregators: allEventAggregators)
        
        if let state = await delegate.load(_KeystoneAggregatorState.self, withKey: _KeystoneAggregatorState.key(for: interval)) {
            return try IntervalAggregatorState(from: state, aggregators: aggregators)
        }
        
        let state = IntervalAggregatorState(interval: interval, aggregators: aggregators)
        try await Self.persistState(state, delegate: delegate)
        
        return state
    }
    
    /// Fetch or create the state within a given interval.
    func state(in interval: DateInterval) async throws -> IntervalAggregatorState {
        if interval == Self.currentEventInterval {
            return self.state.currentState
        }
        
        if interval == Self.allEncompassingDateInterval {
            return self.state.accumulatedState
        }
        
        if let cachedState = self.state.historicalStates[interval] {
            return cachedState
        }
        
        let state = try await Self.state(in: interval, delegate: delegate, eventCategories: eventCategories,
                                         allEventAggregators: allEventAggregators)
        
        self.state.historicalStates[interval] = state
        
        return state
    }
    
    /// Instantiate the aggregators for a state.
    static func instantiateAggregators(eventCategories: [EventCategory],
                                       interval: DateInterval,
                                       allEventsAggregators: [AggregatorMeta]) -> [String: any EventAggregator] {
        var aggregators = [String: any EventAggregator]()
        for category in eventCategories {
            for column in category.columns {
                for meta in column.aggregators {
                    guard aggregators[meta.id] == nil else {
                        continue
                    }
                    
                    if let aggregatorInterval = meta.interval, aggregatorInterval != interval {
                        continue
                    }
                    
                    aggregators[meta.id] = meta.instantiate()
                }
            }
        }
        
        for meta in allEventsAggregators {
            guard aggregators[meta.id] == nil else {
                continue
            }
            
            if let aggregatorInterval = meta.interval, aggregatorInterval != interval {
                continue
            }
            
            aggregators[meta.id] = meta.instantiate()
        }
        
        return aggregators
    }
}

#if DEBUG

fileprivate var firstNowDate: Date = .distantPast
fileprivate var previousNowDate: (returned: Date, real: Date)? = nil
fileprivate var fixedNowDate: Date? = nil

extension KeystoneAnalyzer {
    nonisolated static func setNowDate(_ date: Date) {
        fixedNowDate = date
    }
    
    nonisolated static var now: Date {
        guard ProcessInfo.processInfo.environment["XCTestConfigurationFilePath"] != nil else {
            return Date.now
        }
        
        if let fixedNowDate {
            return fixedNowDate
        }
        
        let realNow = Date.now
        guard let (_, real) = previousNowDate else {
            previousNowDate = (returned: firstNowDate, real: realNow)
            return firstNowDate
        }
        
        let difference = realNow.timeIntervalSinceReferenceDate - real.timeIntervalSinceReferenceDate
        let date = firstNowDate.addingTimeInterval(difference)
        
        return date
    }
}

#else

extension KeystoneAnalyzer {
    static var now: Date { Date.now }
}

#endif

// MARK: Events

extension KeystoneAnalyzer {
    private static func formatDate(_ date: Date) -> String {
        let components = Calendar.reference.dateComponents([.day, .month, .year], from: date)
        let format: (Int?, Int) -> String = { "\($0!)".leftPadding(toMinimumLength: $1, withPad: "0") }
        return "\(format(components.year, 4))\(format(components.month, 2))\(format(components.day, 2))"
    }
    
    private static func eventsKey(for interval: DateInterval) -> String {
        "events-\(formatDate(interval.start))-\(formatDate(interval.end))"
    }
    
    /// Persist the given events.
    func persistEvents(_ events: [KeystoneEvent]) async {
        guard !events.isEmpty else {
            return
        }
        
        var processedEventCount = 0
        let totalEventCount = events.count
        
        var currentInterval: DateInterval = Self.interval(containing: events[0].date)
        var currentIntervalEvents = await self.getProcessedEvents(in: currentInterval) ?? []
        
        var currentIntervalEventCount = currentIntervalEvents.count
        var currentIntervalEventIds = Set(currentIntervalEvents.map { $0.id })
        
        for event in events {
            await updateStatus(.persistingEvents(progress: Double(processedEventCount) / Double(totalEventCount)))
            processedEventCount += 1
            
            let eventInterval = Self.interval(containing: event.date)
            if eventInterval != currentInterval {
                // Only persist if there were changes
                if currentIntervalEventCount != currentIntervalEvents.count {
                    await delegate.persist(currentIntervalEvents, withKey: Self.eventsKey(for: currentInterval))
                }
                
                currentInterval = eventInterval
                currentIntervalEvents = await self.getProcessedEvents(in: currentInterval) ?? []
                currentIntervalEventCount = currentIntervalEvents.count
                currentIntervalEventIds = Set(currentIntervalEvents.map { $0.id })
            }
            
            guard currentIntervalEventIds.insert(event.id).inserted else {
                continue
            }
            
            currentIntervalEvents.append(event)
        }
        
        await delegate.persist(currentIntervalEvents, withKey: Self.eventsKey(for: currentInterval))
    }
    
    /// Fetch the events in the given interval from the delegate.
    ///
    /// - Parameter interval: The interval within which to fetch events.
    /// - Returns: The events fetched from the delegate, or `nil` if they were not found.
    public func getProcessedEvents(in interval: DateInterval) async -> [KeystoneEvent]? {
        if Self.isNormalized(interval) && interval != Self.allEncompassingDateInterval {
            return await delegate.load([KeystoneEvent].self, withKey: Self.eventsKey(for: interval))
        }

        let previousStatus = self.status
        await updateStatus(.fetchingEvents(count: 0, source: "cache"))
        
        let interval = DateInterval(start: max(interval.start, self.state.processedEventInterval.start),
                                    end: min(interval.end, self.state.processedEventInterval.end))
        
        var allEvents = [KeystoneEvent]()
        var currentInterval = Self.interval(containing: interval.end)
        var foundAnyEvents = false
        
        while currentInterval.end > interval.start {
            defer { currentInterval = Self.interval(before: currentInterval) }
            
            guard let events = await delegate.load([KeystoneEvent].self, withKey: Self.eventsKey(for: currentInterval)) else {
                continue
            }
            
            foundAnyEvents = true
            allEvents.append(contentsOf: events)
            
            await updateStatus(.fetchingEvents(count: allEvents.count, source: "cache"))
        }
        
        allEvents = allEvents.filter { interval.contains($0.date) }.sorted { $0.date < $1.date }
        await updateStatus(previousStatus)
        
        guard foundAnyEvents else {
            return nil
        }
        
        return allEvents
    }
}

// MARK: Builder

/// Configure and initialize a ``KeystoneAnalyzer``.
///
/// ```swift
/// var builder = KeystoneAnalyzerBuilder(config: config, backend: backend, delegate: delegate)
/// builder.registerCategory(/* ... */)
/// builder.registerAllEventAggregator(/* ... */)
///
/// let analyzer = try await builder.build()
/// ```
public struct KeystoneAnalyzerBuilder {
    /// The configuration object.
    let config: KeystoneConfig
    
    /// The persistence API.
    let backend: KeystoneBackend
    
    /// The delegate object.
    let delegate: KeystoneDelegate
    
    /// The data aggregators for all events.
    var allEventAggregators: [AggregatorMeta]
    
    /// The known event categories.
    var eventCategories: [EventCategory]
    
    /// Create an analyzer builder.
    ///
    /// - Parameters:
    ///   - config: The configuration object.
    ///   - backend: The backend used to fetch events.
    ///   - delegate: The delegate used for state persistence.
    public init(config: KeystoneConfig,
                backend: KeystoneBackend,
                delegate: KeystoneDelegate) {
        self.config = config
        self.backend = backend
        self.delegate = delegate
        self.eventCategories = []
        self.allEventAggregators = []
    }
}

public extension KeystoneAnalyzerBuilder {
    /// Register an event category with this analyzer.
    ///
    /// - Parameters:
    ///   - name: The name of the category.
    ///   - modify: Closure invoked with an ``EventCategoryBuilder`` that can be used to configure the category.
    mutating func registerCategory(name: String, modify: (inout EventCategoryBuilder) -> Void) {
        var builder = EventCategoryBuilder(name: name)
        modify(&builder)
        
        self.eventCategories.append(builder.build())
    }
    
    /// Register an aggregator for all events.
    ///
    /// - Parameters:
    ///   - id: The ID of the aggregator.
    ///   - interval: The interval within which the aggregator operates.
    ///   - instantiateAggregator: Closure to instantiate the aggregator.
    mutating func registerAllEventAggregator(id: String, interval: DateInterval? = nil, instantiateAggregator: @escaping () -> any EventAggregator) {
        self.allEventAggregators.append(.init(id: id, interval: interval, instantiate: instantiateAggregator))
    }
    
    /// Finalize and create the analyzer.
    ///
    /// - Returns: The `KeystoneAnalyzer` as configured by this builder.
    func build() async throws -> KeystoneAnalyzer {
        try await KeystoneAnalyzer(config: config, delegate: delegate, backend: backend,
                                   eventCategories: eventCategories,
                                   allEventAggregators: allEventAggregators)
    }
}

// MARK: Conformances

extension AnalyzerStatus {
    func isSignificantlyDifferentFrom(_ rhs: AnalyzerStatus) -> Bool {
        switch self {
        case .fetchingEvents(let count, let source):
            guard case .fetchingEvents(let count_, let source_) = rhs else { return true }
            guard source == source_ else { return true }
            // > 1% difference
            guard abs(1 - (Double(count) / Double(count_))) >= 0.01 else { return false }
        case .decodingEvents(let progress, let source):
            guard case .decodingEvents(let progress_, let source_) = rhs else { return true }
            guard source == source_ else { return true }
            guard abs(progress - progress_) >= 0.01 else { return false }
        case .processingEvents(let progress, let detail):
            guard case .processingEvents(let progress_, let detail_) = rhs else { return true }
            guard detail == detail_ else { return true }
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
        case .fetchingEvents(let count, let source):
            guard case .fetchingEvents(let count_, let source_) = rhs else { return false }
            guard count == count_ else { return false }
            guard source == source_ else { return false }
        case .decodingEvents(let progress, let source):
            guard case .decodingEvents(let progress_, let source_) = rhs else { return false }
            guard progress == progress_ else { return false }
            guard source == source_ else { return false }
        case .processingEvents(let progress, let detail):
            guard case .processingEvents(let progress_, let detail_) = rhs else { return false }
            guard detail == detail_ else { return false }
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
        case .fetchingEvents(let count, let source):
            hasher.combine(count)
            hasher.combine(source)
        case .decodingEvents(let progress, let source):
            hasher.combine(progress)
            hasher.combine(source)
        case .processingEvents(let progress, let detail):
            hasher.combine(progress)
            hasher.combine(detail)
        default: break
        }
    }
}
