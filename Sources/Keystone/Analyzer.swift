
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
    
    /// The data aggregators for all events.
    let allEventAggregators: [String: () -> any EventAggregator]
    
    /// The known event categories.
    public let eventCategories: [EventCategory]
    
    /// Map form aggregator IDs to the columns that contain the respective aggregator.
    let aggregatorColumns: [String: [EventColumn?]]
    
    /// States with non-normal intervals that have been queried.
    var nonNormalStates: [DateInterval: IntervalAggregatorState]
    
    /// Whether the state is ready to submit events.
    public var isReady: Bool {
        guard case .ready = status else {
            return false
        }
        
        return true
    }
    
    /// Initialize the analytics state.
    internal init(config: KeystoneConfig, delegate: KeystoneDelegate, backend: KeystoneBackend,
                  eventCategories: [EventCategory], allEventAggregators: [String: () -> any EventAggregator]) async throws {
        self.status = .initializingState
        self.config = config
        self.delegate = delegate
        self.backend = backend
        self.eventCategories = eventCategories
        self.allEventAggregators = allEventAggregators
        self.nonNormalStates = [:]
        
        var aggregatorColumns: [String: [EventColumn?]] = [:]
        for category in eventCategories {
            for column in category.columns {
                for (id, _) in column.aggregators {
                    aggregatorColumns.modify(key: id, defaultValue: []) { $0.append(column) }
                }
            }
        }
        
        for (id, _) in allEventAggregators {
            aggregatorColumns.modify(key: id, defaultValue: []) { $0.append(nil) }
        }
        
        self.aggregatorColumns = aggregatorColumns
        
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
    
    /// Find all aggregators belonging to a column in the given interval.
    public func findAggregators(for category: String, in interval: DateInterval) async throws -> [String: EventAggregator] {
        guard Self.isNormalized(interval) else {
            return try await self.findAggregators(for: category, inNonNormalInterval: interval)
        }
        
        let state = try await self.state(in: interval)
        return state.aggregators.filter { self.aggregatorColumns[$0.key]?.contains { $0?.categoryName == category } ?? false }
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
        
        return state.aggregators.filter { self.aggregatorColumns[$0.key]?.contains { $0?.categoryName == category } ?? false }
    }
    
    /// Load new events.
    public func loadNewEvents() async throws {
        try await self.loadAndProcessNewEvents()
    }
}

// MARK: Date intervals

public extension KeystoneAnalyzer {
    // MARK: Monthly (normalized)
    
    /// Get the date interval covering all time.
    static let allEncompassingDateInterval: DateInterval = {
        DateInterval(start: Date(timeIntervalSinceReferenceDate: 0), duration: 300 * 365 * 24 * 60 * 60)
    }()
    
    /// Get the current date interval.
    static var currentEventInterval: DateInterval {
        let now = KeystoneAnalyzer.now
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
    
    /// Get the current date interval.
    static func isNormalized(_ interval: DateInterval) -> Bool {
        interval == Self.interval(containing: interval.start) || interval == allEncompassingDateInterval
    }
    
    // MARK: Weekly
    
    /// Get the current date interval.
    static func weekInterval(before: DateInterval, weekStartsOn firstWeekday: Date.FirstDayOfWeek) -> DateInterval {
        let previous = before.start.addingTimeInterval(-24*60*60)
        return .init(start: previous.startOfWeek(weekStartsOn: firstWeekday), end: previous.endOfWeek(weekStartsOn: firstWeekday))
    }
    
    /// Get the current date interval.
    static func weekInterval(after: DateInterval, weekStartsOn firstWeekday: Date.FirstDayOfWeek) -> DateInterval {
        let next = after.end.addingTimeInterval(24*60*60)
        return .init(start: next.startOfWeek(weekStartsOn: firstWeekday), end: next.endOfWeek(weekStartsOn: firstWeekday))
    }
    
    /// Get the current date interval.
    static func weekInterval(containing date: Date, weekStartsOn firstWeekday: Date.FirstDayOfWeek) -> DateInterval {
        .init(start: date.startOfWeek(weekStartsOn: firstWeekday), end: date.endOfWeek(weekStartsOn: firstWeekday))
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
            
            try await state.processEvent(event, aggregatorColumns: self.aggregatorColumns, isNewEvent: true)
            try await self.state.accumulatedState.processEvent(event, aggregatorColumns: self.aggregatorColumns,
                                                               isNewEvent: true)
            
            modifiedStates.insert(state)
            
            // Update non-normalized intervals
            for (interval, state) in nonNormalStates {
                guard interval.contains(event.date) else {
                    continue
                }
                
                try await state.processEvent(event, aggregatorColumns: self.aggregatorColumns, isNewEvent: true)
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
        let aggregatorColumns = self.aggregatorColumns.filter { ids.contains($0.key) }
        
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
                    await self.updateStatus(.fetchingEvents(count: count))
                }
            case .processingRecords(let progress):
                Task {
                    await self.updateStatus(.decodingEvents(progress: progress))
                }
            }
        }
        
        // Use as many events from cache as possible
        if var cachedEvents = await self.loadEvents(in: interval) {
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
        var allEvents = [KeystoneEvent]()
        for interval in intervals {
            async let events = await self.loadEvents(in: interval)
            guard let events = await events else {
                continue
            }
            
            allEvents.append(contentsOf: events)
        }
        
        allEvents.sort { $0.date < $1.date }
        
        try await self.processHistoricalEvents(allEvents, forAggregatorIds: uninitializedAggregators)
    }
    
    /// Create and initialize a non-normal aggregator state.
    func createNonNormalAggregatorState(in interval: DateInterval) async throws -> IntervalAggregatorState {
        let aggregators = Self.instantiateAggregators(eventCategories: eventCategories, allEventsAggregators: allEventAggregators)
        let state = IntervalAggregatorState(interval: interval, aggregators: aggregators)
        
        guard let events = await self.loadEvents(in: interval) else {
            return state
        }
        
        let now = KeystoneAnalyzer.now
        let previousStatus = self.status
        
        var processedEvents = 0
        let totalEvents = events.count
        
        // Update state for each event
        for event in events {
            assert(event.date < now, "encountered an event from the future")
            
            await updateStatus(.processingEvents(progress: Double(processedEvents) / Double(totalEvents)))
            processedEvents += 1
            
            try await state.processEvent(event, aggregatorColumns: self.aggregatorColumns, isNewEvent: true)
        }
        
        self.nonNormalStates[interval] = state
        
        await updateStatus(previousStatus)
        return state
    }
}

fileprivate extension IntervalAggregatorState {
    /// Process an event.
    func processEvent(_ event: KeystoneEvent, aggregatorColumns: [String: [EventColumn?]], isNewEvent: Bool) async throws {
        // Update aggregators
        for (id, aggregator) in self.aggregators {
            guard let columns = aggregatorColumns[id] else {
                continue
            }
            
            for column in columns {
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
        let key = KeystoneAggregatorState.key(for: state.interval)
        await delegate.persist(try state.codableState(), withKey: key)
    }
    
    /// Persist a time interval state.
    func removeState(_ state: IntervalAggregatorState) async {
        let key = KeystoneAggregatorState.key(for: state.interval)
        await delegate.persist(Optional<KeystoneAggregatorState>.none, withKey: key)
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
                                                                            allEventsAggregators: allEventAggregators))
        
        try await self.persistState(state.currentState)
    }
    
    /// Fetch or create the state within a given interval.
    static func state(in interval: DateInterval, delegate: KeystoneDelegate,
                      eventCategories: [EventCategory], allEventAggregators: [String: () -> any EventAggregator]) async throws
        -> IntervalAggregatorState
    {
        let aggregators = Self.instantiateAggregators(eventCategories: eventCategories, allEventsAggregators: allEventAggregators)
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
        
        let state = try await Self.state(in: interval, delegate: delegate, eventCategories: eventCategories,
                                         allEventAggregators: allEventAggregators)
        
        self.state.historicalStates[interval] = state
        
        return state
    }
    
    /// Instantiate the aggregators for a state.
    static func instantiateAggregators(eventCategories: [EventCategory], allEventsAggregators: [String: () -> any EventAggregator]) -> [String: any EventAggregator] {
        var aggregators = [String: any EventAggregator]()
        for category in eventCategories {
            for column in category.columns {
                for (id, instantiateAggregator) in column.aggregators {
                    guard aggregators[id] == nil else {
                        continue
                    }
                    
                    aggregators[id] = instantiateAggregator()
                }
            }
        }
        
        for (id, instantiateAggregator) in allEventsAggregators {
            guard aggregators[id] == nil else {
                continue
            }
            
            aggregators[id] = instantiateAggregator()
        }
        
        return aggregators
    }
}

#if DEBUG

extension KeystoneAnalyzer {
    static var firstNowDate: Date = .distantPast
    static var previousNowDate: (returned: Date, real: Date)? = nil
    static var fixedNowDate: Date? = nil
    
    static var now: Date {
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
        if Self.isNormalized(interval) {
            return await delegate.load([KeystoneEvent].self, withKey: Self.eventsKey(for: interval))
        }
        
        return await self.loadEventsFromCache(in: interval)
    }
    
    /// Load events in the given interval from cache.
    private func loadEventsFromCache(in interval: DateInterval) async -> [KeystoneEvent]? {
        let previousStatus = self.status
        
        await updateStatus(.fetchingEvents(count: 0))
        
        var allEvents = [KeystoneEvent]()
        var currentInterval = Self.interval(containing: interval.end)
        
        while currentInterval.end > interval.start {
            defer {
                currentInterval = Self.interval(before: currentInterval)
            }
            
            guard let events = await self.loadEvents(in: currentInterval) else {
                break
            }
            guard !events.isEmpty else {
                continue
            }
            
            allEvents.append(contentsOf: events)
            await updateStatus(.fetchingEvents(count: allEvents.count))
        }
        
        allEvents = allEvents.filter { interval.contains($0.date) }.sorted { $0.date < $1.date }
        await updateStatus(previousStatus)
        
        guard !allEvents.isEmpty else {
            return nil
        }
        
        return allEvents
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
    
    /// The data aggregators for all events.
    var allEventAggregators: [String: () -> any EventAggregator]
    
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
        self.allEventAggregators = [:]
    }
}

public extension KeystoneAnalyzerBuilder {
    /// Register an event category.
    mutating func registerCategory(name: String, modify: (inout EventCategoryBuilder) -> Void) {
        var builder = EventCategoryBuilder(name: name)
        modify(&builder)
        
        self.eventCategories.append(builder.build())
    }
    
    /// Register an aggregator for all events.
    mutating func registerAllEventAggregator(id: String, instantiateAggregator: @escaping () -> any EventAggregator) {
        self.allEventAggregators[id] = instantiateAggregator
    }
    
    /// Build the analyzer.
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




