# ``Keystone``

Keystone is an event processing library for Swift apps that handles event submission, persistence, as well as data aggregation and analysis. 

## Overview

You create events using a `KeystoneClient`, which are persisted using a `KeystoneBackend` that you specify. If your App uses CloudKit, Keystone provides `CloudKitBackend`, which persists events in a CloudKit container.

```swift
// Create & persist an event from the local user
client.submitEvent(
    category: "sessionStartEvent",
    data: ["sessionWasStartedByPushNotification": .boolean(false)])
```

Each `KeystoneEvent` has a unique ID, category, creation date, user ID, as well as event-specific data represented by a dictionary mapping `String` keys to `KeystoneEventData` values. `KeystoneEventData` is an enumeration type that supports strings, numbers, booleans, dates, and custom codable data. 

Each event belongs to a `KeystoneEventCategory`, which defines the aggregators, columns, and data types of the event's data. You configure event categories with a `KeystoneAnalyzerBuilder`, the `KeystoneClient` itself does not know about your event categories.

```swift
// Register an aggregator that counts the number of sessions
builder.registerCategory("sessionStartEvent") { category in
    category.registerAggregator("Sessions") { CountingAggregator() }
}
```

The `KeystoneAnalyzer` class handles event processing, aggregation, and analysis. The analyzer fetches events from the provided `KeystoneBackend` and feeds them into the `KeystoneAggregator` instances that you configured during setup of the analyzer. Using `KeystoneAnalyzer`, you can query the state of your aggregators in specific time intervals, such as the current month, week, year, or all time.

```swift
// Find the aggregator that counts sessions for the current year
let sessions = analyzer.findAggregator(
    withId: "Sessions", 
    in: KeystoneAnalyzer.yearInterval(containing: .now))

print(sessions.valueCount)
```

To speed up the data processing, `KeystoneAnalyzer` will ask the provided `KeystoneDelegate` instance to persist its internal state. If you do not provide a delegate that supports persistence, all events will have to be refetched and -processsed whenever your App restarts.

## Topics

### Events

- ``KeystoneEvent``
- ``KeystoneEventData``

### Configuration

- ``KeystoneConfig``
- ``EventCategory``
- ``EventCategoryBuilder``
- ``EventColumn``
- ``EventColumnBuilder``
- ``AggregatorMeta``

### Event submission

- ``KeystoneClient``

### Event persistence

- ``KeystoneBackend``
- ``BackendStatus``
- ``CloudKitBackend``
- ``MockBackend``

### Event analysis

- ``KeystoneAnalyzer``
- ``KeystoneAnalyzerBuilder``
- ``AnalyzerStatus``
- ``KeystoneDelegate``

### Event aggregators

- ``EventAggregator``
- ``EventProcessingResult``
- ``ChainingAggregator``
- ``NumericStatsAggregator``
- ``CountingAggregator``
- ``LatestEventAggregator``
- ``FilteringAggregator``
- ``MetaFilteringAggregator``
- ``MappingAggregator``
- ``GroupingAggregator``
- ``CountingByGroupAggregator``
- ``DateAggregatorScope``
- ``DateAggregator``
- ``CountingByDateAggregator``
- ``DuplicateEventChecker``
- ``PredicateAggregator(predicate:)``
