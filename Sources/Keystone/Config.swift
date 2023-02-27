
import Foundation
import os

/// Configuration for ``KeystoneAnalyzer``, ``KeystoneClient`` and ``KeystoneBackend``.
public struct KeystoneConfig {
    /// The unique identifier for the current user. New events are submitted with this identifier by default.
    public let userIdentifier: String
    
    /// Custom logging function.
    public var log: Optional<(OSLogType, String) -> Void> = nil
    
    /// Whether or not a search index for events should be created.
    public var createSearchIndex: Bool
    
    /// Function used to extract search keywords from an event.
    public var getSearchKeywords: Optional<(KeystoneEvent, inout Set<String>) -> Void> = nil
    
    /// Create a configuration instance.
    ///
    /// - Parameters:
    ///   - userIdentifier: The unique identifier for the current user.
    ///   - log: Custom logging function. Set this parameter if you want to be informed of internal `Keystone` messages.
    public init(userIdentifier: String,
                createSearchIndex: Bool = false,
                log: Optional<(OSLogType, String) -> Void> = nil) {
        self.userIdentifier = userIdentifier
        self.createSearchIndex = createSearchIndex
        self.log = log
    }
}

/// Information about an aggregator that can be instantiated in multiple time intervals.
public struct AggregatorMeta {
    /// The aggregator ID.
    let id: String
    
    /// The single interval for which this aggregator receives events, if applicable.
    let interval: DateInterval?
    
    /// Function to instantiate this aggregator.
    let instantiate: () -> any EventAggregator
    
    /// Create a new aggregator info structure.
    public init(id: String, interval: DateInterval? = nil, instantiate: @escaping () -> any EventAggregator) {
        self.id = id
        self.interval = interval
        self.instantiate = instantiate
    }
}

/// Register event categories with a ``KeystoneAnalyzer`` using this builder type.
///
/// You should not create instances of this type directly. Instead, use ``KeystoneAnalyzerBuilder/registerCategory(name:modify:)``.
public struct EventCategoryBuilder {
    /// The name of this category.
    public let name: String
    
    /// The data aggregators for this column.
    internal var aggregators: [AggregatorMeta]
    
    /// The columns for events of this category.
    private var columns: [EventColumn]
    
    /// Memberwise initializer.
    internal init(name: String) {
        self.name = name
        self.aggregators = []
        self.columns = []
    }
}

public extension EventCategoryBuilder {
    /// Register an event column.
    ///
    /// - Parameters:
    ///   - columnName: The name of the column.
    ///   - modify: Callback invoked with an ``EventColumnBuilder`` that can be used to configure the column.
    mutating func registerColumn(name columnName: String, modify: (inout EventColumnBuilder) -> Void) {
        guard columnName != "id" else {
            fatalError("column name id is reserved")
        }
        
        var builder = EventColumnBuilder(name: columnName, categoryName: self.name)
        modify(&builder)
        
        let column = builder.build()
        self.columns.append(column)
    }
    
    /// Register an event column.
    ///
    /// - Parameters:
    ///   - columnName: The name of the column.
    ///   - aggregators: The aggregators installed on this column.
    mutating func registerColumn(name columnName: String, aggregators: [String: () -> any EventAggregator] = [:]) {
        guard columnName != "id" else {
            fatalError("column name id is reserved")
        }
        
        self.registerColumn(name: columnName) {
            for (id, instantiate) in aggregators {
                $0.registerAggregator(id: id, instantiateAggregator: instantiate)
            }
        }
    }
    
    /// Register an aggregator for the entire category.
    ///
    /// - Parameters:
    ///   - id: The ID of the aggregator.
    ///   - interval: The interval within which this aggregator is valid.
    ///   - instantiateAggregator: Closure to create an instance of this aggregator.
    mutating func registerAggregator(id: String, interval: DateInterval? = nil, instantiateAggregator: @escaping () -> any EventAggregator) {
        self.aggregators.append(.init(id: id, interval: interval, instantiate: instantiateAggregator))
    }
    
    /// Finalize and create the category.
    ///
    /// - Returns: The event category instance as configured by this builder.
    func build() -> EventCategory {
        var columns = columns
        columns.append(.init(name: "id", categoryName: name, aggregators: aggregators))
        
        return EventCategory(name: name, columns: columns)
    }
}

/// Represents a category of events with its associated columns.
public struct EventCategory {
    /// The name of this category.
    public let name: String
    
    /// The columns for events of this category.
    public let columns: [EventColumn]
    
    /// Default initializer.
    internal init(name: String, columns: [EventColumn]) {
        self.name = name
        self.columns = columns
    }
}

/// Create event columns for an ``EventCategory`` instance using this builder type.
///
/// You should not create instances of this type directly. Instead, use ``EventCategoryBuilder/registerColumn(name:modify:)``.
public struct EventColumnBuilder {
    /// The name of this column.
    internal let name: String
    
    /// The name of the category this column belongs to.
    internal let categoryName: String
    
    /// The data aggregators for this column.
    internal var aggregators: [AggregatorMeta]
    
    /// Memberwise initializer.
    internal init(name: String, categoryName: String) {
        self.name = name
        self.categoryName = categoryName
        self.aggregators = []
    }
}

extension EventColumnBuilder {
    /// Register an aggregator for this column.
    ///
    /// - Parameters:
    ///   - id: The ID of the aggregator.
    ///   - interval: The interval within which this aggregator is valid.
    ///   - instantiateAggregator: Closure to create an instance of this aggregator.
    public mutating func registerAggregator(id: String, interval: DateInterval? = nil, instantiateAggregator: @escaping () -> any EventAggregator) {
        self.aggregators.append(.init(id: id, interval: interval, instantiate: instantiateAggregator))
    }
    
    /// Finalize and create the column.
    ///
    /// - Returns: The event column instance as configured by this builder.
    func build() -> EventColumn {
        .init(name: name, categoryName: categoryName, aggregators: aggregators)
    }
}

/// Represents a single column of an ``EventCategory``.
public struct EventColumn {
    /// The name of this column.
    public let name: String
    
    /// The name of the category this column belongs to.
    public let categoryName: String
    
    /// The data aggregators for this column.
    internal let aggregators: [AggregatorMeta]
    
    /// Memberwise initializer.
    internal init(name: String, categoryName: String, aggregators: [AggregatorMeta]) {
        self.name = name
        self.categoryName = categoryName
        self.aggregators = aggregators
    }
}
