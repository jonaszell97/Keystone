
import Foundation
import os

public struct KeystoneConfig {
    /// The analytics identifier for the current user.
    public let userIdentifier: String
    
    /// The amount of time that all analytics events should be kept in cache.
    public let keepAllEventsForTimeInterval: DateComponents
    
    /// Whether or not aggregator state should be persisted.
    public let persistAggregatorState: Bool
    
    /// Function for logging events.
    public var log: Optional<(OSLogType, String) -> Void> = nil
    
    /// Memberwise initializer.
    public init(userIdentifier: String,
                keepAllEventsForTimeInterval: DateComponents = .init(month: 3),
                persistAggregatorState: Bool = true) {
        self.userIdentifier = userIdentifier
        self.keepAllEventsForTimeInterval = keepAllEventsForTimeInterval
        self.persistAggregatorState = persistAggregatorState
    }
}

public struct AggregatorMeta {
    /// The aggregator ID.
    let id: String
    
    /// The single interval for which this aggregator receives events, if applicable.
    let interval: DateInterval?
    
    /// Function to instantiate this aggregator.
    let instantiate: () -> any EventAggregator
    
    /// Public initializer.
    public init(id: String, interval: DateInterval? = nil, instantiate: @escaping () -> any EventAggregator) {
        self.id = id
        self.interval = interval
        self.instantiate = instantiate
    }
}

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
    mutating func registerAggregator(id: String, interval: DateInterval? = nil, instantiateAggregator: @escaping () -> any EventAggregator) {
        self.aggregators.append(.init(id: id, interval: interval, instantiate: instantiateAggregator))
    }
    
    /// Build the category.
    func build() -> EventCategory {
        var columns = columns
        columns.append(.init(name: "id", categoryName: name, aggregators: aggregators))
        
        return EventCategory(name: name, columns: columns)
    }
}


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
    public mutating func registerAggregator(id: String, interval: DateInterval? = nil, instantiateAggregator: @escaping () -> any EventAggregator) {
        self.aggregators.append(.init(id: id, interval: interval, instantiate: instantiateAggregator))
    }
    
    /// Build the column.
    func build() -> EventColumn {
        .init(name: name, categoryName: categoryName, aggregators: aggregators)
    }
}

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
