
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

public struct EventCategoryBuilder {
    /// The name of this category.
    public let name: String
    
    /// The data aggregators for this column.
    internal var aggregators: [String: () -> any EventAggregator]
    
    /// The columns for events of this category.
    private var columns: [EventColumn]
    
    /// Memberwise initializer.
    internal init(name: String) {
        self.name = name
        self.aggregators = [:]
        self.columns = []
    }
}

public extension EventCategoryBuilder {
    /// Register an event column.
    mutating func registerColumn(name columnName: String, aggregators: [String: () -> any EventAggregator] = [:])
    {
        guard columnName != "id" else {
            fatalError("column name id is reserved")
        }
        
        let column = EventColumn(name: columnName, categoryName: self.name, aggregators: aggregators)
        self.columns.append(column)
    }
    
    /// Register an aggregator for the entire category.
    mutating func registerAggregator(id: String, instantiateAggregator: @escaping () -> any EventAggregator) {
        self.aggregators[id] = instantiateAggregator
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

public struct EventColumn {
    /// The name of this column.
    public let name: String
    
    /// The name of the category this column belongs to.
    public let categoryName: String
    
    /// The data aggregators for this column.
    internal let aggregators: [String: () -> any EventAggregator]
    
    /// Memberwise initializer.
    internal init(name: String, categoryName: String, aggregators: [String: () -> any EventAggregator]) {
        self.name = name
        self.categoryName = categoryName
        self.aggregators = aggregators
    }
}
