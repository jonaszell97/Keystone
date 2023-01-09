
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
    
    /// The columns for events of this category.
    private var columns: [EventColumn]
    
    /// Memberwise initializer.
    internal init(name: String) {
        self.name = name
        self.columns = []
    }
}

public extension EventCategoryBuilder {
    /// Register an event column.
    @discardableResult mutating func registerColumn(name columnName: String, aggregators: @autoclosure @escaping () -> [any EventAggregator])
        -> EventCategoryBuilder
    {
        let column = EventColumn(name: columnName, categoryName: self.name, instantiateAggregators: aggregators)
        self.columns.append(column)
        
        return self
    }
    
    /// Build the category.
    func build() -> EventCategory {
        EventCategory(name: name, columns: columns)
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
    internal let instantiateAggregators: () -> [any EventAggregator]
    
    /// Memberwise initializer.
    internal init(name: String, categoryName: String, instantiateAggregators: @escaping () -> [any EventAggregator]) {
        self.name = name
        self.categoryName = categoryName
        self.instantiateAggregators = instantiateAggregators
    }
}
