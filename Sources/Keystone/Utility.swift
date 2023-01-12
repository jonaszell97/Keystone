
import Foundation

// MARK: Calendar

internal extension Calendar {
    /// Shortcut for the gregorian calendar with the UTC time zone.
    static let reference: Calendar = {
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = .utc
        
        return calendar
    }()
}

internal extension TimeZone {
    /// Shortcut for the UTC time zone.
    static let utc = TimeZone(identifier: "UTC")!
}


extension Date {
    fileprivate static let secondsPerDay: TimeInterval = 24 * 60 * 60
    
    /// - returns: A date representing the start of the day this date is in.
    var startOfDay: Date {
        let components = Calendar.reference.dateComponents([.day, .month, .year], from: self)
        return Calendar.reference.date(from: components)!
    }
    
    /// - returns: A date representing the end of the day this date is in.
    var endOfDay: Date {
        startOfDay.addingTimeInterval(23*60*60 + 59*60 + 59)
    }
    
    /// - returns: A date representing the start of the month this date is in.
    var startOfMonth: Date {
        let components = Calendar.reference.dateComponents([.year, .month], from: self)
        return Calendar.reference.date(from: components)!
    }
    
    /// - returns: A date representing the end of the month this date is in.
    var endOfMonth: Date {
        Calendar.reference.date(byAdding: DateComponents(month: 1, second: -1), to: self.startOfMonth)!
    }
    
    public enum FirstDayOfWeek {
        case sunday, monday
    }
    
    /// - returns: A date representing the start of the week this date is in.
    func startOfWeek(weekStartsOn firstWeekday: FirstDayOfWeek) -> Date {
        let components = Calendar.reference.dateComponents([.year, .month, .day, .weekday], from: self)
        let desiredWeekday: Int
        
        switch firstWeekday {
        case .sunday:
            desiredWeekday = 1
        case .monday:
            desiredWeekday = 2
        }
        
        let weekday = components.weekday!
        let difference: Int
        
        if desiredWeekday > weekday {
            difference = -7 + (desiredWeekday - weekday)
        }
        else {
            difference = desiredWeekday - weekday
        }
        
        return Calendar.reference.date(from: components)!.addingTimeInterval(24*60*60*TimeInterval(difference))
    }
    
    /// - returns: A date representing the end of the week this date is in.
    func endOfWeek(weekStartsOn firstWeekday: FirstDayOfWeek) -> Date {
        startOfWeek(weekStartsOn: firstWeekday).addingTimeInterval(7*24*60*60 - 1)
    }
}

internal extension DateInterval {
    /// - returns: This date interval expanded to contain the given date.
    func expanding(toContain date: Date) -> DateInterval {
        var copy = self
        copy.expand(toContain: date)
        
        return copy
    }
    
    /// Expand this interval to contain the given date.
    mutating func expand(toContain date: Date) {
        if date.timeIntervalSinceReferenceDate < self.start.timeIntervalSinceReferenceDate {
            self.start = date
        }
        else if date.timeIntervalSinceReferenceDate > self.end.timeIntervalSinceReferenceDate {
            self.end = date
        }
    }
}

// MARK: Dictionary

internal extension Dictionary {
    /// Modify the value at the given key, or place a default value and modify that.
    mutating func modify(key: Key, defaultValue: @autoclosure () -> Value, modify: (inout Value) -> Void) {
        if var value = self[key] {
            modify(&value)
            self[key] = value
        }
        else {
            var value = defaultValue()
            modify(&value)
            
            self[key] = value
        }
    }
}

// MARK: String

extension String {
    /// - returns: A version of this string with the length `length`. If this string is longer than `length`, nothing is changed.
    ///            If it is shorter, adds the padding character to the left.
    func leftPadding(toMinimumLength length: Int, withPad character: Character) -> String {
        let stringLength = self.count
        guard stringLength < length else {
            return self
        }
        
        return String(repeatElement(character, count: length - stringLength)) + self
    }
}

// MARK: Set

extension Set {
    /// Insert all elements of the given collection into this set.
    /// - returns: The number of newly inserted items.
    @discardableResult mutating func insert<S: Sequence>(contentsOf sequence: S) -> Int
        where S.Element == Element
    {
        var newValues = 0
        for element in sequence {
            if self.insert(element).inserted {
                newValues += 1
            }
        }
        
        return newValues
    }
}
