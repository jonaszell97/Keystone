
import Foundation

/// Allows efficient searching and filtering of events.
///
/// Search indices are created and updated by ``KeystoneAnalyzer``, and persisted by ``KeystoneDelegate``.
/// With a search index you can perform fast keyword searches in a list of events.
public struct KeystoneSearchIndex {
    /// The date interval this search index is valid for.
    let interval: DateInterval
    
    /// Map from keywords to sets of event IDs that contain the keyword.
    let keywordMap: [String: Set<UUID>]
    
    /// The search predicate.
    let predicate: (String, KeystoneEvent) -> Bool
    
    /// Create an empty search index.
    init(interval: DateInterval, keywordMap: [String: Set<UUID>]) {
        self.interval = interval
        self.keywordMap = keywordMap
        self.predicate = Self.createSearchPredicate(keywordMap: keywordMap)
    }
}

extension KeystoneSearchIndex {
    /// Create a predicate function that can be used to filter an event list based on this search index.
    static func createSearchPredicate(keywordMap: [String: Set<UUID>]) -> (String, KeystoneEvent) -> Bool {
        return { searchTerm, event in
            var words = [String]()
            searchTerm.enumerateSubstrings(in: searchTerm.startIndex..., options: .byWords) { word, _, _, _ in
                guard let word else { return }
                words.append(word.lowercased())
            }
            
            for word in words {
                var foundMatch = false
                for (keyword, ids) in keywordMap {
                    guard keyword.hasPrefix(word) else { continue }
                    guard ids.contains(event.id) else { continue }
                    
                    foundMatch = true
                    break
                }
                
                guard foundMatch else {
                    return false
                }
            }
            
            return true
        }
    }
}

public extension KeystoneEventList {
    /// A predicate function that can be used to filter an event list based on this search index.
    ///
    /// - Note: If ``KeystoneConfig/createSearchIndex`` is `false`, the predicate always returns true.
    /// - Returns: A predicate function that can be used to filter an event list based on this search index.
    var searchPredicate: (String, KeystoneEvent) -> Bool {
        guard let searchIndex else {
            return { _, _ in true }
        }
        
        return searchIndex.predicate
    }
}

extension KeystoneAnalyzer {
    /// Persistence key for the search index.
    static let searchIndexKey: String = "keystone-search-index"
    
    /// Update the search index for new events.
    func createSearchIndex(for events: [KeystoneEvent],
                           keywordMap: [String: Set<UUID>] = [:],
                           interval: DateInterval? = nil) async -> KeystoneSearchIndex {
        assert(!events.isEmpty, "KeystoneAnalyzer.createSearchIndex should not be called with an empty event list")
        
        let previousStatus = self.status
        await updateStatus(.updatingSearchIndex(progress: 0))
        
        let totalEventCount = Double(events.count)
        
        var keywordMap = keywordMap
        var keywords = Set<String>()
        
        for (index, event) in events.enumerated() {
            let progress = Double(index) / totalEventCount
            await updateStatus(.updatingSearchIndex(progress: progress))
            
            // Find raw keywords
            if let getSearchKeywords = config.getSearchKeywords {
                getSearchKeywords(event, &keywords)
            }
            else {
                for (_, data) in event.data {
                    guard case .text(let keyword) = data else {
                        continue
                    }
                    
                    keywords.insert(keyword)
                }
            }
            
            // Process keywords
            for keyword in keywords {
                keyword.lowercased().enumerateSubstrings(in: keyword.startIndex..., options: .byWords) { word, _, _, _ in
                    guard let word else { return }
                    keywordMap.modify(key: word, defaultValue: []) { $0.insert(event.id) }
                }
            }
            
            keywords.removeAll(keepingCapacity: true)
        }
        
        await updateStatus(previousStatus)
        
        let interval = interval ?? DateInterval(start: events.first!.date, end: events.last!.date)
        return KeystoneSearchIndex(interval: interval, keywordMap: keywordMap)
    }
    
    /// Update an existing search index.
    func updateSearchIndex(searchIndex: KeystoneSearchIndex, events: [KeystoneEvent]) async -> KeystoneSearchIndex {
        let interval = DateInterval(start: min(events.first!.date, searchIndex.interval.start),
                                    end: max(events.last!.date, searchIndex.interval.end))
        
        let events = events.filter { !searchIndex.interval.contains($0.date) }
        guard !events.isEmpty else {
            return searchIndex
        }
        
        return await self.createSearchIndex(for: events, keywordMap: searchIndex.keywordMap, interval: interval)
    }
}

// MARK: Conformances

extension KeystoneSearchIndex: Codable {
    enum CodingKeys: String, CodingKey {
        case interval, keywordMap
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(interval, forKey: .interval)
        try container.encode(keywordMap, forKey: .keywordMap)
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.init(
            interval: try container.decode(DateInterval.self, forKey: .interval),
            keywordMap: try container.decode(Dictionary<String, Set<UUID>>.self, forKey: .keywordMap)
        )
    }
}

extension KeystoneSearchIndex: Equatable {
    public static func ==(lhs: KeystoneSearchIndex, rhs: KeystoneSearchIndex) -> Bool {
        return (
            lhs.interval == rhs.interval &&
            lhs.keywordMap == rhs.keywordMap
        )
    }
}

extension KeystoneSearchIndex: Hashable {
    public func hash(into hasher: inout Hasher) {
        hasher.combine(interval)
        hasher.combine(keywordMap)
    }
}
