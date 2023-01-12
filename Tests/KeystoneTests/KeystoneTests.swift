import Foundation
@testable import Keystone
import XCTest

@MainActor final class KeystoneTests: XCTestCase {
    static let dateFormatter: DateFormatter = {
        let dateFormatter = DateFormatter()
        dateFormatter.locale = Locale(identifier: "en_US_POSIX")
        dateFormatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ssZ"
        dateFormatter.timeZone = .utc
        
        return dateFormatter
    }()
    
    static func date(from string: String) -> Date {
        Self.dateFormatter.date(from: string)!
    }
    
    static func setupAnalyzer(builder: inout KeystoneAnalyzerBuilder,
                              eventCount: Int, eventInterval: DateInterval, userCount: Int,
                              using rng: inout ARC4RandomNumberGenerator) -> [KeystoneEvent] {
        let eventCategories: [String: [String: KeystoneEventData.CodingKeys]] = [
            "numericEvent": ["numericValueA": .number, "numericValueB": .number, "textValue": .text],
            "textEvent": ["textValueA": .text, "textValueB": .text],
        ]
        
        builder.registerAllEventAggregator(id: "All Event Count") { CountingAggregator() }
        builder.registerAllEventAggregator(id: "All Event Count By Month") { CountingByDateAggregator(components: [.month]) }
        
        builder.registerCategory(name: "numericEvent") { category in
            category.registerAggregator(id: "numericEvent Count") { CountingAggregator() }
            category.registerColumn(name: "numericValueA", aggregators: ["numericValueA Stats": { NumericStatsAggregator() }])
            category.registerColumn(name: "numericValueB", aggregators: ["numericValueB Stats": { NumericStatsAggregator() }])
        }
        
        builder.registerCategory(name: "textEvent") { category in
            category.registerAggregator(id: "textEvent Count") { CountingAggregator() }
            category.registerColumn(name: "textValueA", aggregators: ["textValueA Count By Group": { CountingByGroupAggregator() }])
            category.registerColumn(name: "textValueB", aggregators: ["textValueB Count By Date": { CountingByDateAggregator(components: [.day]) }])
        }
        
        let builder = MockEventBuilder(userCount: userCount, eventCategories: eventCategories, using: &rng)
        return builder.generateEvents(count: eventCount, in: eventInterval, using: &rng)
    }
    
    func testBasicAggregators() async {
        KeystoneAnalyzer.fixedNowDate = Self.date(from: "2023-01-15T00:00:00+0000")
        
        let config = KeystoneConfig(userIdentifier: "ABC")
        let backend = MockBackend()
        let delegate = MockDelegate()
        
        var rng = ARC4RandomNumberGenerator(seed: 12345)
        let eventInterval = DateInterval(start: Self.date(from: "2023-01-01T00:00:00+0000"),
                                         end: Self.date(from: "2023-01-14T23:59:59+0000"))
        
        var builder = KeystoneAnalyzerBuilder(config: config, backend: backend, delegate: delegate)
        let events = Self.setupAnalyzer(builder: &builder, eventCount: 1_000,
                                        eventInterval: eventInterval,
                                        userCount: 5, using: &rng)
        
        XCTAssertEqual(events.count, 1_000)
        backend.mockEvents = events
        
        var numericValueA_Sum = 0.0
        var numericValueA_Cnt = 0
        var numericValueB_Sum = 0.0
        var numericValueB_Cnt = 0
        var numericEvent_Cnt = 0
        
        var textValueA_Cnts = [KeystoneEventData: Int]()
        var textValueB_Cnts = [DateComponents: Int]()
        var textEvent_Cnt = 0
        
        for event in events {
            switch event.category {
            case "numericEvent":
                numericEvent_Cnt += 1
                if let valueA = event.data["numericValueA"]?.numericValue {
                    numericValueA_Sum += valueA
                    numericValueA_Cnt += 1
                }
                if let valueB = event.data["numericValueB"]?.numericValue {
                    numericValueB_Sum += valueB
                    numericValueB_Cnt += 1
                }
            case "textEvent":
                textEvent_Cnt += 1
                
                if let valueA = event.data["textValueA"]?.stringValue {
                    textValueA_Cnts.modify(key: .text(value: valueA), defaultValue: 0) { $0 += 1 }
                }
                if let valueB = event.data["textValueB"]?.stringValue {
                    _ = valueB
                    let key = Calendar.reference.dateComponents([.day], from: event.date)
                    textValueB_Cnts.modify(key: key, defaultValue: 0) { $0 += 1 }
                }
                
            default:
                break
            }
        }
        
        let currentInterval = KeystoneAnalyzer.interval(containing: eventInterval.start)
        XCTAssertEqual(currentInterval, KeystoneAnalyzer.interval(containing: eventInterval.end))
        
        do {
            let analyzer = try await builder.build()
            
            let allEvent_Count = try await analyzer.findAggregator(withId: "All Event Count", in: currentInterval) as! CountingAggregator
            XCTAssertEqual(allEvent_Count.valueCount, 1_000)
            
            let numericEvent_Count = try await analyzer.findAggregator(withId: "numericEvent Count", in: currentInterval) as! CountingAggregator
            XCTAssertEqual(numericEvent_Count.valueCount, numericEvent_Cnt)
            
            let numericValueA_Stats = try await analyzer.findAggregator(withId: "numericValueA Stats", in: currentInterval) as! NumericStatsAggregator
            XCTAssertEqual(numericValueA_Stats.sum, numericValueA_Sum)
            XCTAssertEqual(numericValueA_Stats.valueCount, numericValueA_Cnt)
            XCTAssertLessThan(abs(numericValueA_Stats.runningAverage - numericValueA_Sum / Double(numericValueA_Cnt)), 0.1)
            
            let numericValueB_Stats = try await analyzer.findAggregator(withId: "numericValueB Stats", in: currentInterval) as! NumericStatsAggregator
            XCTAssertEqual(numericValueB_Stats.sum, numericValueB_Sum)
            XCTAssertEqual(numericValueB_Stats.valueCount, numericValueB_Cnt)
            XCTAssertLessThan(abs(numericValueB_Stats.runningAverage - numericValueB_Sum / Double(numericValueB_Cnt)), 0.1)
            
            let textEvent_Count = try await analyzer.findAggregator(withId: "textEvent Count", in: currentInterval) as! CountingAggregator
            XCTAssertEqual(textEvent_Count.valueCount, textEvent_Cnt)
            
            let textValueA_Count = try await analyzer.findAggregator(withId: "textValueA Count By Group", in: currentInterval) as! CountingByGroupAggregator
            XCTAssertEqual(textValueA_Count.groupedValues, textValueA_Cnts)
            
            let textValueB_Count = try await analyzer.findAggregator(withId: "textValueB Count By Date", in: currentInterval) as! CountingByDateAggregator
            XCTAssertEqual(textValueB_Count.groupedValues, textValueB_Cnts)
        }
        catch {
            XCTAssert(false, error.localizedDescription)
        }
    }
    
    func testBasicAggregatorsWithReload() async {
        KeystoneAnalyzer.fixedNowDate = Self.date(from: "2023-01-15T00:00:00+0000")
        
        let config = KeystoneConfig(userIdentifier: "ABC")
        let backend = MockBackend()
        let delegate = MockDelegate()
        
        var rng = ARC4RandomNumberGenerator(seed: 12345)
        let eventInterval = DateInterval(start: Self.date(from: "2023-01-01T00:00:00+0000"),
                                         end: Self.date(from: "2023-01-14T23:59:59+0000"))
        
        var builder = KeystoneAnalyzerBuilder(config: config, backend: backend, delegate: delegate)
        let events = Self.setupAnalyzer(builder: &builder, eventCount: 1_000,
                                        eventInterval: eventInterval,
                                        userCount: 5, using: &rng)
        
        XCTAssertEqual(events.count, 1_000)
        backend.mockEvents = events
        
        var numericValueA_Sum = 0.0
        var numericValueA_Cnt = 0
        var numericValueB_Sum = 0.0
        var numericValueB_Cnt = 0
        var numericEvent_Cnt = 0
        
        var textValueA_Cnts = [KeystoneEventData: Int]()
        var textValueB_Cnts = [DateComponents: Int]()
        var textEvent_Cnt = 0
        
        for event in events {
            switch event.category {
            case "numericEvent":
                numericEvent_Cnt += 1
                if let valueA = event.data["numericValueA"]?.numericValue {
                    numericValueA_Sum += valueA
                    numericValueA_Cnt += 1
                }
                if let valueB = event.data["numericValueB"]?.numericValue {
                    numericValueB_Sum += valueB
                    numericValueB_Cnt += 1
                }
            case "textEvent":
                textEvent_Cnt += 1
                
                if let valueA = event.data["textValueA"]?.stringValue {
                    textValueA_Cnts.modify(key: .text(value: valueA), defaultValue: 0) { $0 += 1 }
                }
                if let valueB = event.data["textValueB"]?.stringValue {
                    _ = valueB
                    let key = Calendar.reference.dateComponents([.day], from: event.date)
                    textValueB_Cnts.modify(key: key, defaultValue: 0) { $0 += 1 }
                }
                
            default:
                break
            }
        }
        
        let currentInterval = KeystoneAnalyzer.interval(containing: eventInterval.start)
        XCTAssertEqual(currentInterval, KeystoneAnalyzer.interval(containing: eventInterval.end))
        
        do {
            var analyzer = try await builder.build()
            
            for _ in 0..<2 {
                let allEvent_Count = try await analyzer.findAggregator(withId: "All Event Count", in: currentInterval) as! CountingAggregator
                XCTAssertEqual(allEvent_Count.valueCount, 1_000)
                
                let numericEvent_Count = try await analyzer.findAggregator(withId: "numericEvent Count", in: currentInterval) as! CountingAggregator
                XCTAssertEqual(numericEvent_Count.valueCount, numericEvent_Cnt)
                
                let numericValueA_Stats = try await analyzer.findAggregator(withId: "numericValueA Stats", in: currentInterval) as! NumericStatsAggregator
                XCTAssertEqual(numericValueA_Stats.sum, numericValueA_Sum)
                XCTAssertEqual(numericValueA_Stats.valueCount, numericValueA_Cnt)
                XCTAssertLessThan(abs(numericValueA_Stats.runningAverage - numericValueA_Sum / Double(numericValueA_Cnt)), 0.1)
                
                let numericValueB_Stats = try await analyzer.findAggregator(withId: "numericValueB Stats", in: currentInterval) as! NumericStatsAggregator
                XCTAssertEqual(numericValueB_Stats.sum, numericValueB_Sum)
                XCTAssertEqual(numericValueB_Stats.valueCount, numericValueB_Cnt)
                XCTAssertLessThan(abs(numericValueB_Stats.runningAverage - numericValueB_Sum / Double(numericValueB_Cnt)), 0.1)
                
                let textEvent_Count = try await analyzer.findAggregator(withId: "textEvent Count", in: currentInterval) as! CountingAggregator
                XCTAssertEqual(textEvent_Count.valueCount, textEvent_Cnt)
                
                let textValueA_Count = try await analyzer.findAggregator(withId: "textValueA Count By Group", in: currentInterval) as! CountingByGroupAggregator
                XCTAssertEqual(textValueA_Count.groupedValues, textValueA_Cnts)
                
                let textValueB_Count = try await analyzer.findAggregator(withId: "textValueB Count By Date", in: currentInterval) as! CountingByDateAggregator
                XCTAssertEqual(textValueB_Count.groupedValues, textValueB_Cnts)
                
                // Reload the analyzer
                analyzer = try await builder.build()
            }
        }
        catch {
            XCTAssert(false, error.localizedDescription)
        }
    }
    
    func testNewAggregators() async {
        KeystoneAnalyzer.fixedNowDate = Self.date(from: "2023-01-15T00:00:00+0000")
        
        let config = KeystoneConfig(userIdentifier: "ABC")
        let backend = MockBackend()
        let delegate = MockDelegate()
        
        var rng = ARC4RandomNumberGenerator(seed: 12345)
        let eventInterval = DateInterval(start: Self.date(from: "2023-01-01T00:00:00+0000"),
                                         end: Self.date(from: "2023-01-14T23:59:59+0000"))
        
        var builder = KeystoneAnalyzerBuilder(config: config, backend: backend, delegate: delegate)
        let events = Self.setupAnalyzer(builder: &builder, eventCount: 1_000,
                                        eventInterval: eventInterval,
                                        userCount: 5, using: &rng)
        
        XCTAssertEqual(events.count, 1_000)
        backend.mockEvents = events
        
        let currentInterval = KeystoneAnalyzer.interval(containing: eventInterval.start)
        XCTAssertEqual(currentInterval, KeystoneAnalyzer.interval(containing: eventInterval.end))
        
        do {
            var analyzer = try await builder.build()
            
            var allEvent_Count = try await analyzer.findAggregator(withId: "All Event Count", in: currentInterval) as! CountingAggregator
            XCTAssertEqual(allEvent_Count.valueCount, 1_000)
            
            // Reload the analyzer with a new aggregator
            builder.registerAllEventAggregator(id: "All Event Count 2") { CountingAggregator() }
            analyzer = try await builder.build()
            
            allEvent_Count = try await analyzer.findAggregator(withId: "All Event Count", in: currentInterval) as! CountingAggregator
            XCTAssertEqual(allEvent_Count.valueCount, 1_000)
            
            let allEvent_Count2 = try await analyzer.findAggregator(withId: "All Event Count 2", in: currentInterval) as! CountingAggregator
            XCTAssertEqual(allEvent_Count2.valueCount, 1_000)
        }
        catch {
            XCTAssert(false, error.localizedDescription)
        }
    }
    
    func testBasicAggregatorsWithReloadAndNewEvents() async {
        let splitDate = Self.date(from: "2023-01-07T23:59:59+0000")
        
        let config = KeystoneConfig(userIdentifier: "ABC")
        let backend = MockBackend()
        let delegate = MockDelegate()
        
        var rng = ARC4RandomNumberGenerator(seed: 12345)
        let eventInterval = DateInterval(start: Self.date(from: "2023-01-01T00:00:00+0000"),
                                         end: Self.date(from: "2023-01-14T23:59:59+0000"))
        
        var builder = KeystoneAnalyzerBuilder(config: config, backend: backend, delegate: delegate)
        let events = Self.setupAnalyzer(builder: &builder, eventCount: 1_000,
                                        eventInterval: eventInterval,
                                        userCount: 5, using: &rng)
        
        XCTAssertEqual(events.count, 1_000)
        backend.mockEvents = events
        
        let batches = [events.prefix { $0.date <= splitDate }, events.filter { $0.date > splitDate }]
        KeystoneAnalyzer.fixedNowDate = splitDate
        
        var analyzer = try! await builder.build()
        
        var totalEvent_Cnt = 0
        var numericValueA_Sum = 0.0
        var numericValueA_Cnt = 0
        var numericValueB_Sum = 0.0
        var numericValueB_Cnt = 0
        var numericEvent_Cnt = 0
        
        var textValueA_Cnts = [KeystoneEventData: Int]()
        var textValueB_Cnts = [DateComponents: Int]()
        var textEvent_Cnt = 0
        
        for batch in batches {
            for event in batch {
                totalEvent_Cnt += 1
                
                switch event.category {
                case "numericEvent":
                    numericEvent_Cnt += 1
                    if let valueA = event.data["numericValueA"]?.numericValue {
                        numericValueA_Sum += valueA
                        numericValueA_Cnt += 1
                    }
                    if let valueB = event.data["numericValueB"]?.numericValue {
                        numericValueB_Sum += valueB
                        numericValueB_Cnt += 1
                    }
                case "textEvent":
                    textEvent_Cnt += 1
                    
                    if let valueA = event.data["textValueA"]?.stringValue {
                        textValueA_Cnts.modify(key: .text(value: valueA), defaultValue: 0) { $0 += 1 }
                    }
                    if let valueB = event.data["textValueB"]?.stringValue {
                        _ = valueB
                        let key = Calendar.reference.dateComponents([.day], from: event.date)
                        textValueB_Cnts.modify(key: key, defaultValue: 0) { $0 += 1 }
                    }
                    
                default:
                    break
                }
            }
            
            let currentInterval = KeystoneAnalyzer.interval(containing: eventInterval.start)
            XCTAssertEqual(currentInterval, KeystoneAnalyzer.interval(containing: eventInterval.end))
            
            do {
                let allEvent_Count = try await analyzer.findAggregator(withId: "All Event Count", in: currentInterval) as! CountingAggregator
                XCTAssertEqual(allEvent_Count.valueCount, totalEvent_Cnt)
                
                let numericEvent_Count = try await analyzer.findAggregator(withId: "numericEvent Count", in: currentInterval) as! CountingAggregator
                XCTAssertEqual(numericEvent_Count.valueCount, numericEvent_Cnt)
                
                let numericValueA_Stats = try await analyzer.findAggregator(withId: "numericValueA Stats", in: currentInterval) as! NumericStatsAggregator
                XCTAssertEqual(numericValueA_Stats.sum, numericValueA_Sum)
                XCTAssertEqual(numericValueA_Stats.valueCount, numericValueA_Cnt)
                XCTAssertLessThan(abs(numericValueA_Stats.runningAverage - numericValueA_Sum / Double(numericValueA_Cnt)), 0.1)
                
                let numericValueB_Stats = try await analyzer.findAggregator(withId: "numericValueB Stats", in: currentInterval) as! NumericStatsAggregator
                XCTAssertEqual(numericValueB_Stats.sum, numericValueB_Sum)
                XCTAssertEqual(numericValueB_Stats.valueCount, numericValueB_Cnt)
                XCTAssertLessThan(abs(numericValueB_Stats.runningAverage - numericValueB_Sum / Double(numericValueB_Cnt)), 0.1)
                
                let textEvent_Count = try await analyzer.findAggregator(withId: "textEvent Count", in: currentInterval) as! CountingAggregator
                XCTAssertEqual(textEvent_Count.valueCount, textEvent_Cnt)
                
                let textValueA_Count = try await analyzer.findAggregator(withId: "textValueA Count By Group", in: currentInterval) as! CountingByGroupAggregator
                XCTAssertEqual(textValueA_Count.groupedValues, textValueA_Cnts)
                
                let textValueB_Count = try await analyzer.findAggregator(withId: "textValueB Count By Date", in: currentInterval) as! CountingByDateAggregator
                XCTAssertEqual(textValueB_Count.groupedValues, textValueB_Cnts)
            }
            catch {
                XCTAssert(false, error.localizedDescription)
            }
            
            // Reload the analyzer
            KeystoneAnalyzer.fixedNowDate = eventInterval.end
            analyzer = try! await builder.build()
        }
    }
    
    func testEventLoading() async {
        KeystoneAnalyzer.fixedNowDate = Self.date(from: "2023-02-07T23:59:59+0000")
        
        let config = KeystoneConfig(userIdentifier: "ABC")
        let backend = MockBackend()
        let delegate = MockDelegate()
        
        var rng = ARC4RandomNumberGenerator(seed: 987654321)
        let eventInterval = DateInterval(start: Self.date(from: "2023-01-25T00:00:01+0000"),
                                         end: Self.date(from: "2023-02-07T23:59:59+0000"))
        
        var builder = KeystoneAnalyzerBuilder(config: config, backend: backend, delegate: delegate)
        let events = Self.setupAnalyzer(builder: &builder, eventCount: 1_000,
                                        eventInterval: eventInterval,
                                        userCount: 5, using: &rng)
        
        XCTAssertEqual(events.count, 1_000)
        backend.mockEvents = events
        
        let analyzer = try! await builder.build()
        
        // Normal interval
        let currentEventCount = await analyzer.loadEvents(in: KeystoneAnalyzer.currentEventInterval)?.count
        XCTAssertEqual(500, currentEventCount)
        
        let previousEventCount = await analyzer.loadEvents(in: KeystoneAnalyzer.interval(before: KeystoneAnalyzer.currentEventInterval))?.count
        XCTAssertEqual(500, previousEventCount)
        
        let previous2EventCount = await analyzer.loadEvents(in: KeystoneAnalyzer.interval(before:
                                                                                            KeystoneAnalyzer.interval(before: KeystoneAnalyzer.currentEventInterval)))?.count
        XCTAssertNil(previous2EventCount)
        
        // Weekly interval
        let currentWeek = KeystoneAnalyzer.weekInterval(containing: KeystoneAnalyzer.now, weekStartsOn: .monday)
        let currentWeekEventCount = await analyzer.loadEvents(in: currentWeek)!.count
        XCTAssertLessThan(abs((2.0 / 14.0) * 1_000 - Double(currentWeekEventCount)), 1)
        
        let lastWeek = KeystoneAnalyzer.weekInterval(before: currentWeek, weekStartsOn: .monday)
        let lastWeekEventCount = await analyzer.loadEvents(in: lastWeek)!.count
        XCTAssertLessThan(abs((7.0 / 14.0) * 1_000 - Double(lastWeekEventCount)), 1)
        
        let twoWeeksAgo = KeystoneAnalyzer.weekInterval(before: lastWeek, weekStartsOn: .monday)
        let twoWeeksAgoEventCount = await analyzer.loadEvents(in: twoWeeksAgo)!.count
        XCTAssertLessThan(abs((5.0 / 14.0) * 1_000 - Double(twoWeeksAgoEventCount)), 1)
        
        // Daily interval
        for i in 0..<14 {
            let day = eventInterval.start.addingTimeInterval(TimeInterval(i)*24*60*60)
            let interval = DateInterval(start: day.startOfDay, end: day.endOfDay)
            let eventCount = await analyzer.loadEvents(in: interval)!.count
            XCTAssertLessThan(abs((1.0 / 14.0) * 1_000 - Double(eventCount)), 1)
        }
    }
}
