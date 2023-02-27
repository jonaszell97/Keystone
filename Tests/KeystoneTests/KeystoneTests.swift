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
        builder.registerAllEventAggregator(id: "All Event Count By Month") { CountingByDateAggregator(scope: .month) }
        
        builder.registerCategory(name: "numericEvent") { category in
            category.registerAggregator(id: "numericEvent Count") { CountingAggregator() }
            category.registerColumn(name: "numericValueA") { $0.registerAggregator(id: "numericValueA Stats") { NumericStatsAggregator() } }
            category.registerColumn(name: "numericValueB") { $0.registerAggregator(id: "numericValueB Stats") { NumericStatsAggregator() } }
        }
        
        builder.registerCategory(name: "textEvent") { category in
            category.registerAggregator(id: "textEvent Count") { CountingAggregator() }
            category.registerColumn(name: "textValueA") { $0.registerAggregator(id: "textValueA Count By Group") { CountingByGroupAggregator() } }
            category.registerColumn(name: "textValueB") { $0.registerAggregator(id: "textValueB Count By Date") { CountingByDateAggregator(scope: .day) } }
        }
        
        let builder = MockEventBuilder(userCount: userCount, eventCategories: eventCategories, using: &rng)
        return builder.generateEvents(count: eventCount, in: eventInterval, using: &rng)
    }
    
    func testBasicAggregators() async {
        KeystoneAnalyzer.setNowDate(Self.date(from: "2023-01-15T00:00:00+0000"))
        
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
        var textValueB_Cnts = [Date: Int]()
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
                    let key = DateAggregatorScope.day.scopeStartDate(from: event.date)
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
        KeystoneAnalyzer.setNowDate(Self.date(from: "2023-01-15T00:00:00+0000"))
        
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
        var textValueB_Cnts = [Date: Int]()
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
                    let key = DateAggregatorScope.day.scopeStartDate(from: event.date)
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
        KeystoneAnalyzer.setNowDate(Self.date(from: "2023-01-15T00:00:00+0000"))
        
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
        KeystoneAnalyzer.setNowDate(splitDate)
        
        var analyzer = try! await builder.build()
        
        var totalEvent_Cnt = 0
        var numericValueA_Sum = 0.0
        var numericValueA_Cnt = 0
        var numericValueB_Sum = 0.0
        var numericValueB_Cnt = 0
        var numericEvent_Cnt = 0
        
        var textValueA_Cnts = [KeystoneEventData: Int]()
        var textValueB_Cnts = [Date: Int]()
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
                        let key = DateAggregatorScope.day.scopeStartDate(from: event.date)
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
            KeystoneAnalyzer.setNowDate(eventInterval.end)
            analyzer = try! await builder.build()
        }
    }
    
    func testEventLoading() async {
        KeystoneAnalyzer.setNowDate(Self.date(from: "2023-02-07T23:59:59+0000"))
        
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
        let currentEventCount = await analyzer.getProcessedEvents(in: KeystoneAnalyzer.currentEventInterval)?.events.count
        XCTAssertEqual(500, currentEventCount)
        
        let previousEventCount = await analyzer.getProcessedEvents(in: KeystoneAnalyzer.interval(before: KeystoneAnalyzer.currentEventInterval))?.events.count
        XCTAssertEqual(500, previousEventCount)
        
        let previous2EventCount = await analyzer.getProcessedEvents(in:
                                                                        KeystoneAnalyzer.interval(before: KeystoneAnalyzer.interval(before: KeystoneAnalyzer.currentEventInterval)))?.events.count
        XCTAssertNil(previous2EventCount)
        
        // Weekly interval
        let currentWeek = KeystoneAnalyzer.weekInterval(containing: KeystoneAnalyzer.now, weekStartsOnMonday: true)
        let currentWeekEventCount = await analyzer.getProcessedEvents(in: currentWeek)!.events.count
        XCTAssertLessThan(abs((2.0 / 14.0) * 1_000 - Double(currentWeekEventCount)), 1)
        
        let lastWeek = KeystoneAnalyzer.weekInterval(before: currentWeek, weekStartsOnMonday: true)
        let lastWeekEventCount = await analyzer.getProcessedEvents(in: lastWeek)!.events.count
        XCTAssertLessThan(abs((7.0 / 14.0) * 1_000 - Double(lastWeekEventCount)), 1)
        
        let twoWeeksAgo = KeystoneAnalyzer.weekInterval(before: lastWeek, weekStartsOnMonday: true)
        let twoWeeksAgoEventCount = await analyzer.getProcessedEvents(in: twoWeeksAgo)!.events.count
        XCTAssertLessThan(abs((5.0 / 14.0) * 1_000 - Double(twoWeeksAgoEventCount)), 1)
        
        // Daily interval
        for i in 0..<14 {
            let day = eventInterval.start.addingTimeInterval(TimeInterval(i)*24*60*60)
            let interval = DateInterval(start: day.startOfDay, end: day.endOfDay)
            let eventCount = await analyzer.getProcessedEvents(in: interval)!.events.count
            XCTAssertLessThan(abs((1.0 / 14.0) * 1_000 - Double(eventCount)), 1)
        }
    }
    
    func testChainingByGroupAggregator() async {
        let config = KeystoneConfig(userIdentifier: "ABC")
        let backend = MockBackend()
        let delegate = MockDelegate()
        let client = KeystoneClient(config: config, backend: backend)
        
        var builder = KeystoneAnalyzerBuilder(config: config, backend: backend, delegate: delegate)
        builder.registerCategory(name: "testEvent") { category in
            category.registerColumn(name: "group") { column in
                column.registerAggregator(id: "Group Counter") {
                    ChainingByGroupAggregator { CountingAggregator() }
                }
            }
        }
        
        KeystoneAnalyzer.setNowDate(Self.date(from: "2023-01-25T23:59:59+0000"))
        let events = [
            client.createEvent(category: "testEvent", data: ["group": .text(value: "A"),]),
            client.createEvent(category: "testEvent", data: ["group": .text(value: "A"),]),
            client.createEvent(category: "testEvent", data: ["group": .text(value: "B"),]),
            client.createEvent(category: "testEvent", data: ["group": .text(value: "C"),]),
            client.createEvent(category: "testEvent", data: ["group": .text(value: "C"),]),
            client.createEvent(category: "testEvent", data: ["group": .text(value: "C"),]),
            client.createEvent(category: "testEvent", data: ["group": .text(value: "D"),]),
            client.createEvent(category: "testEvent", data: ["group": .text(value: "D"),]),
        ]
        
        KeystoneAnalyzer.setNowDate(Self.date(from: "2023-02-07T23:59:59+0000"))
        backend.mockEvents = events
        
        let analyzer = try! await builder.build()
        let aggregator = try! await analyzer.findAggregator(withId: "Group Counter", in: KeystoneAnalyzer.allEncompassingDateInterval)
        let counter = aggregator as! ChainingByGroupAggregator
        
        XCTAssertEqual((counter.groupedAggregators[.text(value: "A")]! as! CountingAggregator).valueCount, 2)
        XCTAssertEqual((counter.groupedAggregators[.text(value: "B")]! as! CountingAggregator).valueCount, 1)
        XCTAssertEqual((counter.groupedAggregators[.text(value: "C")]! as! CountingAggregator).valueCount, 3)
        XCTAssertEqual((counter.groupedAggregators[.text(value: "D")]! as! CountingAggregator).valueCount, 2)
    }
    
    func testSearch() async {
        let config = KeystoneConfig(userIdentifier: "ABC", createSearchIndex: true)
        let backend = MockBackend()
        let delegate = MockDelegate()
        let client = KeystoneClient(config: config, backend: backend)
        
        let builder = KeystoneAnalyzerBuilder(config: config, backend: backend, delegate: delegate)
        
        KeystoneAnalyzer.setNowDate(Self.date(from: "2023-01-25T23:59:59+0000"))
        let events = [
            client.createEvent(category: "testEvent", data: ["id": .number(value: 1), "message": .text(value: "the quick"),]),
            client.createEvent(category: "testEvent", data: ["id": .number(value: 2), "messages": .text(value: "brown fox"),]),
            client.createEvent(category: "testEvent", data: ["id": .number(value: 3), "message": .text(value: "jumps over the"),]),
            client.createEvent(category: "testEvent", data: ["id": .number(value: 4), "messageX": .text(value: "lazy dog"),]),
            client.createEvent(category: "testEvent", data: ["id": .number(value: 5), "message": .text(value: "In publishing and graphic design, Lorem ipsum is a placeholder text "),]),
            client.createEvent(category: "testEvent", data: ["id": .number(value: 6), "messag3": .text(value: "commonly used to demonstrate the visual form of a document or a typeface"),]),
            client.createEvent(category: "testEvent", data: ["id": .number(value: 7), "mess4g3": .text(value: "without relying on meaningful content."),]),
            client.createEvent(category: "testEvent", data: ["id": .number(value: 8), "message": .text(value: """
The Lorem ipsum text is derived from sections 1.10.32 and 1.10.33 of Cicero's 'De finibus bonorum et malorum'.
"""),]),
        ]
        
        let tests: [(String, Set<Int>)] = [
            ("", [1,2,3,4,5,6,7,8]),
            ("mess", []),
            
            ("xxx", []),
            ("jumps over the lazy dog", []),
            
            ("fo", [2, 6]),
            ("fox", [2]),
            ("FOX", [2]),
            ("brown fox", [2]),
            ("bro fo", [2]),
            ("bRo Fo", [2]),
            
            ("jumps over t", [3]),
            ("jumps OVER the", [3]),
            ("jumps the", [3]),
            ("jumps ThE over", [3]),
            ("over ju", [3]),
            
            ("design,", [5]),
            
            ("and", [5, 8]),
            ("is", [5, 8]),
            
            ("re", [7]),
            ("me content.", [7]),
            
            ("1.10", [8]),
            ("de 1.10", [8]),
            ("Cic", [8]),
            ("cicero'", [8]),
        ]
        
        KeystoneAnalyzer.setNowDate(Self.date(from: "2023-02-07T23:59:59+0000"))
        backend.mockEvents = events
        
        let analyzer = try! await builder.build()
        let eventList = await analyzer.getProcessedEvents(in: KeystoneAnalyzer.interval(before: KeystoneAnalyzer.currentEventInterval))!
        
        XCTAssertEqual(eventList.events.count, events.count)
        
        let predicate = eventList.searchPredicate
        for (keyword, expectedIds) in tests {
            let ids = Set(eventList.events.filter { predicate(keyword, $0) }.map { Int($0.data["id"]!.numericValue!) })
            XCTAssertEqual(ids, expectedIds, "search failed for keyword \(keyword)")
        }
    }
}
