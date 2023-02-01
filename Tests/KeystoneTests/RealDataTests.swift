
import Foundation
@testable import Keystone
import XCTest

@MainActor final class RealDataTests: XCTestCase {
    /// The test analyzer.
    var builder: KeystoneAnalyzerBuilder? = nil
    
    /// The test backend.
    var backend: TestDataBackend? = nil
    
    override func setUp() async throws {
        if backend == nil {
            let backend = TestDataBackend()
            try await backend.initialize()
            
            self.backend = backend
        }
        
        var config = KeystoneConfig(userIdentifier: "ABC-TEST")
        config.log = { _, message in print(message) }
        
        self.builder = KeystoneAnalyzerBuilder(config: config, backend: self.backend!, delegate: MockDelegate())
    }
    
    func testCountingAggregators() async throws {
        guard var builder, let backend else {
            XCTAssert(false, "expected builder to exist")
            return
        }
        
        KeystoneAnalyzer.setNowDate(backend.events.last!.date.addingTimeInterval(24*60*60))
        
        builder.registerAllEventAggregator(id: "allEventCounter") { CountingAggregator() }
        builder.registerAllEventAggregator(id: "allEventCounterByDay") { CountingByDateAggregator(scope: .day) }
        builder.registerAllEventAggregator(id: "allEventCounterByMonth") { CountingByDateAggregator(scope: .month) }
        builder.registerAllEventAggregator(id: "duplicateChecker") { DuplicateEventChecker() }
        
        builder.registerCategory(name: "sessionStart") { category in
            category.registerAggregator(id: "sessionStartCounter") { CountingAggregator() }
        }
        
        builder.registerCategory(name: "leveledUp") { category in
            category.registerColumn(name: "level") { column in
                column.registerAggregator(id: "leveledUpStats") { NumericStatsAggregator() }
            }
        }
        
        let analyzer = try await builder.build()
        
        // All events, CountingAggregator
        let allEventCounter = try await analyzer.findAggregator(withId: "allEventCounter", in: KeystoneAnalyzer.allEncompassingDateInterval)
        XCTAssertNotNil(allEventCounter)
        XCTAssert(allEventCounter is CountingAggregator)
        XCTAssertEqual((allEventCounter as! CountingAggregator).valueCount, backend.events.count)
        
        // All events, CountingByDateAggregator(.day)
        var allEventCountsByDayExpectedValue = [Date: Int]()
        var allEventCountsByMonthExpectedValue = [Date: Int]()
        
        for event in backend.events {
            allEventCountsByDayExpectedValue.modify(key: event.date.startOfDay, defaultValue: 0) { $0 += 1}
            allEventCountsByMonthExpectedValue.modify(key: event.date.startOfMonth, defaultValue: 0) { $0 += 1}
        }
        
        let allEventCounterByDay = try await analyzer.findAggregator(withId: "allEventCounterByDay", in: KeystoneAnalyzer.allEncompassingDateInterval)
        XCTAssertNotNil(allEventCounterByDay)
        XCTAssert(allEventCounterByDay is CountingByDateAggregator)
        XCTAssertEqual((allEventCounterByDay as! CountingByDateAggregator).groupedValues, allEventCountsByDayExpectedValue)
        
        // All events, CountingByDateAggregator(.month)
        let allEventCounterByMonth = try await analyzer.findAggregator(withId: "allEventCounterByMonth", in: KeystoneAnalyzer.allEncompassingDateInterval)
        XCTAssertNotNil(allEventCounterByMonth)
        XCTAssert(allEventCounterByMonth is CountingByDateAggregator)
        XCTAssertEqual((allEventCounterByMonth as! CountingByDateAggregator).groupedValues, allEventCountsByMonthExpectedValue)
        
        // sessionStart events, CountingAggregator
        let sessionStartCounter = try await analyzer.findAggregator(withId: "sessionStartCounter", in: KeystoneAnalyzer.allEncompassingDateInterval)
        XCTAssertNotNil(sessionStartCounter)
        XCTAssert(sessionStartCounter is CountingAggregator)
        XCTAssertEqual((sessionStartCounter as! CountingAggregator).valueCount, backend.events.filter { $0.category == "sessionStart" }.count)
        
        // leveledUp events, NumericStatsAggregator
        let leveledUpStats = try await analyzer.findAggregator(withId: "leveledUpStats", in: KeystoneAnalyzer.allEncompassingDateInterval)
        XCTAssertNotNil(leveledUpStats)
        XCTAssert(leveledUpStats is NumericStatsAggregator)
        XCTAssertGreaterThan((leveledUpStats as! NumericStatsAggregator).valueCount, 0)
        XCTAssertEqual((leveledUpStats as! NumericStatsAggregator).valueCount, backend.events.filter { $0.category == "leveledUp" }.count)
        
        // --- Reload analyzer, no new aggregators ---
        
        let analyzer3 = try await builder.build()
        
        // All events, CountingAggregator, reloaded
        let allEventCounter3 = try await analyzer3.findAggregator(withId: "allEventCounter", in: KeystoneAnalyzer.allEncompassingDateInterval)
        XCTAssertNotNil(allEventCounter3)
        XCTAssert(allEventCounter3 is CountingAggregator)
        XCTAssertEqual((allEventCounter3 as! CountingAggregator).valueCount, backend.events.count)
        
        // --- Add new aggregator, reload analyzer ---
        builder.registerAllEventAggregator(id: "allEventCounter2") { CountingAggregator() }
        builder.registerCategory(name: "sessionStart") { category in
            category.registerAggregator(id: "sessionStartCounter2") { CountingAggregator() }
        }
        
        let analyzer2 = try await builder.build()
        
        // All events, CountingAggregator, added later
        let allEventCounter2 = try await analyzer2.findAggregator(withId: "allEventCounter2", in: KeystoneAnalyzer.allEncompassingDateInterval)
        XCTAssertNotNil(allEventCounter2)
        XCTAssert(allEventCounter2 is CountingAggregator)
        XCTAssertEqual((allEventCounter2 as! CountingAggregator).valueCount, backend.events.count)
        
        // sessionStart events, CountingAggregator, added later
        let sessionStartCounter2 = try await analyzer2.findAggregator(withId: "sessionStartCounter2", in: KeystoneAnalyzer.allEncompassingDateInterval)
        XCTAssertNotNil(sessionStartCounter2)
        XCTAssert(sessionStartCounter2 is CountingAggregator)
        XCTAssertEqual((sessionStartCounter2 as! CountingAggregator).valueCount, backend.events.filter { $0.category == "sessionStart" }.count)
    }
    
    func testCountingByDateAggregators() async throws {
        guard var builder, let backend else {
            XCTAssert(false, "expected builder to exist")
            return
        }
        
        let totalEventSpan = backend.events.last!.date.timeIntervalSinceReferenceDate - backend.events.first!.date.timeIntervalSinceReferenceDate
        let halfDate = backend.events.first!.date.addingTimeInterval(totalEventSpan * 0.5)
        
        KeystoneAnalyzer.setNowDate(halfDate)
        
        builder.registerCategory(name: "onboardingCompleted") { category in
            category.registerAggregator(id: "completedOnboardings") { CountingByDateAggregator(scope: .day) }
        }
        
        let analyzer = try await builder.build()
        
        // onboardingCompleted events, CountingByDateAggregator
        let completedOnboardings = try await analyzer.findAggregator(withId: "completedOnboardings", in: KeystoneAnalyzer.allEncompassingDateInterval)
        XCTAssertNotNil(completedOnboardings)
        XCTAssert(completedOnboardings is CountingByDateAggregator)
        XCTAssertEqual((completedOnboardings as! CountingByDateAggregator).totalEventCount, backend.events.filter { $0.date <= halfDate && $0.category == "onboardingCompleted" }.count)
        
        KeystoneAnalyzer.setNowDate(backend.events.last!.date.addingTimeInterval(24*60*60))
        
        let analyzer2 = try await builder.build()
        
        // onboardingCompleted events, CountingByDateAggregator, reloaded
        let completedOnboardings2 = try await analyzer2.findAggregator(withId: "completedOnboardings", in: KeystoneAnalyzer.allEncompassingDateInterval)
        XCTAssertNotNil(completedOnboardings2)
        XCTAssert(completedOnboardings2 is CountingByDateAggregator)
        XCTAssertEqual((completedOnboardings2 as! CountingByDateAggregator).totalEventCount, backend.events.filter { $0.category == "onboardingCompleted" }.count)
    }
}
