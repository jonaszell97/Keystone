
import CloudKit

public enum BackendStatus {
    /// The backend is ready.
    case ready
    
    /// The backend fetched a given number of records.
    case fetchedRecords(count: Int)
    
    /// The backend is processing records.
    case processingRecords(progress: Double)
}

public protocol KeystoneBackend {
    /// Persist an event to the backend.
    func persist(event: KeystoneEvent) async throws
    
    /// Persist a list of events to the backend.
    func persist(events: [KeystoneEvent]) async throws
    
    /// Load all events.
    func loadAllEvents(updateStatus: @escaping (BackendStatus) -> Void) async throws -> [KeystoneEvent]
    
    /// Load events starting from a date.
    func loadEvents(in interval: DateInterval,
                    updateStatus: @escaping (BackendStatus) -> Void) async throws -> [KeystoneEvent]
}

public extension KeystoneBackend {
    /// Load all events.
    func loadAllEvents(updateStatus: @escaping (BackendStatus) -> Void) async throws -> [KeystoneEvent] {
        try await self.loadEvents(in: .init(start: .distantPast, end: .distantFuture), updateStatus: updateStatus)
    }
    
    /// Persist a list of events to the backend.
    func persist(events: [KeystoneEvent]) async throws {
        for event in events {
            try await self.persist(event: event)
        }
    }
}

public final class CloudKitBackend: KeystoneBackend {
    /// The configuration object.
    let config: KeystoneConfig
    
    /// The identifier of the iCloud container where analytics are stored.
    let containerIdentifier: String
    
    /// The name of the analytics table in CloudKit.
    let tableName: String
    
    /// The iCloud container.
    let container: CKContainer
    
    /// The name of the column identifying user IDs.
    let userIdColumnName: String
    
    /// The user's iCloud account status.
    public let accountStatus: CKAccountStatus
    
    /// The unique iCloud record ID for the user.
    let iCloudRecordID: String
    
    /// Default initializer.
    public init(config: KeystoneConfig,
                containerIdentifier: String,
                tableName: String,
                userIdColumnName: String = "userId") async throws {
        self.config = config
        self.containerIdentifier = containerIdentifier
        self.tableName = tableName
        self.userIdColumnName = userIdColumnName
        
        let container = CKContainer(identifier: containerIdentifier)
        self.container = container
        
        let iCloudRecordID: String = try await withCheckedThrowingContinuation { continuation in
            container.fetchUserRecordID(completionHandler: { (recordID, error) in
                if let error {
                    continuation.resume(throwing: error)
                    return
                }
                
                guard let id = recordID?.recordName else {
                    continuation.resume(throwing: CKError(.notAuthenticated))
                    return
                }
                
                continuation.resume(returning: id)
            })
        }
        
        let accountStatus = await withCheckedContinuation { continuation in
            container.accountStatus { status, error in
                if let error {
                    config.log?(.error, "error fetching account status: \(error.localizedDescription)")
                }
                
                continuation.resume(returning: status)
            }
        }
        
        switch accountStatus {
        case .available:
            fallthrough
        case .temporarilyUnavailable:
            break
        case .couldNotDetermine:
            fallthrough
        case .noAccount:
            fallthrough
        case .restricted:
            fallthrough
        @unknown default:
            throw CKError(.notAuthenticated)
        }
        
        self.iCloudRecordID = iCloudRecordID
        self.accountStatus = accountStatus
    }
}

// MARK: Event submission

extension CloudKitBackend {
    /// Submit an event to CloudKit.
    public func persist(event: KeystoneEvent) async throws {
        guard case .available = self.accountStatus else {
            config.log?(.debug, "early exit in persist(event:) because account status is \(self.accountStatus)")
            throw CKError(.accountTemporarilyUnavailable)
        }
        
        let record = CKRecord(recordType: self.tableName, recordID: .init(recordName: event.id.uuidString))
        try populateRecord(record, for: event)
        
        try await container.publicCloudDatabase.save(record)
    }
    
    /// Submit multiple events to CloudKit.
    public func persist(events: [KeystoneEvent]) async throws {
        guard case .available = self.accountStatus else {
            config.log?(.debug, "early exit in persist(event:) because account status is \(self.accountStatus)")
            throw CKError(.accountTemporarilyUnavailable)
        }
        
        let records = try events.map { event in
            let record = CKRecord(recordType: self.tableName, recordID: .init(recordName: event.id.uuidString))
            try populateRecord(record, for: event)
            
            return record
        }
        
        let (saveResults, _) = try await container.publicCloudDatabase.modifyRecords(saving: records, deleting: [])
        for (id, result) in saveResults {
            if case .failure(let error) = result {
                config.log?(.error, "error saving record with ID \(id): \(error.localizedDescription)")
            }
        }
    }
}

// MARK: Fetch events

extension CloudKitBackend {
    /// Load events starting from a date.
    public func loadEvents(in interval: DateInterval, updateStatus: @escaping (BackendStatus) -> Void)
        async throws -> [KeystoneEvent]
    {
        guard case .available = self.accountStatus else {
            config.log?(.debug, "early exit in loadEvents(in:updateStatus) because account status is \(self.accountStatus)")
            throw CKError(.accountTemporarilyUnavailable)
        }
        
        config.log?(.debug, "[CloudKitBackend] fetching events in interval \(interval)")
        
        let records = try await self.loadNewRecords(in: interval) { loadedRecords in
            updateStatus(.fetchedRecords(count: loadedRecords))
        }
        
        return try await self.createEvents(from: records) { progress in
            updateStatus(.processingRecords(progress: progress))
        }
    }
    
    /// Load all events.
    public func loadAllEvents(updateStatus: @escaping (BackendStatus) -> Void) async throws -> [KeystoneEvent] {
        guard case .available = self.accountStatus else {
            config.log?(.debug, "early exit in loadAllEvents(updateStatus:) because account status is \(self.accountStatus)")
            throw CKError(.accountTemporarilyUnavailable)
        }
        
        config.log?(.debug, "[CloudKitBackend] fetching all events")
        
        let records = try await self.loadNewRecords() { loadedRecords in
            updateStatus(.fetchedRecords(count: loadedRecords))
        }
        
        return try await self.createEvents(from: records) { progress in
            updateStatus(.processingRecords(progress: progress))
        }
    }
    
    /// Load all records matching a predicate.
    private func loadAllRecords(matching predicate: NSPredicate,
                                updateProgress: @escaping (Int) -> Void) async throws -> [CKRecord] {
        let query = CKQuery(recordType: self.tableName, predicate: predicate)
        query.sortDescriptors = [NSSortDescriptor(key: "eventDate", ascending: true)]
        
        var records = [CKRecord]()
        return try await withCheckedThrowingContinuation { continuation in
            self.loadRecordsBatched(matching: query) { nextResults, error, moreResultsAvailable in
                if let error {
                    continuation.resume(throwing: error)
                    return false
                }
                
                guard let nextResults else {
                    continuation.resume(throwing: CKError(.partialFailure))
                    return false
                }
                
                records.append(contentsOf: nextResults)
                
                let recordCount = records.count
                Task { @MainActor in
                    updateProgress(recordCount)
                }
                
                guard moreResultsAvailable else {
                    continuation.resume(returning: records)
                    return false
                }
                
                return true
            }
        }
    }
    
    /// Load all records created after a given date.
    private func loadNewRecords(in interval: DateInterval, updateProgress: @escaping (Int) -> Void) async throws -> [CKRecord] {
        let predicate = NSPredicate(format: "eventDate >= %@ AND eventDate <= %@",
                                    NSDate(timeIntervalSinceReferenceDate: interval.start.timeIntervalSinceReferenceDate),
                                    NSDate(timeIntervalSinceReferenceDate: interval.end.timeIntervalSinceReferenceDate))
        
        return try await loadAllRecords(matching: predicate, updateProgress: updateProgress)
    }
    
    /// Load all records created after a given date.
    private func loadNewRecords(updateProgress: @escaping (Int) -> Void) async throws -> [CKRecord] {
        try await loadAllRecords(matching: NSPredicate(value: true), updateProgress: updateProgress)
    }
    
    /// Create events from loaded records.
    private func createEvents(from records: [CKRecord], updateProgress: @MainActor @escaping (Double) -> Void) async throws -> [KeystoneEvent] {
        return try await withCheckedThrowingContinuation { continuation in
            var events = [KeystoneEvent]()
            for i in 0..<records.count {
                let record = records[i]
                do {
                    let event = try self.decodeRecord(record: record)
                    events.append(event)
                }
                catch {
                    continuation.resume(throwing: error)
                    return
                }
                
                Task { @MainActor in
                    updateProgress(Double(i + 1) / Double(records.count))
                }
            }
            
            continuation.resume(returning: events)
        }
    }
}

extension CloudKitBackend {
    /// Load all records matching a query. The handler callback is invoked whenever new results are available.
    /// If the handler callback returns false, no more records are fetched.
    private func loadRecordsBatched(matching query: CKQuery,
                                    receiveResults: @escaping @MainActor ([CKRecord]?, Error?, Bool) -> Bool) {
        let operation = CKQueryOperation(query: query)
        operation.resultsLimit = CKQueryOperation.maximumResults
        
        var data = [CKRecord]()
        let recordMatchedBlock: (CKRecord.ID, Result<CKRecord, Error>) -> Void = { _, result in
            switch result {
            case .success(let record):
                data.append(record)
            case .failure(let error):
                self.config.log?(.error, "error loading record: \(error.localizedDescription)")
            }
        }
        
        var queryCompletion: Optional<(Result<CKQueryOperation.Cursor?, Error>) -> Void> = nil
        queryCompletion = { result in
            let localData = data
            data.removeAll()
            
            let queryCompletion = queryCompletion
            Task { @MainActor in
                if case .failure(let error) = result {
                    _ = receiveResults(nil, error, false)
                    return
                }
                
                guard case .success(let cursor) = result else {
                    _ = receiveResults(nil, CKError(.batchRequestFailed), false)
                    return
                }
                
                let shouldContinue = receiveResults(localData, nil, true)
                guard shouldContinue else {
                    return
                }
                
                guard let cursor = cursor else {
                    _ = receiveResults([], nil, false)
                    return
                }
                
                let nextOperation = CKQueryOperation(cursor: cursor)
                nextOperation.queryResultBlock = queryCompletion
                nextOperation.recordMatchedBlock = recordMatchedBlock
                
                self.container.publicCloudDatabase.add(nextOperation)
            }
            
        }
        
        operation.queryResultBlock = queryCompletion
        operation.recordMatchedBlock = recordMatchedBlock
        
        self.container.publicCloudDatabase.add(operation)
    }
    
    /// Load all records matching a query.
    private func loadAllRecords(matching query: CKQuery,
                                limit: Int? = nil,
                                _ onComplete: @escaping @MainActor ([CKRecord]) -> Void) {
        let operation = CKQueryOperation(query: query)
        operation.resultsLimit = limit ?? CKQueryOperation.maximumResults
        
        var data = [CKRecord]()
        let recordMatchedBlock: (CKRecord.ID, Result<CKRecord, Error>) -> Void = { _, result in
            switch result {
            case .success(let record):
                data.append(record)
            case .failure(let error):
                self.config.log?(.error, "error loading record: \(error.localizedDescription)")
            }
        }
        
        var queryCompletion: Optional<(Result<CKQueryOperation.Cursor?, Error>) -> Void> = nil
        queryCompletion = { result in
            let data = data.map { $0 }
            let queryCompletion = queryCompletion
            
            Task { @MainActor in
                guard case .success(let cursor) = result else {
                    onComplete(data)
                    return
                }
                
                if let cursor = cursor, limit == nil || limit! < data.count {
                    let nextOperation = CKQueryOperation(cursor: cursor)
                    nextOperation.queryResultBlock = queryCompletion
                    nextOperation.recordMatchedBlock = recordMatchedBlock
                    
                    self.container.publicCloudDatabase.add(nextOperation)
                }
                else {
                    onComplete(data)
                }
            }
        }
        
        operation.queryResultBlock = queryCompletion
        operation.recordMatchedBlock = recordMatchedBlock
        
        self.container.publicCloudDatabase.add(operation)
    }
}

extension CloudKitBackend {
    /// Populate  a CloudKit record with the data from an event.
    private func populateRecord(_ record: CKRecord, for event: KeystoneEvent) throws {
        let data = try JSONEncoder().encode(event.data)
        guard let json = String(data: data, encoding: .utf8) else {
            throw EncodingError.invalidValue(
                event.data, .init(codingPath: [], debugDescription: "encoding event data failed!"))
        }
        
        record[self.userIdColumnName] = config.userIdentifier
        record["category"] = event.category
        record["eventData"] = json
        record["eventDate"] = event.date
    }
    
    /// Decode a record.
    private func decodeRecord(record: CKRecord) throws -> KeystoneEvent {
        guard let id = UUID(uuidString: record.recordID.recordName) else {
            throw DecodingError.dataCorrupted(
                DecodingError.Context(
                    codingPath: [],
                    debugDescription: "KeystoneEvent has malformed UUID: \(record.recordID.recordName)"
                )
            )
        }
        
        guard let userId = record[self.userIdColumnName] as? String else {
            throw DecodingError.dataCorrupted(
                DecodingError.Context(
                    codingPath: [],
                    debugDescription: "KeystoneEvent is missing userId"
                )
            )
        }
        
        guard let category = record["category"] as? String else {
            throw DecodingError.dataCorrupted(
                DecodingError.Context(
                    codingPath: [],
                    debugDescription: "KeystoneEvent is missing category"
                )
            )
        }
        
        guard let eventDataJson = record["eventData"] as? String else {
            throw DecodingError.dataCorrupted(
                DecodingError.Context(
                    codingPath: [],
                    debugDescription: "KeystoneEvent is missing eventData"
                )
            )
        }
        
        guard let eventData = eventDataJson.data(using: .utf8) else {
            throw DecodingError.dataCorrupted(
                DecodingError.Context(
                    codingPath: [],
                    debugDescription: "KeystoneEvent eventData is not valid utf8"
                )
            )
        }
        
        guard let eventDate = record["eventDate"] as? Date else {
            throw DecodingError.dataCorrupted(
                DecodingError.Context(
                    codingPath: [],
                    debugDescription: "KeystoneEvent is missing eventDate"
                )
            )
        }
        
        let data = try JSONDecoder().decode([String: KeystoneEventData].self, from: eventData)
        return KeystoneEvent(id: id,
                             userId: userId,
                             category: category,
                             date: eventDate,
                             data: data)
    }
}

// MARK: MockBackend

public final class MockBackend {
    /// Default initializer.
    public init() { }
}

extension MockBackend: KeystoneBackend {
    /// Persist an event ot the backend.
    public func persist(event: KeystoneEvent) async throws { }
    
    /// Load events starting from a date.
    public func loadEvents(in interval: DateInterval,
                           updateStatus: @escaping (BackendStatus) -> Void) async throws -> [KeystoneEvent] {
        return []
    }
}
