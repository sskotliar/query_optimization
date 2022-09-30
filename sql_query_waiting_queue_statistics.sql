DECLARE @transactionsList TABLE (
    id UNIQUEIDENTIFIER, 
    currentAssignedQueueId UNIQUEIDENTIFIER, 
    queueAccessPointId UNIQUEIDENTIFIER, 
    queueName VARCHAR(100), 
    queueReportCategory VARCHAR(100), 
    queuePriority INT, 
    queueOrganizationHierarchyId UNIQUEIDENTIFIER,
    receivedDate DATE,
    claimedDT DATE,
    transactionStatus VARCHAR(100),
    receivedDateUTC DATE,
    appointmentDT DATE,
    createdDT DATE)


-- 41288dea-50b0-48c2-9757-b4b86c888212
INSERT INTO @transactionsList
SELECT
    Transactions.id,
    Transactions.currentAssignedQueueId, 
    Queues.accessPointId as queueAccessPointId,
    Queues.name as queueName,
    Queues.reportCategory as queueReportCategory,
    Queues.priority as queuePriority,
    Queues.organizationHierarchyId as queueOrganizationHierarchyId,
    Transactions.receivedDate,
    Transactions.claimedDT,
    Transactions.transactionStatus,
    Transactions.receivedDateUTC,
    Transactions.appointmentDT,
    Transactions.createdDT
FROM Transactions 
LEFT JOIN Queues ON Transactions.currentAssignedQueueId = Queues.Id
WHERE Transactions.consumerId = '41288dea-50b0-48c2-9757-b4b86c888212'
    AND Transactions.isActive = 1
    AND Transactions.receivedDate <= '2022-06-30T00:00:00'
    AND (Transactions.claimedDT >= '2022-06-02T00:00:00' OR Transactions.transactionStatus = 'WaitingAssignment')


--SELECT COUNT(*) FROM @transactionsList

-- 2022-06-01
SELECT
    '2022-06-01' as reportDate,
    waitTimeSubQuery.currentAssignedQueueId,
    waitTimeSubQuery.queueAccessPointId,
    waitTimeSubQuery.queueName AS QueueName,
    waitTimeSubQuery.queueReportCategory,
    waitTimeSubQuery.queuePriority,
    waitTimeSubQuery.queueOrganizationHierarchyId,
    COUNT(waitTimeSubQuery.id) AS totalCasesWaiting,
    SUM(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS sumWaitTimeMinutes,
    MAX(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS maxWaitTimeMinutes
FROM (SELECT
    id,
    currentAssignedQueueId,
    queueAccessPointId,
    queueName,
    queueReportCategory,
    queuePriority,
    queueOrganizationHierarchyId,
(CASE WHEN (receivedDateUTC > '0001-01-01T00:00:00Z' OR receivedDate > '0001-01-01T00:00:00Z') AND (appointmentDT IS NULL OR appointmentDT < '2022-09-28T12:58:47')

THEN CAST(DateDiff(MINUTE,
    CASE WHEN receivedDateUTC > '0001-01-01T00:00:00Z'
    THEN CONCAT(SUBSTRING(CAST(receivedDateUTC AS VARCHAR), 0, 19), 'Z')
    ELSE TRY_CAST(CONCAT(CONCAT(SUBSTRING(CAST(receivedDate AS VARCHAR), 0, 10), SUBSTRING(CAST(createdDT AS VARCHAR), 10, 9)), 'Z') AS DATETIME2) END,
    CASE WHEN claimedDT > '0001-01-01T00:00:00Z' AND claimedDT < '2022-06-01T23:59:00Z' AND transactionStatus != 'WaitingAssignment'
    THEN CONCAT(SUBSTRING(CAST(claimedDT AS VARCHAR), 0, 19), 'Z')
    ELSE '2022-06-01T23:59:00Z' END
) AS BIGINT)
ELSE 0 END) AS waitTimeMinutes
FROM @transactionsList
WHERE receivedDate <= '2022-06-01T00:00:00'
AND (claimedDT >= '2022-06-02T00:00:00' OR transactionStatus = 'WaitingAssignment')) waitTimeSubQuery
GROUP BY waitTimeSubQuery.currentAssignedQueueId, 
    waitTimeSubQuery.queueAccessPointId, 
    waitTimeSubQuery.queueName, 
    waitTimeSubQuery.queueReportCategory, 
    waitTimeSubQuery.queuePriority, 
    waitTimeSubQuery.queueOrganizationHierarchyId

--UNION ALL

-- 2022-06-02
SELECT
    '2022-06-02' as reportDate,
    waitTimeSubQuery.currentAssignedQueueId,
    waitTimeSubQuery.queueAccessPointId,
    waitTimeSubQuery.queueName AS QueueName,
    waitTimeSubQuery.queueReportCategory,
    waitTimeSubQuery.queuePriority,
    waitTimeSubQuery.queueOrganizationHierarchyId,
    COUNT(waitTimeSubQuery.id) AS totalCasesWaiting,
    SUM(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS sumWaitTimeMinutes,
    MAX(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS maxWaitTimeMinutes
FROM (SELECT
    id,
    currentAssignedQueueId,
    queueAccessPointId,
    queueName,
    queueReportCategory,
    queuePriority,
    queueOrganizationHierarchyId,
(CASE WHEN (receivedDateUTC > '0001-01-01T00:00:00Z' OR receivedDate > '0001-01-01T00:00:00Z') AND (appointmentDT IS NULL OR appointmentDT < '2022-09-28T12:58:47')

THEN CAST(DateDiff(MINUTE,
    CASE WHEN receivedDateUTC > '0001-01-01T00:00:00Z'
    THEN CONCAT(SUBSTRING(CAST(receivedDateUTC AS VARCHAR), 0, 19), 'Z')
    ELSE TRY_CAST(CONCAT(CONCAT(SUBSTRING(CAST(receivedDate AS VARCHAR), 0, 10), SUBSTRING(CAST(createdDT AS VARCHAR), 10, 9)), 'Z') AS DATETIME2) END,
    CASE WHEN claimedDT > '0001-01-01T00:00:00Z' AND claimedDT < '2022-06-02T23:59:00Z' AND transactionStatus != 'WaitingAssignment'
    THEN CONCAT(SUBSTRING(CAST(claimedDT AS VARCHAR), 0, 19), 'Z')
    ELSE '2022-06-02T23:59:00Z' END
) AS BIGINT)
ELSE 0 END) AS waitTimeMinutes
FROM @transactionsList
WHERE receivedDate <= '2022-06-02T00:00:00'
AND (claimedDT >= '2022-06-03T00:00:00' OR transactionStatus = 'WaitingAssignment')) waitTimeSubQuery
GROUP BY waitTimeSubQuery.currentAssignedQueueId, 
    waitTimeSubQuery.queueAccessPointId, 
    waitTimeSubQuery.queueName, 
    waitTimeSubQuery.queueReportCategory, 
    waitTimeSubQuery.queuePriority, 
    waitTimeSubQuery.queueOrganizationHierarchyId

--UNION ALL

-- 2022-06-03
SELECT
    '2022-06-03' as reportDate,
    waitTimeSubQuery.currentAssignedQueueId,
    waitTimeSubQuery.queueAccessPointId,
    waitTimeSubQuery.queueName AS QueueName,
    waitTimeSubQuery.queueReportCategory,
    waitTimeSubQuery.queuePriority,
    waitTimeSubQuery.queueOrganizationHierarchyId,
    COUNT(waitTimeSubQuery.id) AS totalCasesWaiting,
    SUM(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS sumWaitTimeMinutes,
    MAX(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS maxWaitTimeMinutes
FROM (SELECT
    id,
    currentAssignedQueueId,
    queueAccessPointId,
    queueName,
    queueReportCategory,
    queuePriority,
    queueOrganizationHierarchyId,
(CASE WHEN (receivedDateUTC > '0001-01-01T00:00:00Z' OR receivedDate > '0001-01-01T00:00:00Z') AND (appointmentDT IS NULL OR appointmentDT < '2022-09-28T12:58:47')

THEN CAST(DateDiff(MINUTE,
    CASE WHEN receivedDateUTC > '0001-01-01T00:00:00Z'
    THEN CONCAT(SUBSTRING(CAST(receivedDateUTC AS VARCHAR), 0, 19), 'Z')
    ELSE TRY_CAST(CONCAT(CONCAT(SUBSTRING(CAST(receivedDate AS VARCHAR), 0, 10), SUBSTRING(CAST(createdDT AS VARCHAR), 10, 9)), 'Z') AS DATETIME2) END,
    CASE WHEN claimedDT > '0001-01-01T00:00:00Z' AND claimedDT < '2022-06-03T23:59:00Z' AND transactionStatus != 'WaitingAssignment'
    THEN CONCAT(SUBSTRING(CAST(claimedDT AS VARCHAR), 0, 19), 'Z')
    ELSE '2022-06-03T23:59:00Z' END
) AS BIGINT)
ELSE 0 END) AS waitTimeMinutes
FROM @transactionsList
WHERE receivedDate <= '2022-06-03T00:00:00'
AND (claimedDT >= '2022-06-04T00:00:00' OR transactionStatus = 'WaitingAssignment')) waitTimeSubQuery
GROUP BY waitTimeSubQuery.currentAssignedQueueId, 
    waitTimeSubQuery.queueAccessPointId, 
    waitTimeSubQuery.queueName, 
    waitTimeSubQuery.queueReportCategory, 
    waitTimeSubQuery.queuePriority, 
    waitTimeSubQuery.queueOrganizationHierarchyId


--UNION ALL

-- 2022-06-04
SELECT
    '2022-06-04' as reportDate,
    waitTimeSubQuery.currentAssignedQueueId,
    waitTimeSubQuery.queueAccessPointId,
    waitTimeSubQuery.queueName AS QueueName,
    waitTimeSubQuery.queueReportCategory,
    waitTimeSubQuery.queuePriority,
    waitTimeSubQuery.queueOrganizationHierarchyId,
    COUNT(waitTimeSubQuery.id) AS totalCasesWaiting,
    SUM(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS sumWaitTimeMinutes,
    MAX(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS maxWaitTimeMinutes
FROM (SELECT
    id,
    currentAssignedQueueId,
    queueAccessPointId,
    queueName,
    queueReportCategory,
    queuePriority,
    queueOrganizationHierarchyId,
(CASE WHEN (receivedDateUTC > '0001-01-01T00:00:00Z' OR receivedDate > '0001-01-01T00:00:00Z') AND (appointmentDT IS NULL OR appointmentDT < '2022-09-28T12:58:47')

THEN CAST(DateDiff(MINUTE,
    CASE WHEN receivedDateUTC > '0001-01-01T00:00:00Z'
    THEN CONCAT(SUBSTRING(CAST(receivedDateUTC AS VARCHAR), 0, 19), 'Z')
    ELSE TRY_CAST(CONCAT(CONCAT(SUBSTRING(CAST(receivedDate AS VARCHAR), 0, 10), SUBSTRING(CAST(createdDT AS VARCHAR), 10, 9)), 'Z') AS DATETIME2) END,
    CASE WHEN claimedDT > '0001-01-01T00:00:00Z' AND claimedDT < '2022-06-04T23:59:00Z' AND transactionStatus != 'WaitingAssignment'
    THEN CONCAT(SUBSTRING(CAST(claimedDT AS VARCHAR), 0, 19), 'Z')
    ELSE '2022-06-04T23:59:00Z' END
) AS BIGINT)
ELSE 0 END) AS waitTimeMinutes
FROM @transactionsList
WHERE receivedDate <= '2022-06-04T00:00:00'
AND (claimedDT >= '2022-06-05T00:00:00' OR transactionStatus = 'WaitingAssignment')) waitTimeSubQuery
GROUP BY waitTimeSubQuery.currentAssignedQueueId, 
    waitTimeSubQuery.queueAccessPointId, 
    waitTimeSubQuery.queueName, 
    waitTimeSubQuery.queueReportCategory, 
    waitTimeSubQuery.queuePriority, 
    waitTimeSubQuery.queueOrganizationHierarchyId

--UNION ALL

-- 2022-06-05
SELECT
    '2022-06-05' as reportDate,
    waitTimeSubQuery.currentAssignedQueueId,
    waitTimeSubQuery.queueAccessPointId,
    waitTimeSubQuery.queueName AS QueueName,
    waitTimeSubQuery.queueReportCategory,
    waitTimeSubQuery.queuePriority,
    waitTimeSubQuery.queueOrganizationHierarchyId,
    COUNT(waitTimeSubQuery.id) AS totalCasesWaiting,
    SUM(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS sumWaitTimeMinutes,
    MAX(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS maxWaitTimeMinutes
FROM (SELECT
    id,
    currentAssignedQueueId,
    queueAccessPointId,
    queueName,
    queueReportCategory,
    queuePriority,
    queueOrganizationHierarchyId,
(CASE WHEN (receivedDateUTC > '0001-01-01T00:00:00Z' OR receivedDate > '0001-01-01T00:00:00Z') AND (appointmentDT IS NULL OR appointmentDT < '2022-09-28T12:58:47')

THEN CAST(DateDiff(MINUTE,
    CASE WHEN receivedDateUTC > '0001-01-01T00:00:00Z'
    THEN CONCAT(SUBSTRING(CAST(receivedDateUTC AS VARCHAR), 0, 19), 'Z')
    ELSE TRY_CAST(CONCAT(CONCAT(SUBSTRING(CAST(receivedDate AS VARCHAR), 0, 10), SUBSTRING(CAST(createdDT AS VARCHAR), 10, 9)), 'Z') AS DATETIME2) END,
    CASE WHEN claimedDT > '0001-01-01T00:00:00Z' AND claimedDT < '2022-06-05T23:59:00Z' AND transactionStatus != 'WaitingAssignment'
    THEN CONCAT(SUBSTRING(CAST(claimedDT AS VARCHAR), 0, 19), 'Z')
    ELSE '2022-06-05T23:59:00Z' END
) AS BIGINT)
ELSE 0 END) AS waitTimeMinutes
FROM @transactionsList
WHERE receivedDate <= '2022-06-05T00:00:00'
AND (claimedDT >= '2022-06-06T00:00:00' OR transactionStatus = 'WaitingAssignment')) waitTimeSubQuery
GROUP BY waitTimeSubQuery.currentAssignedQueueId, 
    waitTimeSubQuery.queueAccessPointId, 
    waitTimeSubQuery.queueName, 
    waitTimeSubQuery.queueReportCategory, 
    waitTimeSubQuery.queuePriority, 
    waitTimeSubQuery.queueOrganizationHierarchyId

--UNION ALL

-- 2022-06-06
SELECT
    '2022-06-06' as reportDate,
    waitTimeSubQuery.currentAssignedQueueId,
    waitTimeSubQuery.queueAccessPointId,
    waitTimeSubQuery.queueName AS QueueName,
    waitTimeSubQuery.queueReportCategory,
    waitTimeSubQuery.queuePriority,
    waitTimeSubQuery.queueOrganizationHierarchyId,
    COUNT(waitTimeSubQuery.id) AS totalCasesWaiting,
    SUM(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS sumWaitTimeMinutes,
    MAX(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS maxWaitTimeMinutes
FROM (SELECT
    id,
    currentAssignedQueueId,
    queueAccessPointId,
    queueName,
    queueReportCategory,
    queuePriority,
    queueOrganizationHierarchyId,
(CASE WHEN (receivedDateUTC > '0001-01-01T00:00:00Z' OR receivedDate > '0001-01-01T00:00:00Z') AND (appointmentDT IS NULL OR appointmentDT < '2022-09-28T12:58:47')

THEN CAST(DateDiff(MINUTE,
    CASE WHEN receivedDateUTC > '0001-01-01T00:00:00Z'
    THEN CONCAT(SUBSTRING(CAST(receivedDateUTC AS VARCHAR), 0, 19), 'Z')
    ELSE TRY_CAST(CONCAT(CONCAT(SUBSTRING(CAST(receivedDate AS VARCHAR), 0, 10), SUBSTRING(CAST(createdDT AS VARCHAR), 10, 9)), 'Z') AS DATETIME2) END,
    CASE WHEN claimedDT > '0001-01-01T00:00:00Z' AND claimedDT < '2022-06-06T23:59:00Z' AND transactionStatus != 'WaitingAssignment'
    THEN CONCAT(SUBSTRING(CAST(claimedDT AS VARCHAR), 0, 19), 'Z')
    ELSE '2022-06-06T23:59:00Z' END
) AS BIGINT)
ELSE 0 END) AS waitTimeMinutes
FROM @transactionsList
WHERE receivedDate <= '2022-06-06T00:00:00'
AND (claimedDT >= '2022-06-07T00:00:00' OR transactionStatus = 'WaitingAssignment')) waitTimeSubQuery
GROUP BY waitTimeSubQuery.currentAssignedQueueId, 
    waitTimeSubQuery.queueAccessPointId, 
    waitTimeSubQuery.queueName, 
    waitTimeSubQuery.queueReportCategory, 
    waitTimeSubQuery.queuePriority, 
    waitTimeSubQuery.queueOrganizationHierarchyId

--UNION ALL

-- 2022-06-07
SELECT
    '2022-06-07' as reportDate,
    waitTimeSubQuery.currentAssignedQueueId,
    waitTimeSubQuery.queueAccessPointId,
    waitTimeSubQuery.queueName AS QueueName,
    waitTimeSubQuery.queueReportCategory,
    waitTimeSubQuery.queuePriority,
    waitTimeSubQuery.queueOrganizationHierarchyId,
    COUNT(waitTimeSubQuery.id) AS totalCasesWaiting,
    SUM(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS sumWaitTimeMinutes,
    MAX(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS maxWaitTimeMinutes
FROM (SELECT
    id,
    currentAssignedQueueId,
    queueAccessPointId,
    queueName,
    queueReportCategory,
    queuePriority,
    queueOrganizationHierarchyId,
(CASE WHEN (receivedDateUTC > '0001-01-01T00:00:00Z' OR receivedDate > '0001-01-01T00:00:00Z') AND (appointmentDT IS NULL OR appointmentDT < '2022-09-28T12:58:47')

THEN CAST(DateDiff(MINUTE,
    CASE WHEN receivedDateUTC > '0001-01-01T00:00:00Z'
    THEN CONCAT(SUBSTRING(CAST(receivedDateUTC AS VARCHAR), 0, 19), 'Z')
    ELSE TRY_CAST(CONCAT(CONCAT(SUBSTRING(CAST(receivedDate AS VARCHAR), 0, 10), SUBSTRING(CAST(createdDT AS VARCHAR), 10, 9)), 'Z') AS DATETIME2) END,
    CASE WHEN claimedDT > '0001-01-01T00:00:00Z' AND claimedDT < '2022-06-07T23:59:00Z' AND transactionStatus != 'WaitingAssignment'
    THEN CONCAT(SUBSTRING(CAST(claimedDT AS VARCHAR), 0, 19), 'Z')
    ELSE '2022-06-07T23:59:00Z' END
) AS BIGINT)
ELSE 0 END) AS waitTimeMinutes
FROM @transactionsList
WHERE receivedDate <= '2022-06-07T00:00:00'
AND (claimedDT >= '2022-06-08T00:00:00' OR transactionStatus = 'WaitingAssignment')) waitTimeSubQuery
GROUP BY waitTimeSubQuery.currentAssignedQueueId, 
    waitTimeSubQuery.queueAccessPointId, 
    waitTimeSubQuery.queueName, 
    waitTimeSubQuery.queueReportCategory, 
    waitTimeSubQuery.queuePriority, 
    waitTimeSubQuery.queueOrganizationHierarchyId

--UNION ALL

-- 2022-06-08
SELECT
    '2022-06-08' as reportDate,
    waitTimeSubQuery.currentAssignedQueueId,
    waitTimeSubQuery.queueAccessPointId,
    waitTimeSubQuery.queueName AS QueueName,
    waitTimeSubQuery.queueReportCategory,
    waitTimeSubQuery.queuePriority,
    waitTimeSubQuery.queueOrganizationHierarchyId,
    COUNT(waitTimeSubQuery.id) AS totalCasesWaiting,
    SUM(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS sumWaitTimeMinutes,
    MAX(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS maxWaitTimeMinutes
FROM (SELECT
    id,
    currentAssignedQueueId,
    queueAccessPointId,
    queueName,
    queueReportCategory,
    queuePriority,
    queueOrganizationHierarchyId,
(CASE WHEN (receivedDateUTC > '0001-01-01T00:00:00Z' OR receivedDate > '0001-01-01T00:00:00Z') AND (appointmentDT IS NULL OR appointmentDT < '2022-09-28T12:58:47')

THEN CAST(DateDiff(MINUTE,
    CASE WHEN receivedDateUTC > '0001-01-01T00:00:00Z'
    THEN CONCAT(SUBSTRING(CAST(receivedDateUTC AS VARCHAR), 0, 19), 'Z')
    ELSE TRY_CAST(CONCAT(CONCAT(SUBSTRING(CAST(receivedDate AS VARCHAR), 0, 10), SUBSTRING(CAST(createdDT AS VARCHAR), 10, 9)), 'Z') AS DATETIME2) END,
    CASE WHEN claimedDT > '0001-01-01T00:00:00Z' AND claimedDT < '2022-06-08T23:59:00Z' AND transactionStatus != 'WaitingAssignment'
    THEN CONCAT(SUBSTRING(CAST(claimedDT AS VARCHAR), 0, 19), 'Z')
    ELSE '2022-06-08T23:59:00Z' END
) AS BIGINT)
ELSE 0 END) AS waitTimeMinutes
FROM @transactionsList
WHERE receivedDate <= '2022-06-08T00:00:00'
AND (claimedDT >= '2022-06-09T00:00:00' OR transactionStatus = 'WaitingAssignment')) waitTimeSubQuery
GROUP BY waitTimeSubQuery.currentAssignedQueueId, 
    waitTimeSubQuery.queueAccessPointId, 
    waitTimeSubQuery.queueName, 
    waitTimeSubQuery.queueReportCategory, 
    waitTimeSubQuery.queuePriority, 
    waitTimeSubQuery.queueOrganizationHierarchyId

--UNION ALL

-- 2022-06-09
SELECT
    '2022-06-09' as reportDate,
    waitTimeSubQuery.currentAssignedQueueId,
    waitTimeSubQuery.queueAccessPointId,
    waitTimeSubQuery.queueName AS QueueName,
    waitTimeSubQuery.queueReportCategory,
    waitTimeSubQuery.queuePriority,
    waitTimeSubQuery.queueOrganizationHierarchyId,
    COUNT(waitTimeSubQuery.id) AS totalCasesWaiting,
    SUM(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS sumWaitTimeMinutes,
    MAX(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS maxWaitTimeMinutes
FROM (SELECT
    id,
    currentAssignedQueueId,
    queueAccessPointId,
    queueName,
    queueReportCategory,
    queuePriority,
    queueOrganizationHierarchyId,
(CASE WHEN (receivedDateUTC > '0001-01-01T00:00:00Z' OR receivedDate > '0001-01-01T00:00:00Z') AND (appointmentDT IS NULL OR appointmentDT < '2022-09-28T12:58:47')

THEN CAST(DateDiff(MINUTE,
    CASE WHEN receivedDateUTC > '0001-01-01T00:00:00Z'
    THEN CONCAT(SUBSTRING(CAST(receivedDateUTC AS VARCHAR), 0, 19), 'Z')
    ELSE TRY_CAST(CONCAT(CONCAT(SUBSTRING(CAST(receivedDate AS VARCHAR), 0, 10), SUBSTRING(CAST(createdDT AS VARCHAR), 10, 9)), 'Z') AS DATETIME2) END,
    CASE WHEN claimedDT > '0001-01-01T00:00:00Z' AND claimedDT < '2022-06-09T23:59:00Z' AND transactionStatus != 'WaitingAssignment'
    THEN CONCAT(SUBSTRING(CAST(claimedDT AS VARCHAR), 0, 19), 'Z')
    ELSE '2022-06-09T23:59:00Z' END
) AS BIGINT)
ELSE 0 END) AS waitTimeMinutes
FROM @transactionsList
WHERE receivedDate <= '2022-06-09T00:00:00'
AND (claimedDT >= '2022-06-10T00:00:00' OR transactionStatus = 'WaitingAssignment')) waitTimeSubQuery
GROUP BY waitTimeSubQuery.currentAssignedQueueId, 
    waitTimeSubQuery.queueAccessPointId, 
    waitTimeSubQuery.queueName, 
    waitTimeSubQuery.queueReportCategory, 
    waitTimeSubQuery.queuePriority, 
    waitTimeSubQuery.queueOrganizationHierarchyId

--UNION ALL

-- 2022-06-10
SELECT
    '2022-06-10' as reportDate,
    waitTimeSubQuery.currentAssignedQueueId,
    waitTimeSubQuery.queueAccessPointId,
    waitTimeSubQuery.queueName AS QueueName,
    waitTimeSubQuery.queueReportCategory,
    waitTimeSubQuery.queuePriority,
    waitTimeSubQuery.queueOrganizationHierarchyId,
    COUNT(waitTimeSubQuery.id) AS totalCasesWaiting,
    SUM(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS sumWaitTimeMinutes,
    MAX(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS maxWaitTimeMinutes
FROM (SELECT
    id,
    currentAssignedQueueId,
    queueAccessPointId,
    queueName,
    queueReportCategory,
    queuePriority,
    queueOrganizationHierarchyId,
(CASE WHEN (receivedDateUTC > '0001-01-01T00:00:00Z' OR receivedDate > '0001-01-01T00:00:00Z') AND (appointmentDT IS NULL OR appointmentDT < '2022-09-28T12:58:47')

THEN CAST(DateDiff(MINUTE,
    CASE WHEN receivedDateUTC > '0001-01-01T00:00:00Z'
    THEN CONCAT(SUBSTRING(CAST(receivedDateUTC AS VARCHAR), 0, 19), 'Z')
    ELSE TRY_CAST(CONCAT(CONCAT(SUBSTRING(CAST(receivedDate AS VARCHAR), 0, 10), SUBSTRING(CAST(createdDT AS VARCHAR), 10, 9)), 'Z') AS DATETIME2) END,
    CASE WHEN claimedDT > '0001-01-01T00:00:00Z' AND claimedDT < '2022-06-10T23:59:00Z' AND transactionStatus != 'WaitingAssignment'
    THEN CONCAT(SUBSTRING(CAST(claimedDT AS VARCHAR), 0, 19), 'Z')
    ELSE '2022-06-10T23:59:00Z' END
) AS BIGINT)
ELSE 0 END) AS waitTimeMinutes
FROM @transactionsList
WHERE receivedDate <= '2022-06-10T00:00:00'
AND (claimedDT >= '2022-06-11T00:00:00' OR transactionStatus = 'WaitingAssignment')) waitTimeSubQuery
GROUP BY waitTimeSubQuery.currentAssignedQueueId, 
    waitTimeSubQuery.queueAccessPointId, 
    waitTimeSubQuery.queueName, 
    waitTimeSubQuery.queueReportCategory, 
    waitTimeSubQuery.queuePriority, 
    waitTimeSubQuery.queueOrganizationHierarchyId

--UNION ALL

-- 2022-06-11
SELECT
    '2022-06-11' as reportDate,
    waitTimeSubQuery.currentAssignedQueueId,
    waitTimeSubQuery.queueAccessPointId,
    waitTimeSubQuery.queueName AS QueueName,
    waitTimeSubQuery.queueReportCategory,
    waitTimeSubQuery.queuePriority,
    waitTimeSubQuery.queueOrganizationHierarchyId,
    COUNT(waitTimeSubQuery.id) AS totalCasesWaiting,
    SUM(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS sumWaitTimeMinutes,
    MAX(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS maxWaitTimeMinutes
FROM (SELECT
    id,
    currentAssignedQueueId,
    queueAccessPointId,
    queueName,
    queueReportCategory,
    queuePriority,
    queueOrganizationHierarchyId,
(CASE WHEN (receivedDateUTC > '0001-01-01T00:00:00Z' OR receivedDate > '0001-01-01T00:00:00Z') AND (appointmentDT IS NULL OR appointmentDT < '2022-09-28T12:58:47')

THEN CAST(DateDiff(MINUTE,
    CASE WHEN receivedDateUTC > '0001-01-01T00:00:00Z'
    THEN CONCAT(SUBSTRING(CAST(receivedDateUTC AS VARCHAR), 0, 19), 'Z')
    ELSE TRY_CAST(CONCAT(CONCAT(SUBSTRING(CAST(receivedDate AS VARCHAR), 0, 10), SUBSTRING(CAST(createdDT AS VARCHAR), 10, 9)), 'Z') AS DATETIME2) END,
    CASE WHEN claimedDT > '0001-01-01T00:00:00Z' AND claimedDT < '2022-06-11T23:59:00Z' AND transactionStatus != 'WaitingAssignment'
    THEN CONCAT(SUBSTRING(CAST(claimedDT AS VARCHAR), 0, 19), 'Z')
    ELSE '2022-06-11T23:59:00Z' END
) AS BIGINT)
ELSE 0 END) AS waitTimeMinutes
FROM @transactionsList
WHERE receivedDate <= '2022-06-11T00:00:00'
AND (claimedDT >= '2022-06-12T00:00:00' OR transactionStatus = 'WaitingAssignment')) waitTimeSubQuery
GROUP BY waitTimeSubQuery.currentAssignedQueueId, 
    waitTimeSubQuery.queueAccessPointId, 
    waitTimeSubQuery.queueName, 
    waitTimeSubQuery.queueReportCategory, 
    waitTimeSubQuery.queuePriority, 
    waitTimeSubQuery.queueOrganizationHierarchyId

--UNION ALL

-- 2022-06-12
SELECT
    '2022-06-12' as reportDate,
    waitTimeSubQuery.currentAssignedQueueId,
    waitTimeSubQuery.queueAccessPointId,
    waitTimeSubQuery.queueName AS QueueName,
    waitTimeSubQuery.queueReportCategory,
    waitTimeSubQuery.queuePriority,
    waitTimeSubQuery.queueOrganizationHierarchyId,
    COUNT(waitTimeSubQuery.id) AS totalCasesWaiting,
    SUM(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS sumWaitTimeMinutes,
    MAX(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS maxWaitTimeMinutes
FROM (SELECT
    id,
    currentAssignedQueueId,
    queueAccessPointId,
    queueName,
    queueReportCategory,
    queuePriority,
    queueOrganizationHierarchyId,
(CASE WHEN (receivedDateUTC > '0001-01-01T00:00:00Z' OR receivedDate > '0001-01-01T00:00:00Z') AND (appointmentDT IS NULL OR appointmentDT < '2022-09-28T12:58:47')

THEN CAST(DateDiff(MINUTE,
    CASE WHEN receivedDateUTC > '0001-01-01T00:00:00Z'
    THEN CONCAT(SUBSTRING(CAST(receivedDateUTC AS VARCHAR), 0, 19), 'Z')
    ELSE TRY_CAST(CONCAT(CONCAT(SUBSTRING(CAST(receivedDate AS VARCHAR), 0, 10), SUBSTRING(CAST(createdDT AS VARCHAR), 10, 9)), 'Z') AS DATETIME2) END,
    CASE WHEN claimedDT > '0001-01-01T00:00:00Z' AND claimedDT < '2022-06-12T23:59:00Z' AND transactionStatus != 'WaitingAssignment'
    THEN CONCAT(SUBSTRING(CAST(claimedDT AS VARCHAR), 0, 19), 'Z')
    ELSE '2022-06-12T23:59:00Z' END
) AS BIGINT)
ELSE 0 END) AS waitTimeMinutes
FROM @transactionsList
WHERE receivedDate <= '2022-06-12T00:00:00'
AND (claimedDT >= '2022-06-13T00:00:00' OR transactionStatus = 'WaitingAssignment')) waitTimeSubQuery
GROUP BY waitTimeSubQuery.currentAssignedQueueId, 
    waitTimeSubQuery.queueAccessPointId, 
    waitTimeSubQuery.queueName, 
    waitTimeSubQuery.queueReportCategory, 
    waitTimeSubQuery.queuePriority, 
    waitTimeSubQuery.queueOrganizationHierarchyId

--UNION ALL

-- 2022-06-13
SELECT
    '2022-06-13' as reportDate,
    waitTimeSubQuery.currentAssignedQueueId,
    waitTimeSubQuery.queueAccessPointId,
    waitTimeSubQuery.queueName AS QueueName,
    waitTimeSubQuery.queueReportCategory,
    waitTimeSubQuery.queuePriority,
    waitTimeSubQuery.queueOrganizationHierarchyId,
    COUNT(waitTimeSubQuery.id) AS totalCasesWaiting,
    SUM(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS sumWaitTimeMinutes,
    MAX(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS maxWaitTimeMinutes
FROM (SELECT
    id,
    currentAssignedQueueId,
    queueAccessPointId,
    queueName,
    queueReportCategory,
    queuePriority,
    queueOrganizationHierarchyId,
(CASE WHEN (receivedDateUTC > '0001-01-01T00:00:00Z' OR receivedDate > '0001-01-01T00:00:00Z') AND (appointmentDT IS NULL OR appointmentDT < '2022-09-28T12:58:47')

THEN CAST(DateDiff(MINUTE,
    CASE WHEN receivedDateUTC > '0001-01-01T00:00:00Z'
    THEN CONCAT(SUBSTRING(CAST(receivedDateUTC AS VARCHAR), 0, 19), 'Z')
    ELSE TRY_CAST(CONCAT(CONCAT(SUBSTRING(CAST(receivedDate AS VARCHAR), 0, 10), SUBSTRING(CAST(createdDT AS VARCHAR), 10, 9)), 'Z') AS DATETIME2) END,
    CASE WHEN claimedDT > '0001-01-01T00:00:00Z' AND claimedDT < '2022-06-13T23:59:00Z' AND transactionStatus != 'WaitingAssignment'
    THEN CONCAT(SUBSTRING(CAST(claimedDT AS VARCHAR), 0, 19), 'Z')
    ELSE '2022-06-13T23:59:00Z' END
) AS BIGINT)
ELSE 0 END) AS waitTimeMinutes
FROM @transactionsList
WHERE receivedDate <= '2022-06-13T00:00:00'
AND (claimedDT >= '2022-06-14T00:00:00' OR transactionStatus = 'WaitingAssignment')) waitTimeSubQuery
GROUP BY waitTimeSubQuery.currentAssignedQueueId, 
    waitTimeSubQuery.queueAccessPointId, 
    waitTimeSubQuery.queueName, 
    waitTimeSubQuery.queueReportCategory, 
    waitTimeSubQuery.queuePriority, 
    waitTimeSubQuery.queueOrganizationHierarchyId

--UNION ALL

-- 2022-06-14
SELECT
    '2022-06-14' as reportDate,
    waitTimeSubQuery.currentAssignedQueueId,
    waitTimeSubQuery.queueAccessPointId,
    waitTimeSubQuery.queueName AS QueueName,
    waitTimeSubQuery.queueReportCategory,
    waitTimeSubQuery.queuePriority,
    waitTimeSubQuery.queueOrganizationHierarchyId,
    COUNT(waitTimeSubQuery.id) AS totalCasesWaiting,
    SUM(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS sumWaitTimeMinutes,
    MAX(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS maxWaitTimeMinutes
FROM (SELECT
    id,
    currentAssignedQueueId,
    queueAccessPointId,
    queueName,
    queueReportCategory,
    queuePriority,
    queueOrganizationHierarchyId,
(CASE WHEN (receivedDateUTC > '0001-01-01T00:00:00Z' OR receivedDate > '0001-01-01T00:00:00Z') AND (appointmentDT IS NULL OR appointmentDT < '2022-09-28T12:58:47')

THEN CAST(DateDiff(MINUTE,
    CASE WHEN receivedDateUTC > '0001-01-01T00:00:00Z'
    THEN CONCAT(SUBSTRING(CAST(receivedDateUTC AS VARCHAR), 0, 19), 'Z')
    ELSE TRY_CAST(CONCAT(CONCAT(SUBSTRING(CAST(receivedDate AS VARCHAR), 0, 10), SUBSTRING(CAST(createdDT AS VARCHAR), 10, 9)), 'Z') AS DATETIME2) END,
    CASE WHEN claimedDT > '0001-01-01T00:00:00Z' AND claimedDT < '2022-06-14T23:59:00Z' AND transactionStatus != 'WaitingAssignment'
    THEN CONCAT(SUBSTRING(CAST(claimedDT AS VARCHAR), 0, 19), 'Z')
    ELSE '2022-06-14T23:59:00Z' END
) AS BIGINT)
ELSE 0 END) AS waitTimeMinutes
FROM @transactionsList
WHERE receivedDate <= '2022-06-14T00:00:00'
AND (claimedDT >= '2022-06-15T00:00:00' OR transactionStatus = 'WaitingAssignment')) waitTimeSubQuery
GROUP BY waitTimeSubQuery.currentAssignedQueueId, 
    waitTimeSubQuery.queueAccessPointId, 
    waitTimeSubQuery.queueName, 
    waitTimeSubQuery.queueReportCategory, 
    waitTimeSubQuery.queuePriority, 
    waitTimeSubQuery.queueOrganizationHierarchyId

--UNION ALL

-- 2022-06-15
SELECT
    '2022-06-15' as reportDate,
    waitTimeSubQuery.currentAssignedQueueId,
    waitTimeSubQuery.queueAccessPointId,
    waitTimeSubQuery.queueName AS QueueName,
    waitTimeSubQuery.queueReportCategory,
    waitTimeSubQuery.queuePriority,
    waitTimeSubQuery.queueOrganizationHierarchyId,
    COUNT(waitTimeSubQuery.id) AS totalCasesWaiting,
    SUM(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS sumWaitTimeMinutes,
    MAX(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS maxWaitTimeMinutes
FROM (SELECT
    id,
    currentAssignedQueueId,
    queueAccessPointId,
    queueName,
    queueReportCategory,
    queuePriority,
    queueOrganizationHierarchyId,
(CASE WHEN (receivedDateUTC > '0001-01-01T00:00:00Z' OR receivedDate > '0001-01-01T00:00:00Z') AND (appointmentDT IS NULL OR appointmentDT < '2022-09-28T12:58:47')

THEN CAST(DateDiff(MINUTE,
    CASE WHEN receivedDateUTC > '0001-01-01T00:00:00Z'
    THEN CONCAT(SUBSTRING(CAST(receivedDateUTC AS VARCHAR), 0, 19), 'Z')
    ELSE TRY_CAST(CONCAT(CONCAT(SUBSTRING(CAST(receivedDate AS VARCHAR), 0, 10), SUBSTRING(CAST(createdDT AS VARCHAR), 10, 9)), 'Z') AS DATETIME2) END,
    CASE WHEN claimedDT > '0001-01-01T00:00:00Z' AND claimedDT < '2022-06-15T23:59:00Z' AND transactionStatus != 'WaitingAssignment'
    THEN CONCAT(SUBSTRING(CAST(claimedDT AS VARCHAR), 0, 19), 'Z')
    ELSE '2022-06-15T23:59:00Z' END
) AS BIGINT)
ELSE 0 END) AS waitTimeMinutes
FROM @transactionsList
WHERE receivedDate <= '2022-06-15T00:00:00'
AND (claimedDT >= '2022-06-16T00:00:00' OR transactionStatus = 'WaitingAssignment')) waitTimeSubQuery
GROUP BY waitTimeSubQuery.currentAssignedQueueId, 
    waitTimeSubQuery.queueAccessPointId, 
    waitTimeSubQuery.queueName, 
    waitTimeSubQuery.queueReportCategory, 
    waitTimeSubQuery.queuePriority, 
    waitTimeSubQuery.queueOrganizationHierarchyId

--UNION ALL

-- 2022-06-16
SELECT
    '2022-06-16' as reportDate,
    waitTimeSubQuery.currentAssignedQueueId,
    waitTimeSubQuery.queueAccessPointId,
    waitTimeSubQuery.queueName AS QueueName,
    waitTimeSubQuery.queueReportCategory,
    waitTimeSubQuery.queuePriority,
    waitTimeSubQuery.queueOrganizationHierarchyId,
    COUNT(waitTimeSubQuery.id) AS totalCasesWaiting,
    SUM(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS sumWaitTimeMinutes,
    MAX(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS maxWaitTimeMinutes
FROM (SELECT
    id,
    currentAssignedQueueId,
    queueAccessPointId,
    queueName,
    queueReportCategory,
    queuePriority,
    queueOrganizationHierarchyId,
(CASE WHEN (receivedDateUTC > '0001-01-01T00:00:00Z' OR receivedDate > '0001-01-01T00:00:00Z') AND (appointmentDT IS NULL OR appointmentDT < '2022-09-28T12:58:47')

THEN CAST(DateDiff(MINUTE,
    CASE WHEN receivedDateUTC > '0001-01-01T00:00:00Z'
    THEN CONCAT(SUBSTRING(CAST(receivedDateUTC AS VARCHAR), 0, 19), 'Z')
    ELSE TRY_CAST(CONCAT(CONCAT(SUBSTRING(CAST(receivedDate AS VARCHAR), 0, 10), SUBSTRING(CAST(createdDT AS VARCHAR), 10, 9)), 'Z') AS DATETIME2) END,
    CASE WHEN claimedDT > '0001-01-01T00:00:00Z' AND claimedDT < '2022-06-16T23:59:00Z' AND transactionStatus != 'WaitingAssignment'
    THEN CONCAT(SUBSTRING(CAST(claimedDT AS VARCHAR), 0, 19), 'Z')
    ELSE '2022-06-16T23:59:00Z' END
) AS BIGINT)
ELSE 0 END) AS waitTimeMinutes
FROM @transactionsList
WHERE receivedDate <= '2022-06-16T00:00:00'
AND (claimedDT >= '2022-06-17T00:00:00' OR transactionStatus = 'WaitingAssignment')) waitTimeSubQuery
GROUP BY waitTimeSubQuery.currentAssignedQueueId, 
    waitTimeSubQuery.queueAccessPointId, 
    waitTimeSubQuery.queueName, 
    waitTimeSubQuery.queueReportCategory, 
    waitTimeSubQuery.queuePriority, 
    waitTimeSubQuery.queueOrganizationHierarchyId

--UNION ALL

-- 2022-06-17
SELECT
    '2022-06-17' as reportDate,
    waitTimeSubQuery.currentAssignedQueueId,
    waitTimeSubQuery.queueAccessPointId,
    waitTimeSubQuery.queueName AS QueueName,
    waitTimeSubQuery.queueReportCategory,
    waitTimeSubQuery.queuePriority,
    waitTimeSubQuery.queueOrganizationHierarchyId,
    COUNT(waitTimeSubQuery.id) AS totalCasesWaiting,
    SUM(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS sumWaitTimeMinutes,
    MAX(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS maxWaitTimeMinutes
FROM (SELECT
    id,
    currentAssignedQueueId,
    queueAccessPointId,
    queueName,
    queueReportCategory,
    queuePriority,
    queueOrganizationHierarchyId,
(CASE WHEN (receivedDateUTC > '0001-01-01T00:00:00Z' OR receivedDate > '0001-01-01T00:00:00Z') AND (appointmentDT IS NULL OR appointmentDT < '2022-09-28T12:58:47')

THEN CAST(DateDiff(MINUTE,
    CASE WHEN receivedDateUTC > '0001-01-01T00:00:00Z'
    THEN CONCAT(SUBSTRING(CAST(receivedDateUTC AS VARCHAR), 0, 19), 'Z')
    ELSE TRY_CAST(CONCAT(CONCAT(SUBSTRING(CAST(receivedDate AS VARCHAR), 0, 10), SUBSTRING(CAST(createdDT AS VARCHAR), 10, 9)), 'Z') AS DATETIME2) END,
    CASE WHEN claimedDT > '0001-01-01T00:00:00Z' AND claimedDT < '2022-06-17T23:59:00Z' AND transactionStatus != 'WaitingAssignment'
    THEN CONCAT(SUBSTRING(CAST(claimedDT AS VARCHAR), 0, 19), 'Z')
    ELSE '2022-06-17T23:59:00Z' END
) AS BIGINT)
ELSE 0 END) AS waitTimeMinutes
FROM @transactionsList
WHERE receivedDate <= '2022-06-17T00:00:00'
AND (claimedDT >= '2022-06-18T00:00:00' OR transactionStatus = 'WaitingAssignment')) waitTimeSubQuery
GROUP BY waitTimeSubQuery.currentAssignedQueueId, 
    waitTimeSubQuery.queueAccessPointId, 
    waitTimeSubQuery.queueName, 
    waitTimeSubQuery.queueReportCategory, 
    waitTimeSubQuery.queuePriority, 
    waitTimeSubQuery.queueOrganizationHierarchyId

--UNION ALL

-- 2022-06-18
SELECT
    '2022-06-18' as reportDate,
    waitTimeSubQuery.currentAssignedQueueId,
    waitTimeSubQuery.queueAccessPointId,
    waitTimeSubQuery.queueName AS QueueName,
    waitTimeSubQuery.queueReportCategory,
    waitTimeSubQuery.queuePriority,
    waitTimeSubQuery.queueOrganizationHierarchyId,
    COUNT(waitTimeSubQuery.id) AS totalCasesWaiting,
    SUM(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS sumWaitTimeMinutes,
    MAX(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS maxWaitTimeMinutes
FROM (SELECT
    id,
    currentAssignedQueueId,
    queueAccessPointId,
    queueName,
    queueReportCategory,
    queuePriority,
    queueOrganizationHierarchyId,
(CASE WHEN (receivedDateUTC > '0001-01-01T00:00:00Z' OR receivedDate > '0001-01-01T00:00:00Z') AND (appointmentDT IS NULL OR appointmentDT < '2022-09-28T12:58:47')

THEN CAST(DateDiff(MINUTE,
    CASE WHEN receivedDateUTC > '0001-01-01T00:00:00Z'
    THEN CONCAT(SUBSTRING(CAST(receivedDateUTC AS VARCHAR), 0, 19), 'Z')
    ELSE TRY_CAST(CONCAT(CONCAT(SUBSTRING(CAST(receivedDate AS VARCHAR), 0, 10), SUBSTRING(CAST(createdDT AS VARCHAR), 10, 9)), 'Z') AS DATETIME2) END,
    CASE WHEN claimedDT > '0001-01-01T00:00:00Z' AND claimedDT < '2022-06-18T23:59:00Z' AND transactionStatus != 'WaitingAssignment'
    THEN CONCAT(SUBSTRING(CAST(claimedDT AS VARCHAR), 0, 19), 'Z')
    ELSE '2022-06-18T23:59:00Z' END
) AS BIGINT)
ELSE 0 END) AS waitTimeMinutes
FROM @transactionsList
WHERE receivedDate <= '2022-06-18T00:00:00'
AND (claimedDT >= '2022-06-19T00:00:00' OR transactionStatus = 'WaitingAssignment')) waitTimeSubQuery
GROUP BY waitTimeSubQuery.currentAssignedQueueId, 
    waitTimeSubQuery.queueAccessPointId, 
    waitTimeSubQuery.queueName, 
    waitTimeSubQuery.queueReportCategory, 
    waitTimeSubQuery.queuePriority, 
    waitTimeSubQuery.queueOrganizationHierarchyId

--UNION ALL

-- 2022-06-19
SELECT
    '2022-06-19' as reportDate,
    waitTimeSubQuery.currentAssignedQueueId,
    waitTimeSubQuery.queueAccessPointId,
    waitTimeSubQuery.queueName AS QueueName,
    waitTimeSubQuery.queueReportCategory,
    waitTimeSubQuery.queuePriority,
    waitTimeSubQuery.queueOrganizationHierarchyId,
    COUNT(waitTimeSubQuery.id) AS totalCasesWaiting,
    SUM(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS sumWaitTimeMinutes,
    MAX(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS maxWaitTimeMinutes
FROM (SELECT
    id,
    currentAssignedQueueId,
    queueAccessPointId,
    queueName,
    queueReportCategory,
    queuePriority,
    queueOrganizationHierarchyId,
(CASE WHEN (receivedDateUTC > '0001-01-01T00:00:00Z' OR receivedDate > '0001-01-01T00:00:00Z') AND (appointmentDT IS NULL OR appointmentDT < '2022-09-28T12:58:47')

THEN CAST(DateDiff(MINUTE,
    CASE WHEN receivedDateUTC > '0001-01-01T00:00:00Z'
    THEN CONCAT(SUBSTRING(CAST(receivedDateUTC AS VARCHAR), 0, 19), 'Z')
    ELSE TRY_CAST(CONCAT(CONCAT(SUBSTRING(CAST(receivedDate AS VARCHAR), 0, 10), SUBSTRING(CAST(createdDT AS VARCHAR), 10, 9)), 'Z') AS DATETIME2) END,
    CASE WHEN claimedDT > '0001-01-01T00:00:00Z' AND claimedDT < '2022-06-19T23:59:00Z' AND transactionStatus != 'WaitingAssignment'
    THEN CONCAT(SUBSTRING(CAST(claimedDT AS VARCHAR), 0, 19), 'Z')
    ELSE '2022-06-19T23:59:00Z' END
) AS BIGINT)
ELSE 0 END) AS waitTimeMinutes
FROM @transactionsList
WHERE receivedDate <= '2022-06-19T00:00:00'
AND (claimedDT >= '2022-06-20T00:00:00' OR transactionStatus = 'WaitingAssignment')) waitTimeSubQuery
GROUP BY waitTimeSubQuery.currentAssignedQueueId, 
    waitTimeSubQuery.queueAccessPointId, 
    waitTimeSubQuery.queueName, 
    waitTimeSubQuery.queueReportCategory, 
    waitTimeSubQuery.queuePriority, 
    waitTimeSubQuery.queueOrganizationHierarchyId

--UNION ALL

-- 2022-06-20
SELECT
    '2022-06-20' as reportDate,
    waitTimeSubQuery.currentAssignedQueueId,
    waitTimeSubQuery.queueAccessPointId,
    waitTimeSubQuery.queueName AS QueueName,
    waitTimeSubQuery.queueReportCategory,
    waitTimeSubQuery.queuePriority,
    waitTimeSubQuery.queueOrganizationHierarchyId,
    COUNT(waitTimeSubQuery.id) AS totalCasesWaiting,
    SUM(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS sumWaitTimeMinutes,
    MAX(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS maxWaitTimeMinutes
FROM (SELECT
    id,
    currentAssignedQueueId,
    queueAccessPointId,
    queueName,
    queueReportCategory,
    queuePriority,
    queueOrganizationHierarchyId,
(CASE WHEN (receivedDateUTC > '0001-01-01T00:00:00Z' OR receivedDate > '0001-01-01T00:00:00Z') AND (appointmentDT IS NULL OR appointmentDT < '2022-09-28T12:58:47')

THEN CAST(DateDiff(MINUTE,
    CASE WHEN receivedDateUTC > '0001-01-01T00:00:00Z'
    THEN CONCAT(SUBSTRING(CAST(receivedDateUTC AS VARCHAR), 0, 19), 'Z')
    ELSE TRY_CAST(CONCAT(CONCAT(SUBSTRING(CAST(receivedDate AS VARCHAR), 0, 10), SUBSTRING(CAST(createdDT AS VARCHAR), 10, 9)), 'Z') AS DATETIME2) END,
    CASE WHEN claimedDT > '0001-01-01T00:00:00Z' AND claimedDT < '2022-06-20T23:59:00Z' AND transactionStatus != 'WaitingAssignment'
    THEN CONCAT(SUBSTRING(CAST(claimedDT AS VARCHAR), 0, 19), 'Z')
    ELSE '2022-06-20T23:59:00Z' END
) AS BIGINT)
ELSE 0 END) AS waitTimeMinutes
FROM @transactionsList
WHERE receivedDate <= '2022-06-20T00:00:00'
AND (claimedDT >= '2022-06-21T00:00:00' OR transactionStatus = 'WaitingAssignment')) waitTimeSubQuery
GROUP BY waitTimeSubQuery.currentAssignedQueueId, 
    waitTimeSubQuery.queueAccessPointId, 
    waitTimeSubQuery.queueName, 
    waitTimeSubQuery.queueReportCategory, 
    waitTimeSubQuery.queuePriority, 
    waitTimeSubQuery.queueOrganizationHierarchyId

--UNION ALL

-- 2022-06-21
SELECT
    '2022-06-21' as reportDate,
    waitTimeSubQuery.currentAssignedQueueId,
    waitTimeSubQuery.queueAccessPointId,
    waitTimeSubQuery.queueName AS QueueName,
    waitTimeSubQuery.queueReportCategory,
    waitTimeSubQuery.queuePriority,
    waitTimeSubQuery.queueOrganizationHierarchyId,
    COUNT(waitTimeSubQuery.id) AS totalCasesWaiting,
    SUM(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS sumWaitTimeMinutes,
    MAX(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS maxWaitTimeMinutes
FROM (SELECT
    id,
    currentAssignedQueueId,
    queueAccessPointId,
    queueName,
    queueReportCategory,
    queuePriority,
    queueOrganizationHierarchyId,
(CASE WHEN (receivedDateUTC > '0001-01-01T00:00:00Z' OR receivedDate > '0001-01-01T00:00:00Z') AND (appointmentDT IS NULL OR appointmentDT < '2022-09-28T12:58:47')

THEN CAST(DateDiff(MINUTE,
    CASE WHEN receivedDateUTC > '0001-01-01T00:00:00Z'
    THEN CONCAT(SUBSTRING(CAST(receivedDateUTC AS VARCHAR), 0, 19), 'Z')
    ELSE TRY_CAST(CONCAT(CONCAT(SUBSTRING(CAST(receivedDate AS VARCHAR), 0, 10), SUBSTRING(CAST(createdDT AS VARCHAR), 10, 9)), 'Z') AS DATETIME2) END,
    CASE WHEN claimedDT > '0001-01-01T00:00:00Z' AND claimedDT < '2022-06-21T23:59:00Z' AND transactionStatus != 'WaitingAssignment'
    THEN CONCAT(SUBSTRING(CAST(claimedDT AS VARCHAR), 0, 19), 'Z')
    ELSE '2022-06-21T23:59:00Z' END
) AS BIGINT)
ELSE 0 END) AS waitTimeMinutes
FROM @transactionsList
WHERE receivedDate <= '2022-06-21T00:00:00'
AND (claimedDT >= '2022-06-22T00:00:00' OR transactionStatus = 'WaitingAssignment')) waitTimeSubQuery
GROUP BY waitTimeSubQuery.currentAssignedQueueId, 
    waitTimeSubQuery.queueAccessPointId, 
    waitTimeSubQuery.queueName, 
    waitTimeSubQuery.queueReportCategory, 
    waitTimeSubQuery.queuePriority, 
    waitTimeSubQuery.queueOrganizationHierarchyId

--UNION ALL

-- 2022-06-22
SELECT
    '2022-06-22' as reportDate,
    waitTimeSubQuery.currentAssignedQueueId,
    waitTimeSubQuery.queueAccessPointId,
    waitTimeSubQuery.queueName AS QueueName,
    waitTimeSubQuery.queueReportCategory,
    waitTimeSubQuery.queuePriority,
    waitTimeSubQuery.queueOrganizationHierarchyId,
    COUNT(waitTimeSubQuery.id) AS totalCasesWaiting,
    SUM(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS sumWaitTimeMinutes,
    MAX(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS maxWaitTimeMinutes
FROM (SELECT
    id,
    currentAssignedQueueId,
    queueAccessPointId,
    queueName,
    queueReportCategory,
    queuePriority,
    queueOrganizationHierarchyId,
(CASE WHEN (receivedDateUTC > '0001-01-01T00:00:00Z' OR receivedDate > '0001-01-01T00:00:00Z') AND (appointmentDT IS NULL OR appointmentDT < '2022-09-28T12:58:47')

THEN CAST(DateDiff(MINUTE,
    CASE WHEN receivedDateUTC > '0001-01-01T00:00:00Z'
    THEN CONCAT(SUBSTRING(CAST(receivedDateUTC AS VARCHAR), 0, 19), 'Z')
    ELSE TRY_CAST(CONCAT(CONCAT(SUBSTRING(CAST(receivedDate AS VARCHAR), 0, 10), SUBSTRING(CAST(createdDT AS VARCHAR), 10, 9)), 'Z') AS DATETIME2) END,
    CASE WHEN claimedDT > '0001-01-01T00:00:00Z' AND claimedDT < '2022-06-22T23:59:00Z' AND transactionStatus != 'WaitingAssignment'
    THEN CONCAT(SUBSTRING(CAST(claimedDT AS VARCHAR), 0, 19), 'Z')
    ELSE '2022-06-22T23:59:00Z' END
) AS BIGINT)
ELSE 0 END) AS waitTimeMinutes
FROM @transactionsList
WHERE receivedDate <= '2022-06-22T00:00:00'
AND (claimedDT >= '2022-06-23T00:00:00' OR transactionStatus = 'WaitingAssignment')) waitTimeSubQuery
GROUP BY waitTimeSubQuery.currentAssignedQueueId, 
    waitTimeSubQuery.queueAccessPointId, 
    waitTimeSubQuery.queueName, 
    waitTimeSubQuery.queueReportCategory, 
    waitTimeSubQuery.queuePriority, 
    waitTimeSubQuery.queueOrganizationHierarchyId

--UNION ALL

-- 2022-06-23
SELECT
    '2022-06-23' as reportDate,
    waitTimeSubQuery.currentAssignedQueueId,
    waitTimeSubQuery.queueAccessPointId,
    waitTimeSubQuery.queueName AS QueueName,
    waitTimeSubQuery.queueReportCategory,
    waitTimeSubQuery.queuePriority,
    waitTimeSubQuery.queueOrganizationHierarchyId,
    COUNT(waitTimeSubQuery.id) AS totalCasesWaiting,
    SUM(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS sumWaitTimeMinutes,
    MAX(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS maxWaitTimeMinutes
FROM (SELECT
    id,
    currentAssignedQueueId,
    queueAccessPointId,
    queueName,
    queueReportCategory,
    queuePriority,
    queueOrganizationHierarchyId,
(CASE WHEN (receivedDateUTC > '0001-01-01T00:00:00Z' OR receivedDate > '0001-01-01T00:00:00Z') AND (appointmentDT IS NULL OR appointmentDT < '2022-09-28T12:58:47')

THEN CAST(DateDiff(MINUTE,
    CASE WHEN receivedDateUTC > '0001-01-01T00:00:00Z'
    THEN CONCAT(SUBSTRING(CAST(receivedDateUTC AS VARCHAR), 0, 19), 'Z')
    ELSE TRY_CAST(CONCAT(CONCAT(SUBSTRING(CAST(receivedDate AS VARCHAR), 0, 10), SUBSTRING(CAST(createdDT AS VARCHAR), 10, 9)), 'Z') AS DATETIME2) END,
    CASE WHEN claimedDT > '0001-01-01T00:00:00Z' AND claimedDT < '2022-06-23T23:59:00Z' AND transactionStatus != 'WaitingAssignment'
    THEN CONCAT(SUBSTRING(CAST(claimedDT AS VARCHAR), 0, 19), 'Z')
    ELSE '2022-06-23T23:59:00Z' END
) AS BIGINT)
ELSE 0 END) AS waitTimeMinutes
FROM @transactionsList
WHERE receivedDate <= '2022-06-23T00:00:00'
AND (claimedDT >= '2022-06-24T00:00:00' OR transactionStatus = 'WaitingAssignment')) waitTimeSubQuery
GROUP BY waitTimeSubQuery.currentAssignedQueueId, 
    waitTimeSubQuery.queueAccessPointId, 
    waitTimeSubQuery.queueName, 
    waitTimeSubQuery.queueReportCategory, 
    waitTimeSubQuery.queuePriority, 
    waitTimeSubQuery.queueOrganizationHierarchyId

--UNION ALL

-- 2022-06-24
SELECT
    '2022-06-24' as reportDate,
    waitTimeSubQuery.currentAssignedQueueId,
    waitTimeSubQuery.queueAccessPointId,
    waitTimeSubQuery.queueName AS QueueName,
    waitTimeSubQuery.queueReportCategory,
    waitTimeSubQuery.queuePriority,
    waitTimeSubQuery.queueOrganizationHierarchyId,
    COUNT(waitTimeSubQuery.id) AS totalCasesWaiting,
    SUM(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS sumWaitTimeMinutes,
    MAX(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS maxWaitTimeMinutes
FROM (SELECT
    id,
    currentAssignedQueueId,
    queueAccessPointId,
    queueName,
    queueReportCategory,
    queuePriority,
    queueOrganizationHierarchyId,
(CASE WHEN (receivedDateUTC > '0001-01-01T00:00:00Z' OR receivedDate > '0001-01-01T00:00:00Z') AND (appointmentDT IS NULL OR appointmentDT < '2022-09-28T12:58:47')

THEN CAST(DateDiff(MINUTE,
    CASE WHEN receivedDateUTC > '0001-01-01T00:00:00Z'
    THEN CONCAT(SUBSTRING(CAST(receivedDateUTC AS VARCHAR), 0, 19), 'Z')
    ELSE TRY_CAST(CONCAT(CONCAT(SUBSTRING(CAST(receivedDate AS VARCHAR), 0, 10), SUBSTRING(CAST(createdDT AS VARCHAR), 10, 9)), 'Z') AS DATETIME2) END,
    CASE WHEN claimedDT > '0001-01-01T00:00:00Z' AND claimedDT < '2022-06-24T23:59:00Z' AND transactionStatus != 'WaitingAssignment'
    THEN CONCAT(SUBSTRING(CAST(claimedDT AS VARCHAR), 0, 19), 'Z')
    ELSE '2022-06-24T23:59:00Z' END
) AS BIGINT)
ELSE 0 END) AS waitTimeMinutes
FROM @transactionsList
WHERE receivedDate <= '2022-06-24T00:00:00'
AND (claimedDT >= '2022-06-25T00:00:00' OR transactionStatus = 'WaitingAssignment')) waitTimeSubQuery
GROUP BY waitTimeSubQuery.currentAssignedQueueId, 
    waitTimeSubQuery.queueAccessPointId, 
    waitTimeSubQuery.queueName, 
    waitTimeSubQuery.queueReportCategory, 
    waitTimeSubQuery.queuePriority, 
    waitTimeSubQuery.queueOrganizationHierarchyId

--UNION ALL

-- 2022-06-25
SELECT
    '2022-06-25' as reportDate,
    waitTimeSubQuery.currentAssignedQueueId,
    waitTimeSubQuery.queueAccessPointId,
    waitTimeSubQuery.queueName AS QueueName,
    waitTimeSubQuery.queueReportCategory,
    waitTimeSubQuery.queuePriority,
    waitTimeSubQuery.queueOrganizationHierarchyId,
    COUNT(waitTimeSubQuery.id) AS totalCasesWaiting,
    SUM(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS sumWaitTimeMinutes,
    MAX(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS maxWaitTimeMinutes
FROM (SELECT
    id,
    currentAssignedQueueId,
    queueAccessPointId,
    queueName,
    queueReportCategory,
    queuePriority,
    queueOrganizationHierarchyId,
(CASE WHEN (receivedDateUTC > '0001-01-01T00:00:00Z' OR receivedDate > '0001-01-01T00:00:00Z') AND (appointmentDT IS NULL OR appointmentDT < '2022-09-28T12:58:47')

THEN CAST(DateDiff(MINUTE,
    CASE WHEN receivedDateUTC > '0001-01-01T00:00:00Z'
    THEN CONCAT(SUBSTRING(CAST(receivedDateUTC AS VARCHAR), 0, 19), 'Z')
    ELSE TRY_CAST(CONCAT(CONCAT(SUBSTRING(CAST(receivedDate AS VARCHAR), 0, 10), SUBSTRING(CAST(createdDT AS VARCHAR), 10, 9)), 'Z') AS DATETIME2) END,
    CASE WHEN claimedDT > '0001-01-01T00:00:00Z' AND claimedDT < '2022-06-25T23:59:00Z' AND transactionStatus != 'WaitingAssignment'
    THEN CONCAT(SUBSTRING(CAST(claimedDT AS VARCHAR), 0, 19), 'Z')
    ELSE '2022-06-25T23:59:00Z' END
) AS BIGINT)
ELSE 0 END) AS waitTimeMinutes
FROM @transactionsList
WHERE receivedDate <= '2022-06-25T00:00:00'
AND (claimedDT >= '2022-06-26T00:00:00' OR transactionStatus = 'WaitingAssignment')) waitTimeSubQuery
GROUP BY waitTimeSubQuery.currentAssignedQueueId, 
    waitTimeSubQuery.queueAccessPointId, 
    waitTimeSubQuery.queueName, 
    waitTimeSubQuery.queueReportCategory, 
    waitTimeSubQuery.queuePriority, 
    waitTimeSubQuery.queueOrganizationHierarchyId

--UNION ALL

-- 2022-06-26
SELECT
    '2022-06-26' as reportDate,
    waitTimeSubQuery.currentAssignedQueueId,
    waitTimeSubQuery.queueAccessPointId,
    waitTimeSubQuery.queueName AS QueueName,
    waitTimeSubQuery.queueReportCategory,
    waitTimeSubQuery.queuePriority,
    waitTimeSubQuery.queueOrganizationHierarchyId,
    COUNT(waitTimeSubQuery.id) AS totalCasesWaiting,
    SUM(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS sumWaitTimeMinutes,
    MAX(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS maxWaitTimeMinutes
FROM (SELECT
    id,
    currentAssignedQueueId,
    queueAccessPointId,
    queueName,
    queueReportCategory,
    queuePriority,
    queueOrganizationHierarchyId,
(CASE WHEN (receivedDateUTC > '0001-01-01T00:00:00Z' OR receivedDate > '0001-01-01T00:00:00Z') AND (appointmentDT IS NULL OR appointmentDT < '2022-09-28T12:58:47')

THEN CAST(DateDiff(MINUTE,
    CASE WHEN receivedDateUTC > '0001-01-01T00:00:00Z'
    THEN CONCAT(SUBSTRING(CAST(receivedDateUTC AS VARCHAR), 0, 19), 'Z')
    ELSE TRY_CAST(CONCAT(CONCAT(SUBSTRING(CAST(receivedDate AS VARCHAR), 0, 10), SUBSTRING(CAST(createdDT AS VARCHAR), 10, 9)), 'Z') AS DATETIME2) END,
    CASE WHEN claimedDT > '0001-01-01T00:00:00Z' AND claimedDT < '2022-06-26T23:59:00Z' AND transactionStatus != 'WaitingAssignment'
    THEN CONCAT(SUBSTRING(CAST(claimedDT AS VARCHAR), 0, 19), 'Z')
    ELSE '2022-06-26T23:59:00Z' END
) AS BIGINT)
ELSE 0 END) AS waitTimeMinutes
FROM @transactionsList
WHERE receivedDate <= '2022-06-26T00:00:00'
AND (claimedDT >= '2022-06-27T00:00:00' OR transactionStatus = 'WaitingAssignment')) waitTimeSubQuery
GROUP BY waitTimeSubQuery.currentAssignedQueueId, 
    waitTimeSubQuery.queueAccessPointId, 
    waitTimeSubQuery.queueName, 
    waitTimeSubQuery.queueReportCategory, 
    waitTimeSubQuery.queuePriority, 
    waitTimeSubQuery.queueOrganizationHierarchyId

--UNION ALL

-- 2022-06-27
SELECT
    '2022-06-27' as reportDate,
    waitTimeSubQuery.currentAssignedQueueId,
    waitTimeSubQuery.queueAccessPointId,
    waitTimeSubQuery.queueName AS QueueName,
    waitTimeSubQuery.queueReportCategory,
    waitTimeSubQuery.queuePriority,
    waitTimeSubQuery.queueOrganizationHierarchyId,
    COUNT(waitTimeSubQuery.id) AS totalCasesWaiting,
    SUM(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS sumWaitTimeMinutes,
    MAX(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS maxWaitTimeMinutes
FROM (SELECT
    id,
    currentAssignedQueueId,
    queueAccessPointId,
    queueName,
    queueReportCategory,
    queuePriority,
    queueOrganizationHierarchyId,
(CASE WHEN (receivedDateUTC > '0001-01-01T00:00:00Z' OR receivedDate > '0001-01-01T00:00:00Z') AND (appointmentDT IS NULL OR appointmentDT < '2022-09-28T12:58:47')

THEN CAST(DateDiff(MINUTE,
    CASE WHEN receivedDateUTC > '0001-01-01T00:00:00Z'
    THEN CONCAT(SUBSTRING(CAST(receivedDateUTC AS VARCHAR), 0, 19), 'Z')
    ELSE TRY_CAST(CONCAT(CONCAT(SUBSTRING(CAST(receivedDate AS VARCHAR), 0, 10), SUBSTRING(CAST(createdDT AS VARCHAR), 10, 9)), 'Z') AS DATETIME2) END,
    CASE WHEN claimedDT > '0001-01-01T00:00:00Z' AND claimedDT < '2022-06-27T23:59:00Z' AND transactionStatus != 'WaitingAssignment'
    THEN CONCAT(SUBSTRING(CAST(claimedDT AS VARCHAR), 0, 19), 'Z')
    ELSE '2022-06-27T23:59:00Z' END
) AS BIGINT)
ELSE 0 END) AS waitTimeMinutes
FROM @transactionsList
WHERE receivedDate <= '2022-06-27T00:00:00'
AND (claimedDT >= '2022-06-28T00:00:00' OR transactionStatus = 'WaitingAssignment')) waitTimeSubQuery
GROUP BY waitTimeSubQuery.currentAssignedQueueId, 
    waitTimeSubQuery.queueAccessPointId, 
    waitTimeSubQuery.queueName, 
    waitTimeSubQuery.queueReportCategory, 
    waitTimeSubQuery.queuePriority, 
    waitTimeSubQuery.queueOrganizationHierarchyId

--UNION ALL

-- 2022-06-28
SELECT
    '2022-06-28' as reportDate,
    waitTimeSubQuery.currentAssignedQueueId,
    waitTimeSubQuery.queueAccessPointId,
    waitTimeSubQuery.queueName AS QueueName,
    waitTimeSubQuery.queueReportCategory,
    waitTimeSubQuery.queuePriority,
    waitTimeSubQuery.queueOrganizationHierarchyId,
    COUNT(waitTimeSubQuery.id) AS totalCasesWaiting,
    SUM(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS sumWaitTimeMinutes,
    MAX(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS maxWaitTimeMinutes
FROM (SELECT
    id,
    currentAssignedQueueId,
    queueAccessPointId,
    queueName,
    queueReportCategory,
    queuePriority,
    queueOrganizationHierarchyId,
(CASE WHEN (receivedDateUTC > '0001-01-01T00:00:00Z' OR receivedDate > '0001-01-01T00:00:00Z') AND (appointmentDT IS NULL OR appointmentDT < '2022-09-28T12:58:47')

THEN CAST(DateDiff(MINUTE,
    CASE WHEN receivedDateUTC > '0001-01-01T00:00:00Z'
    THEN CONCAT(SUBSTRING(CAST(receivedDateUTC AS VARCHAR), 0, 19), 'Z')
    ELSE TRY_CAST(CONCAT(CONCAT(SUBSTRING(CAST(receivedDate AS VARCHAR), 0, 10), SUBSTRING(CAST(createdDT AS VARCHAR), 10, 9)), 'Z') AS DATETIME2) END,
    CASE WHEN claimedDT > '0001-01-01T00:00:00Z' AND claimedDT < '2022-06-28T23:59:00Z' AND transactionStatus != 'WaitingAssignment'
    THEN CONCAT(SUBSTRING(CAST(claimedDT AS VARCHAR), 0, 19), 'Z')
    ELSE '2022-06-28T23:59:00Z' END
) AS BIGINT)
ELSE 0 END) AS waitTimeMinutes
FROM @transactionsList
WHERE receivedDate <= '2022-06-28T00:00:00'
AND (claimedDT >= '2022-06-29T00:00:00' OR transactionStatus = 'WaitingAssignment')) waitTimeSubQuery
GROUP BY waitTimeSubQuery.currentAssignedQueueId, 
    waitTimeSubQuery.queueAccessPointId, 
    waitTimeSubQuery.queueName, 
    waitTimeSubQuery.queueReportCategory, 
    waitTimeSubQuery.queuePriority, 
    waitTimeSubQuery.queueOrganizationHierarchyId

--UNION ALL

-- 2022-06-29
SELECT
    '2022-06-29' as reportDate,
    waitTimeSubQuery.currentAssignedQueueId,
    waitTimeSubQuery.queueAccessPointId,
    waitTimeSubQuery.queueName AS QueueName,
    waitTimeSubQuery.queueReportCategory,
    waitTimeSubQuery.queuePriority,
    waitTimeSubQuery.queueOrganizationHierarchyId,
    COUNT(waitTimeSubQuery.id) AS totalCasesWaiting,
    SUM(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS sumWaitTimeMinutes,
    MAX(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS maxWaitTimeMinutes
FROM (SELECT
    id,
    currentAssignedQueueId,
    queueAccessPointId,
    queueName,
    queueReportCategory,
    queuePriority,
    queueOrganizationHierarchyId,
(CASE WHEN (receivedDateUTC > '0001-01-01T00:00:00Z' OR receivedDate > '0001-01-01T00:00:00Z') AND (appointmentDT IS NULL OR appointmentDT < '2022-09-28T12:58:47')

THEN CAST(DateDiff(MINUTE,
    CASE WHEN receivedDateUTC > '0001-01-01T00:00:00Z'
    THEN CONCAT(SUBSTRING(CAST(receivedDateUTC AS VARCHAR), 0, 19), 'Z')
    ELSE TRY_CAST(CONCAT(CONCAT(SUBSTRING(CAST(receivedDate AS VARCHAR), 0, 10), SUBSTRING(CAST(createdDT AS VARCHAR), 10, 9)), 'Z') AS DATETIME2) END,
    CASE WHEN claimedDT > '0001-01-01T00:00:00Z' AND claimedDT < '2022-06-29T23:59:00Z' AND transactionStatus != 'WaitingAssignment'
    THEN CONCAT(SUBSTRING(CAST(claimedDT AS VARCHAR), 0, 19), 'Z')
    ELSE '2022-06-29T23:59:00Z' END
) AS BIGINT)
ELSE 0 END) AS waitTimeMinutes
FROM @transactionsList
WHERE receivedDate <= '2022-06-29T00:00:00'
AND (claimedDT >= '2022-06-30T00:00:00' OR transactionStatus = 'WaitingAssignment')) waitTimeSubQuery
GROUP BY waitTimeSubQuery.currentAssignedQueueId, 
    waitTimeSubQuery.queueAccessPointId, 
    waitTimeSubQuery.queueName, 
    waitTimeSubQuery.queueReportCategory, 
    waitTimeSubQuery.queuePriority, 
    waitTimeSubQuery.queueOrganizationHierarchyId

--UNION ALL

-- 2022-06-30
SELECT
    '2022-06-30' as reportDate,
    waitTimeSubQuery.currentAssignedQueueId,
    waitTimeSubQuery.queueAccessPointId,
    waitTimeSubQuery.queueName AS QueueName,
    waitTimeSubQuery.queueReportCategory,
    waitTimeSubQuery.queuePriority,
    waitTimeSubQuery.queueOrganizationHierarchyId,
    COUNT(waitTimeSubQuery.id) AS totalCasesWaiting,
    SUM(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS sumWaitTimeMinutes,
    MAX(CASE WHEN waitTimeSubQuery.waitTimeMinutes > 0 THEN waitTimeSubQuery.waitTimeMinutes ELSE 0 END) AS maxWaitTimeMinutes
FROM (SELECT
    id,
    currentAssignedQueueId,
    queueAccessPointId,
    queueName,
    queueReportCategory,
    queuePriority,
    queueOrganizationHierarchyId,
(CASE WHEN (receivedDateUTC > '0001-01-01T00:00:00Z' OR receivedDate > '0001-01-01T00:00:00Z') AND (appointmentDT IS NULL OR appointmentDT < '2022-09-28T12:58:47')

THEN CAST(DateDiff(MINUTE,
    CASE WHEN receivedDateUTC > '0001-01-01T00:00:00Z'
    THEN CONCAT(SUBSTRING(CAST(receivedDateUTC AS VARCHAR), 0, 19), 'Z')
    ELSE TRY_CAST(CONCAT(CONCAT(SUBSTRING(CAST(receivedDate AS VARCHAR), 0, 10), SUBSTRING(CAST(createdDT AS VARCHAR), 10, 9)), 'Z') AS DATETIME2) END,
    CASE WHEN claimedDT > '0001-01-01T00:00:00Z' AND claimedDT < '2022-06-30T23:59:00Z' AND transactionStatus != 'WaitingAssignment'
    THEN CONCAT(SUBSTRING(CAST(claimedDT AS VARCHAR), 0, 19), 'Z')
    ELSE '2022-06-30T23:59:00Z' END
) AS BIGINT)
ELSE 0 END) AS waitTimeMinutes
FROM @transactionsList
WHERE receivedDate <= '2022-06-30T00:00:00'
AND (claimedDT >= '2022-07-01T00:00:00' OR transactionStatus = 'WaitingAssignment')) waitTimeSubQuery
GROUP BY waitTimeSubQuery.currentAssignedQueueId, 
    waitTimeSubQuery.queueAccessPointId, 
    waitTimeSubQuery.queueName, 
    waitTimeSubQuery.queueReportCategory, 
    waitTimeSubQuery.queuePriority, 
    waitTimeSubQuery.queueOrganizationHierarchyId