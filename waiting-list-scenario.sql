-- Business rules:
-- 		- Waiting list entry IDs are never reused for a waiting list ID


-- Create tables:
-- 		WaitingList - Records define a waiting list
-- 		WaitingListEntry - Records define an entry on a list
-- 		WaitingListAudit - Records define an audit trail of transfers between lists

CREATE TABLE dbo.WaitingList (
	ListID CHAR(3) NOT NULL,
	PRIMARY KEY (ListID)
);

CREATE TABLE dbo.WaitingListEntry (
	ListID CHAR(3) NOT NULL,
	EntryID INT NOT NULL,
	PRIMARY KEY (ListID, EntryID),
	FOREIGN KEY (ListID) REFERENCES dbo.WaitingList(ListID)
);

CREATE TABLE dbo.WaitingListAudit (
	OldListID CHAR(3) NOT NULL,
	OldEntryID INT NOT NULL,
	NewListID CHAR(3) NOT NULL,
	NewEntryID INT NOT NULL,
	AuditDateTime DATETIME NOT NULL
);


-- Create sample data:
--		- Three waiting lists
--		- Three entries on different lists, the entries are at mixed stages of the audit trails
-- 		- An audit trail of audit records between the lists

INSERT INTO dbo.WaitingList(ListID) VALUES ('WL1');
INSERT INTO dbo.WaitingList(ListID) VALUES ('WL2');
INSERT INTO dbo.WaitingList(ListID) VALUES ('WL3');

INSERT INTO dbo.WaitingListEntry(ListID, EntryID) VALUES ('WL1', 1);
INSERT INTO dbo.WaitingListEntry(ListID, EntryID) VALUES ('WL2', 999);
INSERT INTO dbo.WaitingListEntry(ListID, EntryID) VALUES ('WL1', 55);

-- Audit sequence = WL1/1 -> WL3/99 -> WL2/10 -> WL3/50
INSERT INTO dbo.WaitingListAudit(OldListID, OldEntryID, NewListID, NewEntryID, AuditDateTime) VALUES ('WL1', 1, 'WL3', 99, '2017-01-01 10:30:00');
INSERT INTO dbo.WaitingListAudit(OldListID, OldEntryID, NewListID, NewEntryID, AuditDateTime) VALUES ('WL3', 99, 'WL2', 10, '2017-02-01 11:30:00');
INSERT INTO dbo.WaitingListAudit(OldListID, OldEntryID, NewListID, NewEntryID, AuditDateTime) VALUES ('WL2', 10, 'WL3', 50, '2017-03-01 12:30:00');

-- Audit sequence = WL2/99 -> WL3/10 -> WL1/100 -> WL2/999
INSERT INTO dbo.WaitingListAudit(OldListID, OldEntryID, NewListID, NewEntryID, AuditDateTime) VALUES ('WL2', 99, 'WL3', 10, '2018-01-01 10:30:00');
INSERT INTO dbo.WaitingListAudit(OldListID, OldEntryID, NewListID, NewEntryID, AuditDateTime) VALUES ('WL3', 10, 'WL1', 100, '2018-02-01 11:30:00');
INSERT INTO dbo.WaitingListAudit(OldListID, OldEntryID, NewListID, NewEntryID, AuditDateTime) VALUES ('WL1', 100, 'WL2', 999, '2018-03-01 12:30:00');

-- Audit sequence = WL3/999 -> WL2/1001 -> WL1/55 -> WL3/1200 -> WL2/800 -> WL1 -> 99999
INSERT INTO dbo.WaitingListAudit(OldListID, OldEntryID, NewListID, NewEntryID, AuditDateTime) VALUES ('WL3', 999, 'WL2', 1001, '2019-01-01 10:30:00');
INSERT INTO dbo.WaitingListAudit(OldListID, OldEntryID, NewListID, NewEntryID, AuditDateTime) VALUES ('WL2', 1001, 'WL1', 55, '2019-02-01 11:30:00');
INSERT INTO dbo.WaitingListAudit(OldListID, OldEntryID, NewListID, NewEntryID, AuditDateTime) VALUES ('WL1', 55, 'WL3', 1200, '2019-03-01 12:30:00');
INSERT INTO dbo.WaitingListAudit(OldListID, OldEntryID, NewListID, NewEntryID, AuditDateTime) VALUES ('WL3', 1200, 'WL2', 800, '2019-04-01 13:30:00');
INSERT INTO dbo.WaitingListAudit(OldListID, OldEntryID, NewListID, NewEntryID, AuditDateTime) VALUES ('WL2', 800, 'WL1', 99999, '2019-05-01 14:30:00');


-- Cursor through all audit records where the old entry details exist in the waiting list entry table
DECLARE @Cur as CURSOR;
DECLARE @Cur_OldListID as CHAR(3);
DECLARE @Cur_OldEntryID as INT;

SET @Cur = CURSOR FOR 
	SELECT OldListID, OldEntryID
	FROM dbo.WaitingListAudit aud
	WHERE EXISTS (
		SELECT NULL
		FROM dbo.WaitingListEntry e
		WHERE 
			e.ListID = aud.OldListID
			AND e.EntryID = aud.OldEntryID
	);
	
OPEN @Cur;

FETCH NEXT FROM @Cur INTO @Cur_OldListID, @Cur_OldEntryID;
	
WHILE @@FETCH_STATUS = 0
BEGIN
	-- Create recursive CTE
	WITH auditCTE AS (
		SELECT OldListID, OldEntryID, NewListID, NewEntryID, AuditDateTime
		FROM dbo.WaitingListAudit
		WHERE OldListID = @Cur_OldListID AND OldEntryID = @Cur_OldEntryID

		UNION ALL
		
		SELECT a.OldListID, a.OldEntryID, a.NewListID, a.NewEntryID, a.AuditDateTime
		FROM dbo.WaitingListAudit a
		INNER JOIN auditCTE cte ON cte.NewListID = a.OldListID AND cte.NewEntryID = a.OldEntryID
	)
	UPDATE dbo.WaitingListEntry
	SET 
		ListID = cte.NewListID,
		EntryID = cte.NewEntryID
	FROM 
		auditCTE cte
	WHERE 
		ListID = @Cur_OldListID
		AND EntryID = @Cur_OldEntryID
		AND cte.AuditDateTime = (
			SELECT MAX(AuditDateTime)
			FROM auditCTE
		);
 
	FETCH NEXT FROM @Cur INTO @Cur_OldListID, @Cur_OldEntryID;
END

CLOSE @Cur;
DEALLOCATE @Cur;


-- Print results
SELECT * FROM dbo.WaitingListEntry;

IF EXISTS (SELECT NULL FROM dbo.WaitingListEntry WHERE ListID = 'WL3' AND EntryID = 50)
	PRINT '[Success] Found WL3/50';
ELSE
	PRINT '[Error] Could not find WL3/50';

IF EXISTS (SELECT NULL FROM dbo.WaitingListEntry WHERE ListID = 'WL2' AND EntryID = 999)
	PRINT '[Success] Found WL2/999';
ELSE
	PRINT '[Error] Could not find WL2/999';

IF EXISTS (SELECT NULL FROM dbo.WaitingListEntry WHERE ListID = 'WL1' AND EntryID = 99999)
	PRINT '[Success] Found WL1/99999';
ELSE
	PRINT '[Error] Could not find WL1/99999';