USE [master];
GO
ALTER DATABASE [AX_Liides_Test2]
SET SINGLE_USER
WITH ROLLBACK IMMEDIATE;

-- =====================================================
-- STEP 1: Setting Database Authorization and Enable Service Broker
-- =====================================================
PRINT 'Step 1: Setting database authorization and enabling Service Broker...';

ALTER AUTHORIZATION ON DATABASE::[AX_Liides_Test2] TO [sa];
PRINT 'Database authorization set to sa';

SELECT
    name,
    is_broker_enabled,
    service_broker_guid
FROM sys.databases
WHERE name = 'AX_Liides_Test2';

-- Enable Service Broker if not already enabled
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'AX_Liides_Test2' AND is_broker_enabled = 1)
BEGIN
    PRINT 'Service Broker is disabled. Enabling...';
    ALTER DATABASE [AX_Liides_Test2] SET ENABLE_BROKER;
    PRINT 'Service Broker is enabled for database';
END
ELSE
BEGIN
    PRINT 'Service Broker is enabled for database';
END

-- Switch to target database
USE [AX_Liides_Test2];
GO

-- =====================================================
-- STEP 2: Cleanup first
-- =====================================================
PRINT 'Step 2: Cleaning up existing Service Broker objects...';

-- Clear transmission queue
WHILE EXISTS (SELECT 1 FROM sys.transmission_queue)
BEGIN
    DECLARE @cleanup_handle UNIQUEIDENTIFIER = (SELECT TOP 1 conversation_handle FROM sys.transmission_queue);
END CONVERSATION @cleanup_handle WITH CLEANUP;
END
PRINT 'Transmission queue cleared';

-- Drop objects in correct dependency order
IF EXISTS (SELECT 1 FROM sys.services WHERE name = 'FlintService')
BEGIN
    DROP SERVICE [FlintService];
    PRINT 'Dropped service: FlintService';
END

IF EXISTS (SELECT 1 FROM sys.service_queues WHERE name = 'flint.events.export.queue')
BEGIN
    DROP QUEUE [flint.events.export.queue];
    PRINT 'Dropped queue: flint.events.export.queue';
END

IF EXISTS (SELECT 1 FROM sys.service_contracts WHERE name = 'FlintTableChangeContract')
BEGIN
    DROP CONTRACT [FlintTableChangeContract];
    PRINT 'Dropped contract: FlintTableChangeContract';
END

IF EXISTS (SELECT 1 FROM sys.service_message_types WHERE name = 'FlintTableChangeMessage')
BEGIN
    DROP MESSAGE TYPE [FlintTableChangeMessage];
    PRINT 'Dropped message type: FlintTableChangeMessage';
END

PRINT 'Cleanup completed successfully';
GO

-- =====================================================
-- STEP 3: Create Message Type
-- =====================================================
PRINT 'Step 3: Creating Message Type...';

CREATE MESSAGE TYPE [FlintTableChangeMessage]
    VALIDATION = NONE;

PRINT 'Created message type: FlintTableChangeMessage';
GO

-- =====================================================
-- STEP 4: Create Contract
-- =====================================================
PRINT 'Step 4: Creating Contract...';

CREATE CONTRACT [FlintTableChangeContract]
(
    [FlintTableChangeMessage] SENT BY INITIATOR
);

PRINT 'Created contract: FlintTableChangeContract';
GO

-- =====================================================
-- STEP 5: Create Queue
-- =====================================================
PRINT 'Step 5: Creating Queue...';

CREATE QUEUE [flint.events.export.queue]
WITH STATUS = ON,
     RETENTION = OFF;

ALTER QUEUE [flint.events.export.queue]
    WITH POISON_MESSAGE_HANDLING (STATUS = OFF);
GO

PRINT 'Created queue: flint.events.export.queue';
GO

-- =====================================================
-- STEP 6: Create Service
-- =====================================================
PRINT 'Step 6: Creating Service...';

CREATE SERVICE [FlintService]
    ON QUEUE [flint.events.export.queue]
    ([FlintTableChangeContract]);

PRINT 'Created service: FlintService';
GO

-- =====================================================
-- STEP 7: Create Stored Procedure
-- =====================================================
PRINT 'Step 7: Creating Stored Procedure...';
GO

CREATE OR ALTER PROCEDURE [dbo].[SendTableChangeMessage]
    @TableName NVARCHAR(128),
    @Operation NVARCHAR(10),
    @EventId NVARCHAR(50),
    @Record NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @ConversationHandle UNIQUEIDENTIFIER;
    DECLARE @JsonMessage NVARCHAR(MAX);

    -- Create JSON message
    SET @JsonMessage = N'{
        "eventId": "' + @EventId + '",
        "tableName": "' + @TableName + '",
        "operation": "' + @Operation + '",
        "timestamp": "' + CONVERT(NVARCHAR(30), GETUTCDATE(), 126) + '"';

    IF @Record IS NOT NULL
        SET @JsonMessage = @JsonMessage + ',
        "record": ' + @Record;

    SET @JsonMessage = @JsonMessage + '}';

BEGIN DIALOG @ConversationHandle
        FROM SERVICE [FlintService]
        TO SERVICE 'FlintService'
        ON CONTRACT [FlintTableChangeContract]
        WITH ENCRYPTION = OFF;

    -- Send message
    SEND ON CONVERSATION @ConversationHandle
        MESSAGE TYPE [FlintTableChangeMessage]
        (@JsonMessage);

    -- End conversation immediately
END CONVERSATION @ConversationHandle;

    PRINT 'Message sent successfully with EventId: ' + @EventId;
END;
GO

PRINT 'Created stored procedure: SendTableChangeMessage';
GO

-- =====================================================
-- STEP 8: Create Table Trigger
-- =====================================================
PRINT 'Step 8: Creating OSYNDEXP Trigger...';

-- Drop existing trigger if it exists
IF EXISTS (SELECT 1 FROM sys.triggers WHERE name = 'TR_OSYNDEXP_INSERT')
BEGIN
DROP TRIGGER [dbo].[TR_OSYNDEXP_INSERT];
PRINT 'Dropped existing trigger: TR_OSYNDEXP_INSERT';
END
GO

CREATE TRIGGER [dbo].[TR_OSYNDEXP_INSERT]
ON [OSYNDEXP]
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @EventId NVARCHAR(50);
    DECLARE @Record NVARCHAR(MAX);
    DECLARE @Jrknr INT;

    DECLARE insert_cursor CURSOR FOR
SELECT Jrknr FROM inserted;

OPEN insert_cursor;
FETCH NEXT FROM insert_cursor INTO @Jrknr;

WHILE @@FETCH_STATUS = 0
BEGIN
        SET @EventId = 'OSYNDEXP_' + CAST(@Jrknr AS NVARCHAR(50)) + '_' + CAST(NEWID() AS NVARCHAR(36));

SELECT @Record = (
    SELECT Jrknr, Id, Tunnus
    FROM inserted
    WHERE Jrknr = @Jrknr
    FOR JSON AUTO, WITHOUT_ARRAY_WRAPPER
    );

EXEC [dbo].[SendTableChangeMessage]
            @TableName = 'OSYNDEXP',
            @Operation = 'INSERT',
            @EventId = @EventId,
            @Record = @Record;

FETCH NEXT FROM insert_cursor INTO @Jrknr;
END;

CLOSE insert_cursor;
DEALLOCATE insert_cursor;
END;
GO

PRINT 'Created trigger: TR_OSYNDEXP_INSERT';
GO

-- =====================================================
-- STEP 9: Verification and Testing
-- =====================================================
PRINT 'Step 9: Verifying Service Broker setup...';

-- Check all objects were created
SELECT 'Message Types' as ObjectType, name COLLATE DATABASE_DEFAULT as ObjectName
FROM sys.service_message_types WHERE name = 'FlintTableChangeMessage'
UNION ALL
SELECT 'Contracts' as ObjectType, name COLLATE DATABASE_DEFAULT as ObjectName
FROM sys.service_contracts WHERE name = 'FlintTableChangeContract'
UNION ALL
SELECT 'Queues' as ObjectType, name COLLATE DATABASE_DEFAULT as ObjectName
FROM sys.service_queues WHERE name = 'flint.events.export.queue'
UNION ALL
SELECT 'Services' as ObjectType, name COLLATE DATABASE_DEFAULT as ObjectName
FROM sys.services WHERE name = 'FlintService'
UNION ALL
SELECT 'Triggers' as ObjectType, name COLLATE DATABASE_DEFAULT as ObjectName
FROM sys.triggers WHERE name = 'TR_OSYNDEXP_INSERT';
-- Test the setup
DECLARE @TestEventId NVARCHAR(50) = 'SETUP_TEST_' + CAST(NEWID() AS NVARCHAR(36));

EXEC [dbo].[SendTableChangeMessage]
    @TableName = 'SETUP_VERIFICATION',
    @Operation = 'INSERT',
    @EventId = @TestEventId,
    @Record = '{"test": "test_message", "status": "success"}';

-- Check results
SELECT COUNT(*) as total_messages FROM [flint.events.export.queue];

-- Show queue contents
SELECT
    conversation_handle,
    message_type_name,
    CAST(message_body AS NVARCHAR(MAX)) as json_message,
    message_enqueue_time
FROM [flint.events.export.queue]
WHERE message_type_name = 'FlintTableChangeMessage'
ORDER BY message_enqueue_time DESC;

GO

-- =====================================================
-- STEP 10: Final Test
-- =====================================================
PRINT 'Step 10: Testing end-to-end flow...';

INSERT INTO [OSYNDEXP] (Id, Tunnus)
VALUES (1006, 1006);

SELECT
    'Latest Messages' as info,
    CAST(message_body AS NVARCHAR(MAX)) as json_message,
    message_enqueue_time
FROM [flint.events.export.queue]
WHERE message_type_name = 'FlintTableChangeMessage'
ORDER BY message_enqueue_time DESC;

PRINT 'Service Broker is ready for use!';
GO

-- =====================================================
-- CLEANUP STEP: Clean Up Test Messages
-- =====================================================
PRINT 'Cleanup Step: Cleaning up test messages from queue...';

DECLARE @message_count INT = 0;
DECLARE @conversation_handle UNIQUEIDENTIFIER;
DECLARE @message_type_name NVARCHAR(256);
DECLARE @message_body VARBINARY(MAX);

WHILE EXISTS (SELECT 1 FROM [flint.events.export.queue] WHERE message_type_name = 'FlintTableChangeMessage')
BEGIN
    RECEIVE TOP(1)
        @conversation_handle = conversation_handle,
        @message_type_name = message_type_name,
        @message_body = message_body
    FROM [flint.events.export.queue];

    IF @@ROWCOUNT > 0
BEGIN
        SET @message_count = @message_count + 1;
        IF @message_type_name = 'FlintTableChangeMessage'
END CONVERSATION @conversation_handle;
END
ELSE
        BREAK;
END

PRINT 'Removed ' + CAST(@message_count AS NVARCHAR(10)) + ' test messages from queue';

PRINT '=====================================================';
PRINT 'SERVICE BROKER SETUP COMPLETE!';
PRINT '=====================================================';
GO

ALTER DATABASE [AX_Liides_Test2]
SET MULTI_USER;