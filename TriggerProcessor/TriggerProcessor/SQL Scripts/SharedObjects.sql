USE HomeDecor

BEGIN TRANSACTION

-- Enable SQL Server Service Broker for this database
IF (SELECT is_broker_enabled FROM sys.databases WHERE name = N'HomeDecor') = 0 BEGIN
	ALTER DATABASE HomeDecor SET ENABLE_BROKER;
END

GO

-- Create the messaging schema to hold our queues and such
IF NOT EXISTS (SELECT name FROM sys.schemas WHERE name = N'messaging') BEGIN
	EXEC sp_executesql N'CREATE SCHEMA messaging'
END
GO

-- Create the End-of-Stream notification message we publish to the target when we want to wrap up a conversation
IF NOT EXISTS (SELECT * FROM sys.service_message_types WHERE name = N'ServiceBroker_EndOfStream') BEGIN
	CREATE MESSAGE TYPE ServiceBroker_EndOfStream AUTHORIZATION dbo VALIDATION = NONE;
END
GO

-- Create the response queue that will receive "we're done" notifications from our various target applications
IF NOT EXISTS (SELECT * FROM sys.service_queues sq INNER JOIN sys.schemas s ON s.schema_id = sq.schema_id WHERE s.name = N'messaging' AND sq.name = N'TriggerResponseQueue') BEGIN
	CREATE QUEUE messaging.TriggerResponseQueue WITH STATUS = ON
END
GO

-- Create the Dialog cache table. This hold the handles to active dialogs for each service
IF NOT EXISTS (SELECT * FROM sys.objects o INNER JOIN sys.schemas s ON s.schema_id = o.schema_id WHERE s.name = 'messaging' AND o.name = 'Dialogs' AND type = 'U') BEGIN
	CREATE TABLE messaging.Dialogs (
		FromService SYSNAME NOT NULL,
		ToService SYSNAME NOT NULL,
		OnContract SYSNAME NOT NULL,
		Handle UNIQUEIDENTIFIER NOT NULL,
		PRIMARY KEY (FromService, ToService, OnContract),
		UNIQUE (Handle)
	);
END
GO

-- Create the generic stored procedure we use to manage conversation handles and publish messages
IF EXISTS (SELECT * FROM sys.objects o INNER JOIN sys.schemas s ON s.schema_id = o.schema_id WHERE s.name = 'messaging' AND o.name = 'SendMessage' AND type = 'P') BEGIN
	DROP PROCEDURE messaging.SendMessage
END
GO

CREATE PROCEDURE messaging.SendMessage(@fromService SYSNAME, @toService SYSNAME, @onContract SYSNAME, @messageType SYSNAME, @messageBody XML) AS BEGIN
	SET NOCOUNT ON;

	DECLARE @handle UNIQUEIDENTIFIER;
	DECLARE @counter INT = 1;
	DECLARE @error INT;

	BEGIN TRANSACTION;

	-- Will need a loop to retry in case the conversation is in a state that does not allow transmission
	WHILE (1 = 1) BEGIN

		-- Seek an eligible conversation in our Dialog cache
		SELECT @handle = Handle
		FROM messaging.Dialogs WITH (UPDLOCK)
		WHERE FromService = @fromService AND ToService = @toService AND OnContract = @OnContract;

		IF @handle IS NULL BEGIN

			-- Need to start a new conversation
			BEGIN DIALOG CONVERSATION @handle
			FROM SERVICE @fromService
			TO SERVICE @toService
			ON CONTRACT @onContract
			WITH ENCRYPTION = OFF;

			-- Set an expiration timer on the conversation
			BEGIN CONVERSATION TIMER (@handle) TIMEOUT = 3600;

			INSERT INTO messaging.Dialogs
			(FromService, ToService, OnContract, Handle)
			VALUES (@fromService, @toService, @onContract, @handle);
		END;

		-- Attempt to SEND on the associated conversation
		SEND ON CONVERSATION @handle MESSAGE TYPE @messageType (@messageBody);

		SELECT @error = @@ERROR;

		-- Successful send, just exit the loop
		IF @error = 0 BREAK;

		SELECT @counter = @counter + 1;

		-- We failed 10 times in a row, something must be broken
		IF @counter > 10 BEGIN
			RAISERROR (N'Failed to SEND on a conversation for more than 10 times. Error %i.', 16, 1, @error) WITH LOG;
			BREAK;
		END

		-- Delete the associated conversation from the table and try again
		DELETE FROM messaging.Dialogs WHERE Handle = @handle;
		SELECT @handle = NULL;
	END

	COMMIT TRANSACTION;
END
GO

-- Create the generic stored procedure we use to clean up our open Dialogs when we receive a
-- "we're done" notification from a target service.
IF EXISTS (SELECT * FROM sys.objects o INNER JOIN sys.schemas s ON s.schema_id = o.schema_id WHERE s.name = 'messaging' AND o.name = 'HandleMessageResponses' AND type = 'P') BEGIN
	DROP PROCEDURE messaging.HandleMessageResponses
END
GO

CREATE PROCEDURE messaging.HandleMessageResponses AS BEGIN
	DECLARE @handle UNIQUEIDENTIFIER;
	DECLARE @messageTypeName SYSNAME;
	DECLARE @messageBody VARBINARY(MAX);

	BEGIN TRANSACTION;

	RECEIVE TOP(1)
		@handle = conversation_handle,
		@messageTypeName = message_type_name,
		@messageBody = message_body
	FROM messaging.TriggerResponseQueue;

	IF @handle IS NOT NULL BEGIN

		-- Delete the message from the Dialogs table before sending the ServiceBroker_EndOfStream message.
		-- The order is important to avoid deadlocks. Strictly speaking, we should only delete if the
		-- message type is timer or error, but is simpler and safer to just delete always
		DELETE FROM messaging.Dialogs WHERE [Handle] = @handle;
		
		-- This is a message automatically triggered by the timer set up when the conversation was
		-- created. It signals that it's time to release the dialog handle. We do this by publishing an
		-- [EndOfStream] message to the target. The target will then finish processing any
		-- pending messages, and respond with an EndDialog message to let us know it's all
		-- done.
		IF @messageTypeName = N'http://schemas.microsoft.com/SQL/ServiceBroker/DialogTimer' BEGIN;
			SEND ON CONVERSATION @handle MESSAGE TYPE ServiceBroker_EndOfStream
		
		-- Now the target has responded to let us know it's done processing messages and has ended
		-- its side of the conversation, and we're free to end it on our end as well.
		END	ELSE IF @messageTypeName = N'http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog' BEGIN;
			END CONVERSATION @handle;
		
		-- If we receive an error message, write it to the log. Maybe add some alerts here?
		END	ELSE IF @messageTypeName = N'http://schemas.microsoft.com/SQL/ServiceBroker/Error' BEGIN;
			END CONVERSATION @handle;

			DECLARE @error INT;
			DECLARE @description NVARCHAR(4000);

			WITH XMLNAMESPACES ('http://schemas.microsoft.com/SQL/ServiceBroker/Error' AS ssb)
			SELECT
				@error = CAST(@messageBody AS XML).value('(//ssb:Error/ssb:Code)[1]', 'INT'),
				@description = CAST(@messageBody AS XML).value('(//ssb:Error/ssb:Description)[1]', 'NVARCHAR(4000)')

			RAISERROR(N'Received error Code:%i Description:"%s"', 16, 1, @error, @description) WITH LOG;
		END
	END

	COMMIT TRANSACTION
END
GO

-- Alter our response queue to call the newly-created HandleMessageResponses stored procedure automatically
ALTER QUEUE messaging.TriggerResponseQueue
WITH ACTIVATION (
	STATUS = ON,
	MAX_QUEUE_READERS = 1,
	PROCEDURE_NAME = messaging.HandleMessageResponses,
	EXECUTE AS OWNER
);
GO

COMMIT TRANSACTION
GO