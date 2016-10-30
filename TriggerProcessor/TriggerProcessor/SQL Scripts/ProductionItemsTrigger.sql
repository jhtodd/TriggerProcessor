-- Create Message Types
CREATE MESSAGE TYPE ProductionItemTriggerMessage AUTHORIZATION dbo VALIDATION = NONE;
GO

-- Create Contracts
CREATE CONTRACT ProductionItemTriggerContract AUTHORIZATION dbo (ProductionItemTriggerMessage SENT BY ANY, ServiceBroker_EndOfStream SENT BY ANY);
GO

-- Create Queues
CREATE QUEUE messaging.ProductionItemTriggerQueue WITH STATUS = ON
GO

-- Create Service Initiators 
CREATE SERVICE ProductionItemTriggerInitiator AUTHORIZATION dbo ON QUEUE messaging.TriggerResponseQueue (ProductionItemTriggerContract); 
GO

-- Create Service Targets
CREATE SERVICE ProductionItemTriggerTarget AUTHORIZATION dbo ON QUEUE messaging.ProductionItemTriggerQueue (ProductionItemTriggerContract);
GO

-- Create the actual trigger
CREATE TRIGGER dbo.ProductionItemTrigger ON dbo.ProductionItems FOR INSERT, UPDATE, DELETE AS BEGIN
    
	DECLARE @didAnythingHappen INT = CASE WHEN EXISTS (SELECT 1 FROM inserted) OR EXISTS (SELECT 1 FROM deleted) THEN 1 ELSE 0 END
	
	IF @didAnythingHappen != 0 BEGIN

		-- Construct the XML contents of the message we're publishing
		DECLARE @messageBody XML = (
			SELECT
				GETDATE() AS [Date],
				SUSER_NAME() AS [User],
				APP_NAME() AS [Application],
				(SELECT transaction_id FROM sys.dm_tran_current_transaction) AS TransactionId,
				(SELECT * FROM inserted FOR XML RAW, TYPE) AS Inserted,
				(SELECT * FROM deleted FOR XML RAW, TYPE) AS Deleted
			FOR XML PATH('Root')
		)

		EXEC messaging.SendMessage
			'ProductionItemTriggerInitiator',
			'ProductionItemTriggerTarget',
			'ProductionItemTriggerContract',
			'ProductionItemTriggerMessage',
			@messageBody
	END
END
GO