-- =============================================  
-- Author:  Lee Daniel  
-- Create date: 2019-10-28  
--  
-- Parameter:   
--  
-- Description: Updates TelegenceDevice based on the data in TelegenceDeviceStaging.  
--  
-- =============================================  
CREATE PROCEDURE [dbo].[usp_Telegence_Update_Device]  
    @ServiceProviderId int   
AS  
BEGIN  
    BEGIN TRANSACTION  
      
    set nocount on;  
    declare @ActivatedStatusId int = (select id from [dbo].[DeviceStatus] where [Status] = 'a' AND IntegrationId = 6);  
    declare @UnknownStatusId int = (select id from [dbo].[DeviceStatus] where [Status] = 'Unknown' AND IntegrationId = 6);  
  
    MERGE [dbo].[TelegenceDevice] AS TARGET  
    USING   
    (  
        SELECT   
            SubscriberNumber,  
            FoundationAccountNumber,  
            BillingAccountNumber,  
            CreatedBy,   
            [CreatedDate],   
            [RefreshTimestamp],  
            DeviceStatusId,  
            [ServiceProviderId],  
            [SubscriberNumberStatus],  
            [BanStatus],  
            [ContractStatus]  
        FROM (  
            SELECT   
                tdes.[SubscriberNumber],  
                FoundationAccountNumber,  
                tdes.[BillingAccountNumber],  
                'usp_Telegence_Update_Device' AS CreatedBy,   
                tdes.[CreatedDate] as [CreatedDate],  
                tdes.[RefreshTimestamp],  
                ROW_NUMBER() OVER(PARTITION BY tdes.[SubscriberNumber] ORDER BY tdes.[CreatedDate] DESC) AS RecordNumber,  
                st.Id as DeviceStatusId,  
                tdes.[ServiceProviderId],  
                tdes.[SubscriberNumberStatus],  
                tdes.BanStatus,  
                cs.ContractStatus  
            FROM [dbo].[TelegenceDeviceStaging] tdes  
                left outer join [dbo].[DeviceStatus] st ON LOWER(tdes.[SubscriberNumberStatus]) = LOWER(st.[Status]) AND IntegrationId = 6  
                left outer join [dbo].[TelegenceDeviceContractStatusStaging] cs ON tdes.[SubscriberNumber] = cs.[SubscriberNumber]  
            WHERE tdes.SubscriberNumber IS NOT NULL  
        ) a  
        WHERE RecordNumber = 1  
    )  AS SOURCE  
    ON  
    TARGET.SubscriberNumber = SOURCE.SubscriberNumber  
    WHEN MATCHED  
        THEN  
            UPDATE   
            SET   
                TARGET.[OldDeviceStatusId] = TARGET.[DeviceStatusId],  
                TARGET.[DeviceStatusId] = SOURCE.[DeviceStatusId],  
                TARGET.FoundationAccountNumber = SOURCE.FoundationAccountNumber,  
                TARGET.BillingAccountNumber = SOURCE.BillingAccountNumber,  
                TARGET.[RefreshTimestamp] = SOURCE.[RefreshTimestamp],  
                TARGET.ModifiedBy = SOURCE.CreatedBy,  
                TARGET.ModifiedDate = SOURCE.CreatedDate,  
                TARGET.[ServiceProviderId] = SOURCE.[ServiceProviderId],  
                TARGET.SubscriberNumberStatus = SOURCE.SubscriberNumberStatus,  
                TARGET.BanStatus = SOURCE.BanStatus,  
                TARGET.ContractStatus = Source.ContractStatus,  
                TARGET.[FailedSyncCount] = NULL, -- Reset FailedSyncCount  
                /* Keeping it consistent with Jasper.  The name LastActivatedDate is misleading as it contains the date the Activated status ended the last.*/  
                TARGET.LastActivatedDate = CASE   
                                                WHEN   
                                                    TARGET.[DeviceStatusId] = @ActivatedStatusId   
                                                    AND   
                                                    SOURCE.DeviceStatusId <> @ActivatedStatusId   
                                                THEN   
                                                    GETUTCDATE() /* Keeping it to getdate to be consistent.  It probably needs to be updated to getutcdate()*/  
                                                ELSE   
                                                    TARGET.LastActivatedDate   
                                                END  
    WHEN NOT MATCHED BY TARGET AND SOURCE.ServiceProviderId = @ServiceProviderId  
        THEN  
            INSERT (  
                SubscriberNumber,  
                FoundationAccountNumber,  
                BillingAccountNumber,   
                [DeviceStatusId],  
                SubscriberNumberStatus,  
                [RefreshTimestamp],  
                CreatedBy,  
                CreatedDate,  
                IsActive,  
                IsDeleted,  
                [ServiceProviderId],  
                [BanStatus],  
                [ContractStatus]  
            )   
            VALUES (  
                SOURCE.SubscriberNumber,  
                SOURCE.FoundationAccountNumber,  
                SOURCE.BillingAccountNumber,  
                SOURCE.DeviceStatusId,  
                SOURCE.SubscriberNumberStatus,  
                SOURCE.[RefreshTimestamp],  
                SOURCE.CreatedBy,  
                SOURCE.CreatedDate,  
                1,  
                0,  
                [ServiceProviderId],  
                SOURCE.BanStatus,  
                SOURCE.ContractStatus  
            );  
  
	--Update devices present in staging 
	-- Save old status and update to actual status from carrier
	UPDATE [dbo].[TelegenceDevice]
	SET 
		[dbo].[TelegenceDevice].OldDeviceStatusId = [dbo].[TelegenceDevice].DeviceStatusId,  -- save previous status
		[dbo].[TelegenceDevice].DeviceStatusId = [dbo].[TelegenceDeviceStaging].DeviceStatusId,    -- update to actual status
		[dbo].[TelegenceDevice].SubscriberNumberStatus = [dbo].[TelegenceDeviceStaging].SubscriberNumberStatus,
		[dbo].[TelegenceDevice].FailedSyncCount = 0,                   -- reset failed sync count
		[dbo].[TelegenceDevice].ModifiedBy = 'usp_Telegence_Update_Device',
		[dbo].[TelegenceDevice].ModifiedDate = GETUTCDATE()
	FROM [dbo].[TelegenceDeviceStaging]
	WHERE [dbo].[TelegenceDevice].SubscriberNumber = [dbo].[TelegenceDeviceStaging].SubscriberNumber;

    -- Count failed sync  
    UPDATE [dbo].[TelegenceDevice]  
    SET [FailedSyncCount] = COALESCE([FailedSyncCount], 0) + 1  
    WHERE ServiceProviderId = @ServiceProviderId  
        AND COALESCE([FailedSyncCount], 0) <= 3  
        AND SubscriberNumber NOT IN (SELECT SubscriberNumber FROM [TelegenceDeviceStaging]);  
  
    -- Update status for FailedSyncCount > 3  
    UPDATE [dbo].[TelegenceDevice]  
    SET   
        [DeviceStatusId] = @UnknownStatusId,  
        [OldDeviceStatusId] = [DeviceStatusId],  
        [SubscriberNumberStatus] = 'Unknown',  
        [ModifiedBy] = 'usp_Telegence_Update_Device',  
        [ModifiedDate] = GETUTCDATE(),  
        [FailedSyncCount] = NULL -- Reset FailedSyncCount  
    WHERE ServiceProviderId = @ServiceProviderId  
        AND COALESCE([FailedSyncCount], 0) > 3  
        AND SubscriberNumber NOT IN (SELECT SubscriberNumber FROM [TelegenceDeviceStaging]);  
  
    INSERT INTO   
        [dbo].[TelegenceDeviceSyncAudit] (  
            [LastSyncDate],  
            [ActiveCount],  
            [SuspendCount],  
            [CreatedBy],  
            [CreatedDate],  
            [IsActive],  
            [IsDeleted],  
            [BillYear],  
            [BillMonth],  
            [ServiceProviderId]  
        )  
        select  
            CAST(GETUTCDATE() AS DATE),  
            [A] AS [ActiveCount],  
            [S] AS [SuspendCount],  
            'usp_Telegence_Update_Device',  
            CURRENT_TIMESTAMP,  
            1 /*[IsActive]*/,  
            0 /*[IsDeleted]*/,  
            BillYear,  
            BillMonth,  
            ServiceProviderId  
        FROM  
            (  
                SELECT   
                    tDev.ServiceProviderId,  
                    MONTH(MAX(tDevDet.NextBillCycleDate)) AS BillMonth,  
                    YEAR(MAX(tDevDet.NextBillCycleDate)) AS BillYear,  
                    st.[Status],  
                    COUNT(*) AS total  
                from   
                    [dbo].[TelegenceDevice] tDev  
                    INNER JOIN [dbo].[DeviceStatus] st ON tDev.DeviceStatusId = st.id  
                    INNER JOIN [dbo].[TelegenceDeviceDetailStaging] tDevDet ON tDev.SubscriberNumber = tDevDet.SubscriberNumber  
                GROUP BY   
                    tDev.ServiceProviderId, st.[Status]  
            ) AS summary PIVOT(  
                SUM(total) FOR [Status] IN ([A],[S])  
                )    
            AS pv  
        WHERE ServiceProviderId = @ServiceProviderId  
  
    COMMIT TRANSACTION  
END