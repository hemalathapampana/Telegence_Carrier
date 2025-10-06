### dbo.usp_Telegence_Update_Device (modified to set Unknown on NOT MATCHED immediately)

```sql
-- =============================================
-- Author:  Lee Daniel (Updated by automation)
-- Create date: 2019-10-28
--
-- Description: Updates TelegenceDevice based on the data in TelegenceDeviceStaging.
--              Updated to remove 3-sync hold, add feed-validity guard, and set
--              devices missing from a valid feed to Unknown immediately.
-- =============================================
CREATE OR ALTER PROCEDURE [dbo].[usp_Telegence_Update_Device]
    @ServiceProviderId INT
AS
BEGIN
    SET NOCOUNT ON;

    -- Feature flag: allow safe enable/disable of immediate Unknown behavior
    DECLARE @EnableImmediateUnknown BIT = 1;

    -- Feed completeness guard (basic heuristic using staging freshness and consistency)
    DECLARE @IsFeedValid BIT = 0;
    DECLARE @FeedMaxRefreshUtc DATETIME;

    SELECT @FeedMaxRefreshUtc = MAX(tdes.RefreshTimestamp)
    FROM [dbo].[TelegenceDeviceStaging] tdes
    WHERE tdes.ServiceProviderId = @ServiceProviderId
      AND tdes.SubscriberNumber IS NOT NULL;

    IF @FeedMaxRefreshUtc IS NOT NULL
    BEGIN
        DECLARE @FeedRowCount INT = (
            SELECT COUNT(*) FROM [dbo].[TelegenceDeviceStaging] t
            WHERE t.ServiceProviderId = @ServiceProviderId
              AND t.SubscriberNumber IS NOT NULL
              AND t.RefreshTimestamp = @FeedMaxRefreshUtc
        );

        DECLARE @DistinctRefreshCnt INT = (
            SELECT COUNT(DISTINCT CONVERT(date, t.RefreshTimestamp))
            FROM [dbo].[TelegenceDeviceStaging] t
            WHERE t.ServiceProviderId = @ServiceProviderId
              AND t.SubscriberNumber IS NOT NULL
              AND t.RefreshTimestamp >= DATEADD(day, -1, @FeedMaxRefreshUtc)
        );

        IF (@FeedRowCount > 0 AND @DistinctRefreshCnt <= 1)
            SET @IsFeedValid = 1;
    END

    IF (@IsFeedValid = 0)
    BEGIN
        -- Do not mutate statuses on invalid/incomplete feeds
        RAISERROR('usp_Telegence_Update_Device: FEED_INVALID for ServiceProviderId %d. Skipping status updates.', 10, 1, @ServiceProviderId);
        RETURN;
    END

    BEGIN TRANSACTION;

    DECLARE @ActivatedStatusId INT = (SELECT id FROM [dbo].[DeviceStatus] WHERE [Status] = 'a' AND IntegrationId = 6);
    DECLARE @UnknownStatusId  INT = (SELECT id FROM [dbo].[DeviceStatus] WHERE [Status] = 'Unknown' AND IntegrationId = 6);

    -- Build this run's latest staging rows per SubscriberNumber
    ;WITH LatestStaging AS (
        SELECT
            tdes.SubscriberNumber,
            tdes.FoundationAccountNumber,
            tdes.BillingAccountNumber,
            'usp_Telegence_Update_Device' AS CreatedBy,
            tdes.CreatedDate,
            tdes.RefreshTimestamp,
            ROW_NUMBER() OVER (PARTITION BY tdes.SubscriberNumber ORDER BY tdes.CreatedDate DESC) AS rn,
            st.Id AS DeviceStatusId,
            tdes.ServiceProviderId,
            tdes.SubscriberNumberStatus,
            tdes.BanStatus,
            cs.ContractStatus,
            st.[Status] AS EffectiveStatusText
        FROM [dbo].[TelegenceDeviceStaging] tdes
        LEFT JOIN [dbo].[DeviceStatus] st
            ON LOWER(tdes.[SubscriberNumberStatus]) = LOWER(st.[Status]) AND st.IntegrationId = 6
        LEFT JOIN [dbo].[TelegenceDeviceContractStatusStaging] cs
            ON tdes.[SubscriberNumber] = cs.[SubscriberNumber]
        WHERE tdes.SubscriberNumber IS NOT NULL
          AND tdes.ServiceProviderId = @ServiceProviderId
          AND tdes.RefreshTimestamp = @FeedMaxRefreshUtc
    ),
    SourceRows AS (
        SELECT * FROM LatestStaging WHERE rn = 1
    )
    SELECT * INTO #Source FROM SourceRows;

    -- Upsert/merge and mark missing devices Unknown immediately when feature flag enabled
    MERGE [dbo].[TelegenceDevice] AS TARGET
    USING #Source AS SOURCE
        ON TARGET.SubscriberNumber = SOURCE.SubscriberNumber
    WHEN MATCHED THEN
        UPDATE SET
            TARGET.[OldDeviceStatusId]      = TARGET.[DeviceStatusId],
            TARGET.[DeviceStatusId]         = SOURCE.[DeviceStatusId],
            TARGET.FoundationAccountNumber  = SOURCE.FoundationAccountNumber,
            TARGET.BillingAccountNumber     = SOURCE.BillingAccountNumber,
            TARGET.[RefreshTimestamp]       = SOURCE.[RefreshTimestamp],
            TARGET.ModifiedBy               = SOURCE.CreatedBy,
            TARGET.ModifiedDate             = SOURCE.CreatedDate,
            TARGET.[ServiceProviderId]      = SOURCE.[ServiceProviderId],
            TARGET.SubscriberNumberStatus   = SOURCE.SubscriberNumberStatus,
            TARGET.BanStatus                = SOURCE.BanStatus,
            TARGET.ContractStatus           = SOURCE.ContractStatus,
            -- New/extended fields (must exist in schema)
            TARGET.RawCarrierStatus         = SOURCE.SubscriberNumberStatus,
            TARGET.LastSeenAtCarrier        = GETUTCDATE(),
            TARGET.EffectiveStatus          = COALESCE(SOURCE.EffectiveStatusText, 'Unknown'),
            TARGET.StatusReason             = 'CARRIER_STATUS',
            TARGET.LastActivatedDate        = CASE
                                                WHEN TARGET.[DeviceStatusId] = @ActivatedStatusId
                                                     AND SOURCE.DeviceStatusId <> @ActivatedStatusId
                                                THEN GETUTCDATE()
                                                ELSE TARGET.LastActivatedDate
                                              END
    WHEN NOT MATCHED BY TARGET AND SOURCE.ServiceProviderId = @ServiceProviderId THEN
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
            [ContractStatus],
            -- New/extended fields (must exist in schema)
            RawCarrierStatus,
            LastSeenAtCarrier,
            EffectiveStatus,
            StatusReason
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
            1, -- IsActive
            0, -- IsDeleted
            SOURCE.[ServiceProviderId],
            SOURCE.BanStatus,
            SOURCE.ContractStatus,
            SOURCE.SubscriberNumberStatus,
            GETUTCDATE(),
            COALESCE(SOURCE.EffectiveStatusText, 'Unknown'),
            'CARRIER_STATUS'
        )
    WHEN NOT MATCHED BY SOURCE AND TARGET.ServiceProviderId = @ServiceProviderId AND @EnableImmediateUnknown = 1 THEN
        UPDATE SET
            TARGET.[OldDeviceStatusId]       = TARGET.[DeviceStatusId],
            TARGET.[DeviceStatusId]          = @UnknownStatusId,
            TARGET.[SubscriberNumberStatus]  = 'Unknown',
            TARGET.[EffectiveStatus]         = 'Unknown',
            TARGET.[StatusReason]            = 'NOT_FOUND_IN_FEED',
            TARGET.[ModifiedBy]              = 'usp_Telegence_Update_Device',
            TARGET.[ModifiedDate]            = GETUTCDATE();

    -- Remove temporary artifacts
    DROP TABLE IF EXISTS #Source;

    -- Sync audit (for valid feed only)
    INSERT INTO [dbo].[TelegenceDeviceSyncAudit] (
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
    SELECT
        CAST(GETUTCDATE() AS DATE),
        [A] AS [ActiveCount],
        [S] AS [SuspendCount],
        'usp_Telegence_Update_Device',
        CURRENT_TIMESTAMP,
        1,
        0,
        BillYear,
        BillMonth,
        ServiceProviderId
    FROM (
        SELECT
            tDev.ServiceProviderId,
            MONTH(MAX(tDevDet.NextBillCycleDate)) AS BillMonth,
            YEAR(MAX(tDevDet.NextBillCycleDate)) AS BillYear,
            st.[Status],
            COUNT(*) AS total
        FROM [dbo].[TelegenceDevice] tDev
        INNER JOIN [dbo].[DeviceStatus] st ON tDev.DeviceStatusId = st.id
        INNER JOIN [dbo].[TelegenceDeviceDetailStaging] tDevDet ON tDev.SubscriberNumber = tDevDet.SubscriberNumber
        GROUP BY tDev.ServiceProviderId, st.[Status]
    ) AS summary
    PIVOT (SUM(total) FOR [Status] IN ([A],[S])) AS pv
    WHERE ServiceProviderId = @ServiceProviderId;

    COMMIT TRANSACTION;
END
```
