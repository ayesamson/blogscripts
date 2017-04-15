/*****************************************************************
**	Author	:	Samson J. Loo (justsamson.com | @just_samson)
**	Created : 9/26/2010
**	Intent	: Notification of subscriptions that are nearing the
**						the expiration threshold
**	Notes	: Requires db mail
**	Version	: 0.01
*****************************************************************/

IF OBJECT_ID('sp_RP_GetExpiringSubscribers') IS NOT NULL
DROP PROCEDURE sp_RP_GetExpiringSubscribers
GO
CREATE PROCEDURE [dbo].[sp_RP_GetExpiringSubscribers]
AS

DECLARE @cnt INT
SET @cnt = 0
SELECT	@cnt = COUNT(a.[subscriber_name])
FROM [Distribution]..[MSmerge_sessions] s
	JOIN [Distribution]..[MSmerge_agents] a ON s.agent_id = a.id
	JOIN [Distribution]..[MSreplication_monitordata] m ON s.agent_id = m.agent_id
	JOIN [Distribution]..[MSpublicationthresholds] t ON m.publication_id = t.publication_id
WHERE s.end_time IN
(
	SELECT TOP 1 s1.end_time 
	FROM [Distribution]..[MSmerge_sessions] s1
	WHERE s.agent_id = s1.agent_id
	ORDER BY s1.end_time DESC
) AND DATEDIFF(d,s.[start_time],getdate()) >= CAST((CAST(t.[value] AS DECIMAL)/100)* m.[retention] AS INT)
AND t.[isenabled] = 1
GROUP BY s.[start_time]

IF @cnt > 0
	BEGIN
		
		DECLARE @tableHTML  NVARCHAR(MAX) ;

		SET @tableHTML =
			N'<H1><font color=&quot;#FF0000&quot;>Expiring Subscription Report</font></H1>' +
			N'<table border=&quot;0&quot; cellspacing=&quot;2&quot; cellpadding=&quot;2&quot;>' +
			N'<tr><th bgcolor=&quot;#BDBDBD&quot;>Subscriber</th>' +
						N'<th bgcolor=&quot;#BDBDBD&quot;>Status</th>
							<th bgcolor=&quot;#BDBDBD&quot;>Delivery Rate</th>
							<th bgcolor=&quot;#BDBDBD&quot;>Last Sync</th>' +
						N'<th bgcolor=&quot;#BDBDBD&quot;>Duration</th>
							<th bgcolor=&quot;#BDBDBD&quot;>Conn Type</th>
							<th bgcolor=&quot;#BDBDBD&quot;>Result</th>
							<th bgcolor=&quot;#BDBDBD&quot;>Days Behind</th>					
						  <th bgcolor=&quot;#BDBDBD&quot;>Subscriber Status</th></tr>' +
			CAST ( ( 

		SELECT
		td = CASE
					WHEN CHARINDEX('',a.[subscriber_name]) > 0 THEN LEFT(a.[subscriber_name],CHARINDEX('',a.[subscriber_name])-1)
					ELSE a.[subscriber_name]
				END	
			,''
			,td = CASE
				WHEN s.[runstatus] = 3 THEN 'Synchornizing'
				WHEN s.[runstatus] = 5 THEN 'Retrying failed command'
			ELSE 'Not Synchronizing'
			END 
			,''
			,td = CAST(s.[delivery_rate] AS VARCHAR) + ' rows/sec'
			,''
			,td = s.[start_time]
			,''
			,td = CAST((s.[duration]/86400) AS VARCHAR) 
			+ '.' + CAST(REPLACE(STR(((s.[duration]/3600) - ((s.[duration]/86400) * 24)),2),SPACE(1),0) AS VARCHAR)
			+ ':' + CAST(REPLACE(STR((s.[duration] % 3600/60),2),SPACE(1),0) AS VARCHAR)
			+ ':' + CAST(REPLACE(STR((s.[duration] % 60),2),SPACE(1),0) AS VARCHAR)	
			,''
			,td = CASE 
				WHEN s.[connection_type] = 1 THEN 'LAN'
				WHEN s.[connection_type] = 2 THEN 'Dialup'
				WHEN s.[connection_type] = 3 THEN 'Web Sync'
			END
			,''
			,td = CASE
				WHEN s.[runstatus] = 1 THEN 'Start'
				WHEN s.[runstatus] = 2 THEN 'Succeed'
				WHEN s.[runstatus] = 3 THEN 'In Progress'
				WHEN s.[runstatus] = 4 THEN 'Idle'
				WHEN s.[runstatus] = 5 THEN 'Retry'
				WHEN s.[runstatus] = 6 THEN 'Error'
			END
			,''
			,td = DATEDIFF(d,s.[start_time],getdate())
			,''
			,td = CASE 
				WHEN (DATEDIFF(d,s.[start_time],getdate()) < CAST((CAST(t.[value] AS DECIMAL)/100)* m.[retention] AS INT)) THEN 'Good'
				WHEN (DATEDIFF(d,s.[start_time],getdate()) <= m.[retention]) THEN 'Expiring Soon'
				WHEN (DATEDIFF(d,s.[start_time],getdate()) > m.[retention]) THEN 'Expired'
			END
			--,m.[retention]
		FROM [Distribution]..[MSmerge_sessions] s
			JOIN [Distribution]..[MSmerge_agents] a ON s.agent_id = a.id
			JOIN [Distribution]..[MSreplication_monitordata] m ON a.id = m.agent_id
			JOIN [Distribution]..[MSpublicationthresholds] t ON m.publication_id = t.publication_id
		WHERE s.end_time IN
		(
			SELECT TOP 1 s1.end_time 
			FROM [Distribution]..[MSmerge_sessions] s1
			WHERE s.agent_id = s1.agent_id
			ORDER BY s1.end_time DESC
		) AND DATEDIFF(d,s.[start_time],getdate()) >= CAST((CAST(t.[value] AS DECIMAL)/100)* m.[retention] AS INT)
		AND t.[isenabled] = 1
		ORDER BY s.[start_time]    
		    
			FOR XML PATH('tr'), TYPE 
			) AS NVARCHAR(MAX) ) +
			N'</table>' ;

		EXEC msdb.dbo.sp_send_dbmail 
			@profile_name = 'WorkingNotifier',
			@recipients='you@yourdomain.com',
			@copy_recipients='someone@somewhere.com',
			@subject = 'Expiring Subscription Report',
			@body = @tableHTML,
			@body_format = 'HTML' ;
		
		
	END
ELSE
	BEGIN
		PRINT 'No Records Found!'
	END