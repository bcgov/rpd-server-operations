/* 
Review server logs
*/

SELECT [load_ts]
      ,[batch_id]
      ,[source_system]
      ,[source_table]
      ,[New]
      ,[Changed]
      ,[Unchanged]
      ,[Missing]
      ,[Duration]
  FROM [BuildingIntelligence].[ServerLogs].[InfBronze]
  ORDER BY load_ts;

  SELECT [load_ts]
      ,[batch_id]
      ,[source_system]
      ,[source_table]
      ,[output_table]
      ,[data_rows]
      ,[duration]
  FROM [BuildingIntelligence].[ServerLogs].[InfSilver]
  ORDER BY load_ts;

  SELECT [load_ts]
      ,[batch_id]
      ,[source_system]
      ,[source_table]
      ,[New]
      ,[Changed]
      ,[Unchanged]
      ,[Missing]
      ,[Duration]
  FROM [BuildingIntelligence].[ServerLogs].[InfBronze]
  WHERE source_table = 'archibus_bl'
  ORDER BY load_ts
  ;