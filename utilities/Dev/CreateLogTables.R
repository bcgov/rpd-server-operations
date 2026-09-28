library(DBI)
library(odbc)
library(dplyr)

ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"

# Connect to SQL database
con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = SQL_SERVER,
  database = DB_NAME,
  Trusted_Connection = "Yes"
)

SCHEMA_NAME = "ServerLogs"
# ServerLogs.InfBronze
TABLE_NAME = "InfBronze"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
# dbRemoveTable(con, TARGET_TABLE)
if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- glue::glue(
    "CREATE TABLE
    {SCHEMA_NAME}.{TABLE_NAME}
    (
      load_ts                       DATETIME2(3) NOT NULL,
      batch_id                      BIGINT       NOT NULL,
      source_system                 VARCHAR(200) NOT NULL,
      source_table                  VARCHAR(200) NOT NULL,
      New                           INT          NOT NULL,
      Changed                       INT          NOT NULL,
      Unchanged                     INT          NOT NULL
    );"
  )
  dbExecute(con, sql)
}
