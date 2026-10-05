# Load helper functions
source(here::here("utilities/utilities.R"))

# Load libraries
library(base64enc, quietly = TRUE, warn.conflicts = FALSE)
library(dplyr, quietly = TRUE, warn.conflicts = FALSE)
library(here, quietly = TRUE, warn.conflicts = FALSE)
library(httr2, quietly = TRUE, warn.conflicts = FALSE)
library(jsonlite, quietly = TRUE, warn.conflicts = FALSE)
library(lubridate, quietly = TRUE, warn.conflicts = FALSE)
library(purrr, quietly = TRUE, warn.conflicts = FALSE)
library(tibble, quietly = TRUE, warn.conflicts = FALSE)
library(tidyr, quietly = TRUE, warn.conflicts = FALSE)

library(odbc, quietly = TRUE, warn.conflicts = FALSE)
library(DBI, quietly = TRUE, warn.conflicts = FALSE)

# Setup necessary variables
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

# Query SQL Datasets ####
query <- dbSendQuery(con, "SELECT * FROM ServerLogs.InfBronze")
archibus_bl_logs <- dbFetch(query, n = -1)
dbClearResult(query)

sql <- glue::glue_sql(
  "DELETE FROM ServerLogs.InfBronze
  WHERE batch_id IN ('20261001185315')",
  .con = con
)

dbExecute(conn = con, statement = sql)


query <- dbSendQuery(con, "SELECT * FROM InfBronze.archibus_bl")
archibus_bl <- dbFetch(query, n = -1)
dbClearResult(query)

test <- archibus_bl |>
  group_by(bl_bl_id_key) |>
  mutate(count = n()) |>
  filter(count > 1)

query <- dbSendQuery(con, "SELECT * FROM InfBronze.archibus_rmpct")
archibus_rmpct <- dbFetch(query, n = -1)
dbClearResult(query)

# Update ServerLogs.InfBronze ####

# add Missing column
sql <- glue::glue_sql(
  "ALTER TABLE ServerLogs.InfBronze
  ADD Missing INT DEFAULT 0;",
  .con = con
)
dbExecute(conn = con, statement = sql)

# add duration column
sql <- glue::glue_sql(
  "ALTER TABLE ServerLogs.InfBronze
  ADD Duration DECIMAL(10,2);",
  .con = con
)
dbExecute(conn = con, statement = sql)

# Okay the DEFAULT call didn't work, have three rows we need to update
# sql <- glue::glue_sql(
#   "UPDATE ServerLogs.InfBronze
#   SET Missing = 0, Duration = 7.40
#   WHERE batch_id = '20261001120026';",
#   .con = con
# )
# dbExecute(conn = con, statement = sql)

# sql <- glue::glue_sql(
#   "UPDATE ServerLogs.InfBronze
#   SET Missing = 0, Duration = 8.30
#   WHERE batch_id = '20260930120022';",
#   .con = con
# )
# dbExecute(conn = con, statement = sql)

# sql <- glue::glue_sql(
#   "UPDATE ServerLogs.InfBronze
#   SET Missing = 0, Duration = 7.90
#   WHERE batch_id = '20260929155140';",
#   .con = con
# )
# dbExecute(conn = con, statement = sql)

# Okay three rows updated and should be good to go. Just need to implement in regular script
# sql <- glue::glue_sql(
#   "UPDATE ServerLogs.InfBronze
#   SET Duration = 8.8
#   WHERE batch_id = '20261005120021';",
#   .con = con
# )
# dbExecute(conn = con, statement = sql)
