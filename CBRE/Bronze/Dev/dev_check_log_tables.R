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
  # driver = "ODBC Driver 18 for SQL Server",
  server = SQL_SERVER,
  database = DB_NAME,
  Trusted_Connection = "Yes",
  # TrustServerCertificate = "Yes"
)

# Query SQL Datasets ####
query <- dbSendQuery(con, "SELECT * FROM ServerLogs.InfBronze")
InfBronze_logs <- dbFetch(query, n = -1)
dbClearResult(query)


# archibus_bl ####
query <- dbSendQuery(con, "SELECT * FROM InfBronze.archibus_bl")
archibus_bl <- dbFetch(query, n = -1)
dbClearResult(query)

new <- archibus_bl |>
  filter(bronze_batch_id == "20261007120019")

test <- archibus_bl |>
  group_by(bl_bl_id_key) |>
  mutate(count = n()) |>
  filter(count > 1) |>
  arrange(bl_bl_id_key, bronze_load_ts)

# archibus_dp ####
query <- dbSendQuery(con, "SELECT * FROM InfBronze.archibus_dp")
archibus_dp <- dbFetch(query, n = -1)
dbClearResult(query)

test <- archibus_dp |>
  group_by(dp_dv_id, dp_dp_id) |>
  mutate(count = n()) |>
  filter(count > 1) |>
  arrange(dp_dv_id, dp_dp_id, bronze_load_ts)

set1 <- test[1, ]
set2 <- test[2, ]
set3 <- test[3, ]
set4 <- test[4, ]

df <- data.frame(
  Row_1 = t(set1),
  Row_2 = t(set2),
  Row_3 = t(set3),
  Row_4 = t(set4)
) |>
  filter(Row_1 != Row_2)

set1 <- test[5, ]
set2 <- test[6, ]
set3 <- test[7, ]
set4 <- test[8, ]

df <- data.frame(
  Row_1 = t(set1),
  Row_2 = t(set2),
  Row_3 = t(set3),
  Row_4 = t(set4)
) |>
  filter(Row_1 != Row_2)

repeating_group <- archibus_dp |>
  group_by(dp_dv_id, dp_dp_id) |>
  mutate(count = n()) |>
  filter(count > 1) |>
  select(
    dp_name,
    dp_customer_category,
    dp_dv_id,
    dp_dp_id,
    dp_status,
    dp_option2
  ) |>
  distinct()

# archibus_costsheet_v ####
query <- dbSendQuery(con, "SELECT * FROM InfBronze.archibus_costsheet_v")
archibus_costsheet_v <- dbFetch(query, n = -1)
dbClearResult(query)

test <- archibus_costsheet_v |>
  group_by(costsheet_v_ls_id) |>
  mutate(count = n()) |>
  filter(count > 1) |>
  arrange(costsheet_v_ls_id, bronze_load_ts)

set1 <- test[5, ]
set2 <- test[6, ]

df <- data.frame(
  Row_1 = t(set1),
  Row_2 = t(set2)
) |>
  filter(Row_1 != Row_2)

# archibus_fl ####
query <- dbSendQuery(con, "SELECT * FROM InfBronze.archibus_fl")
archibus_fl <- dbFetch(query, n = -1)
dbClearResult(query)

test <- archibus_fl |>
  group_by(fl_bl_id, fl_fl_id) |>
  mutate(count = n()) |>
  filter(count > 1) |>
  arrange(fl_bl_id, fl_fl_id, bronze_load_ts)

# archibus_ls ####
query <- dbSendQuery(con, "SELECT * FROM InfBronze.archibus_ls")
archibus_ls <- dbFetch(query, n = -1)
dbClearResult(query)

new <- archibus_ls |>
  filter(bronze_batch_id == "20261008120232")

test <- archibus_ls |>
  group_by(ls_ls_id) |>
  mutate(count = n()) |>
  filter(count > 1)

test <- archibus_ls |>
  group_by(ls_ls_id) |>
  mutate(count = n()) |>
  filter(count > 1) |>
  arrange(ls_ls_id, bronze_load_ts)

check <- test |>
  filter(ls_ls_id == "PLA00001353")

set1 <- test[1, ]
set2 <- test[2, ]

df <- data.frame(Row_1 = t(set1), Row_2 = t(set2)) |>
  filter(Row_1 != Row_2)

# archibus_property ####
query <- dbSendQuery(con, "SELECT * FROM InfBronze.archibus_property")
archibus_property <- dbFetch(query, n = -1)
dbClearResult(query)

test <- archibus_property |>
  group_by(property_pr_id) |>
  mutate(count = n()) |>
  filter(count > 1) |>
  arrange(property_pr_id, bronze_load_ts)

check <- test |>
  filter(property_pr_id == "N2000556")

set1 <- test[1, ]
set2 <- test[2, ]

df <- data.frame(Row_1 = t(set1), Row_2 = t(set2)) |>
  filter(Row_1 != Row_2)

# archibus_rmpct ####
# Monitor date last calc and see if it changes every time or this is just a one off.
query <- dbSendQuery(con, "SELECT * FROM InfBronze.archibus_rmpct")
archibus_rmpct <- dbFetch(query, n = -1)
dbClearResult(query)

test <- archibus_rmpct |>
  group_by(rmpct_pct_id) |>
  mutate(count = n()) |>
  filter(count > 1) |>
  arrange(rmpct_pct_id, bronze_load_ts) |>
  filter(rmpct_pct_id == "38418")

set1 <- test[1, ]
set2 <- test[2, ]
set3 <- test[3, ]
set4 <- test[4, ]

df <- data.frame(
  Row_1 = t(set1),
  Row_2 = t(set2),
  Row_3 = t(set3),
  Row_4 = t(set4)
) |>
  filter(Row_1 != Row_2)
# Tidy up Server logs ####
sql <- glue::glue_sql(
  "DELETE FROM ServerLogs.InfBronze
  WHERE batch_id IN ('20261008120357', '20261007120339', '20261006120154')",
  .con = con
)

dbExecute(conn = con, statement = sql)

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
