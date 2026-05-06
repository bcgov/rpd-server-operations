ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "CbreStaging"
TABLE_NAME <- "fact_project_risk"
CBRE_TABLE_NAME <- "fact_project_risk_vw"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
API_NAME <- "CBRE"
SCRIPT_NAME <- "fact_project_risk"

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

# Load helper functions
source(here::here("./utilities/R/cbre_api_function.R"))
source(here::here("./utilities/R/event_logger.R"))

# Connect to SQL database
con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = SQL_SERVER,
  database = DB_NAME,
  Trusted_Connection = "Yes"
)

raw_data <- extract_cbre_data(CBRE_TABLE_NAME)

clean_data <- raw_data |>
  select_if(~ !all(is.na(.))) |>
  select_if(~ !all(. == 0)) |>
  select_if(~ !all(. == '-1')) |>
  select_if(~ !all(. == "N/A")) |>
  select_if(~ !all(. == "-")) |>
  mutate(RefreshDate = as.POSIXct(Sys.time())) |>
  mutate(
    across(
      c(
        edp_update_ts,
        source_created_ts,
        source_modified_ts
      ),
      ~ as.POSIXct(.x, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC")
    )
  ) |>
  mutate(
    across(
      c(
        reported_date,
        closed_date,
        date_of_impact,
        last_update_date
      ),
      as.POSIXct
    )
  ) |>
  mutate(
    across(
      c(
        potential_schedule_impact,
        potential_budget_impact,
        actual_schedule_impact,
        actual_budget_impact
      ),
      as.double
    )
  ) |>
  select(
    project_skey,
    risk_skey,
    responsible_contact_skey,
    edp_update_ts,
    project_phase,
    project_category,
    reported_date,
    last_update_date,
    risk_id,
    risk_profile,
    risk_desc,
    risk_number,
    risk_rating,
    risk_status,
    risk_mitigation,
    potential_schedule_impact,
    potential_budget_impact,
    impact_probability,
    date_of_impact,
    impact_desc,
    timeline_to_impact,
    actual_budget_impact,
    actual_schedule_impact,
    impact_severity,
    resolution_reason,
    closed_date,
    source_created_ts,
    source_modified_ts,
    source_unique_id,
    source_partition_id,
    source_system_code
  )

dbWriteTable(con, TARGET_TABLE, clean_data)
