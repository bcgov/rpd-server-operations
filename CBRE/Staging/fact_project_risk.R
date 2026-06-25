# Set necessary variables
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

# Connect to SQL database
con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = SQL_SERVER,
  database = DB_NAME,
  Trusted_Connection = "Yes"
)

raw_data <- call_cbre_api(
  CBRE_TABLE_NAME,
  start_time = etl_window$cbre_start_time,
  end_time = etl_window$cbre_end_time
)

if (raw_data$status == "partial") {
  # True API/network failure
  error_msg <- paste0(
    "API extraction failed for table '",
    CBRE_TABLE_NAME,
    "' ",
    "(window ",
    etl_window$start_time,
    " to ",
    etl_window$end_time,
    "): ",
    raw_data$error
  )
  log_daily_etl_run(
    api_name = API_NAME,
    script_name = SCRIPT_NAME,
    table_name = TABLE_NAME,
    duration = as.numeric(difftime(Sys.time(), task_start, units = "secs")),
    status = "FAILURE",
    message = error_msg
  )
  stop(error_msg)
}

if (raw_data$status == "no_data") {
  # API succeeded, nothing to load
  no_data_msg <- paste0(
    "No data returned from API for window ",
    etl_window$start_time,
    " to ",
    etl_window$end_time
  )
  cat(no_data_msg, "— nothing to load. Exiting gracefully.\n")
  log_daily_etl_run(
    api_name = API_NAME,
    script_name = SCRIPT_NAME,
    table_name = TABLE_NAME,
    duration = as.numeric(difftime(Sys.time(), task_start, units = "secs")),
    status = "NO_DATA",
    message = no_data_msg
  )
  cond <- structure(
    class = c("no_data_condition", "condition"),
    list(message = no_data_msg)
  )
  stop(cond)
}
clean_data <- raw_data |>
  purrr::pluck("data") |>
  # select_if(~ !all(is.na(.))) |>
  # select_if(~ !all(. == 0)) |>
  # select_if(~ !all(. == '-1')) |>
  # select_if(~ !all(. == "N/A")) |>
  # select_if(~ !all(. == "-")) |>
  mutate(RefreshDate = as.POSIXct(Sys.time())) |>
  mutate(
    across(
      c(
        edp_update_ts,
        source_created_ts,
        source_modified_ts
      ),
      ~ as.POSIXct(.x, format = "%Y-%m-%dT%H:%M:%OSZ")
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
