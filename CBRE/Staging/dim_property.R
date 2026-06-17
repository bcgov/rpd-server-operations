# For server logging
# Begin timer
task_start <- Sys.time()

# Setup necessary variables
ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "CbreStaging"
TABLE_NAME <- "dim_property"
CBRE_TABLE_NAME <- "dim_property_vw"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
API_NAME <- "CBRE"
SCRIPT_NAME <- "dim_property"

# Connect to SQL database
con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = SQL_SERVER,
  database = DB_NAME,
  Trusted_Connection = "Yes"
)

# Query API
raw_data <- call_cbre_api(
  CBRE_TABLE_NAME,
  start_time = etl_window$start_time,
  end_time = etl_window$end_time
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
  # # comment out these after initial data analysis as risk of
  # # losing columns in small data loads
  # select_if(~ !all(is.na(.))) |>
  # select_if(~ !all(. == 0)) |>
  # select_if(~ !all(. == '-1')) |>
  # select_if(~ !all(. == "N/A")) |>
  # select_if(~ !all(. == "-")) |>
  select(
    property_skey,
    client_property_id,
    client_property_name,
    reporting_code_3,
    client_additional_attrib,
    address_line1,
    city_name,
    state_province_code,
    zip_code,
    country_code,
    associated_project_number,
    location_id,
    source_unique_id,
    source_system_code,
    edp_update_ts
  ) |>
  mutate(RefreshDate = as.POSIXct(Sys.time()), .before = everything()) |>
  mutate(
    Identifier = case_when(
      source_system_code == "JDE" ~ reporting_code_3,
      source_system_code == "SI7" ~ stringr::str_extract(
        client_additional_attrib,
        '([B-N]\\d*)',
        group = TRUE
      )
    ),
    .keep = "unused",
    .after = client_property_name
  ) |>
  mutate(
    across(
      c(
        property_skey
      ),
      as.character
    )
  ) |>
  mutate(
    across(
      c(
        edp_update_ts
      ),
      ~ as.POSIXct(.x, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC")
    )
  )

# dbRemoveTable(con, TARGET_TABLE)
if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- paste0(
    "CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    TABLE_NAME,
    " (
      RefreshDate                    DATETIME2(3)   NOT NULL,
      property_skey                  NVARCHAR(20)   NOT NULL,
      client_property_id             NVARCHAR(20)   NULL,
      client_property_name           NVARCHAR(100)  NULL,
      Identifier                     NVARCHAR(100)  NULL,
      address_line1                  NVARCHAR(100)  NULL,
      city_name                      NVARCHAR(100)  NULL,
      state_province_code            NVARCHAR(10)   NULL,
      zip_code                       NVARCHAR(30)   NULL,
      country_code                   NVARCHAR(10)   NULL,
      associated_project_number      NVARCHAR(20)   NULL,
      location_id                    NVARCHAR(10)   NULL,
      source_unique_id               NVARCHAR(20)   NULL,
      source_system_code             NVARCHAR(10)   NULL,
      edp_update_ts                  DATETIME2(3)   NULL
    );"
  )

  dbExecute(con, sql)
}

# Database Transaction ####
etl_error <- NULL

dbBegin(con)

tryCatch(
  {
    if (dbExistsTable(con, TEMP_TABLE)) {
      dbRemoveTable(con, TEMP_TABLE)
    }

    dbExecute(
      con,
      paste0(
        "CREATE TABLE ",
        TEMP_TABLE,
        " (
          RefreshDate                    DATETIME2(3)   NOT NULL,
          property_skey                  NVARCHAR(20)   NOT NULL,
          client_property_id             NVARCHAR(20)   NULL,
          client_property_name           NVARCHAR(100)  NULL,
          Identifier                     NVARCHAR(100)  NULL,
          address_line1                  NVARCHAR(100)  NULL,
          city_name                      NVARCHAR(100)  NULL,
          state_province_code            NVARCHAR(10)   NULL,
          zip_code                       NVARCHAR(30)   NULL,
          country_code                   NVARCHAR(10)   NULL,
          associated_project_number      NVARCHAR(20)   NULL,
          location_id                    NVARCHAR(10)   NULL,
          source_unique_id               NVARCHAR(20)   NULL,
          source_system_code             NVARCHAR(10)   NULL,
          edp_update_ts                  DATETIME2(3)   NULL
        );"
      )
    )

    dbWriteTable(
      con,
      name = TEMP_TABLE,
      value = clean_data,
      append = TRUE,
      overwrite = FALSE
    )

    # -- Guard: catch duplicate keys in source data before touching target --
    dup_count <- dbGetQuery(
      con,
      paste0(
        "SELECT COUNT(*) AS n
         FROM (
           SELECT property_skey
           FROM ",
        TEMP_TABLE,
        "
           GROUP BY property_skey
           HAVING COUNT(*) > 1
         ) dupes;"
      )
    )$n

    if (dup_count > 0) {
      stop(paste0(
        "Duplicate property_skey values detected in source data (",
        dup_count,
        " keys affected). Rolling back."
      ))
    }

    # -- Update matched rows --
    n_updated <- dbExecute(
      con,
      paste0(
        "UPDATE tgt
         SET
           tgt.RefreshDate               = src.RefreshDate,
           tgt.client_property_id        = src.client_property_id,
           tgt.client_property_name      = src.client_property_name,
           tgt.Identifier                = src.Identifier,
           tgt.address_line1             = src.address_line1,
           tgt.city_name                 = src.city_name,
           tgt.state_province_code       = src.state_province_code,
           tgt.zip_code                  = src.zip_code,
           tgt.country_code              = src.country_code,
           tgt.associated_project_number = src.associated_project_number,
           tgt.location_id               = src.location_id,
           tgt.source_unique_id          = src.source_unique_id,
           tgt.source_system_code        = src.source_system_code,
           tgt.edp_update_ts             = src.edp_update_ts
         FROM ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
         INNER JOIN ",
        TEMP_TABLE,
        " src
           ON tgt.property_skey = src.property_skey;"
      )
    )

    # -- Insert new rows --
    n_inserted <- dbExecute(
      con,
      paste0(
        "INSERT INTO ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " (
          RefreshDate,
          property_skey,
          client_property_id,
          client_property_name,
          Identifier,
          address_line1,
          city_name,
          state_province_code,
          zip_code,
          country_code,
          associated_project_number,
          location_id,
          source_unique_id,
          source_system_code,
          edp_update_ts
        )
        SELECT
          src.RefreshDate,
          src.property_skey,
          src.client_property_id,
          src.client_property_name,
          src.Identifier,
          src.address_line1,
          src.city_name,
          src.state_province_code,
          src.zip_code,
          src.country_code,
          src.associated_project_number,
          src.location_id,
          src.source_unique_id,
          src.source_system_code,
          src.edp_update_ts
        FROM ",
        TEMP_TABLE,
        " src
        LEFT JOIN ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
          ON tgt.property_skey = src.property_skey
        WHERE tgt.property_skey IS NULL;"
      )
    )

    dbCommit(con)

    n_updated <<- n_updated
    n_inserted <<- n_inserted

    cat("ETL complete — updated:", n_updated, "| inserted:", n_inserted, "\n")
  },
  error = function(e) {
    dbRollback(con)
    etl_error <<- e
  }
)

task_end <- Sys.time()
task_duration <- interval(task_start, task_end) / dseconds()

if (is.null(etl_error)) {
  log_daily_etl_run(
    api_name = API_NAME,
    script_name = SCRIPT_NAME,
    table_name = TABLE_NAME,
    duration = task_duration,
    status = "SUCCESS",
    n_inserted = n_inserted,
    n_updated = n_updated,
    n_deleted = NA,
    message = "ETL completed successfully"
  )
} else {
  log_daily_etl_run(
    api_name = API_NAME,
    script_name = SCRIPT_NAME,
    table_name = TABLE_NAME,
    status = "FAILURE",
    message = substr(etl_error$message, 1, 500)
  )
  stop(etl_error)
}
