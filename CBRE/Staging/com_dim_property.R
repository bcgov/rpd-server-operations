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
TABLE_NAME <- "com_dim_property"
CBRE_TABLE_NAME <- "com_dim_property_vw"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
API_NAME <- "CBRE"
SCRIPT_NAME <- "com_dim_property"

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
  # # comment out these after initial data analysis as risk of
  # # losing columns in small data loads
  # select_if(~ !all(is.na(.))) |>
  # select_if(~ !all(. == 0)) |>
  # select_if(~ !all(. == '-1')) |>
  # select_if(~ !all(. == "N/A")) |>
  # select_if(~ !all(. == "-")) |>
  mutate(
    across(
      c(
        closed_date,
        edp_create_ts,
        edp_update_ts
      ),
      ~ as.POSIXct(.x, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC")
    )
  ) |>
  mutate(
    across(
      c(
        property_size,
      ),
      as.double
    )
  ) |>
  mutate(
    across(
      c(
        property_skey,
        market_skey,
        submarket_skey,
        cost_model_skey,
        cost_location_skey,
        cbre_standardized_property_type_skey,
        cbre_standardized_property_skey
      ),
      as.character
    )
  ) |>
  mutate(RefreshDate = as.POSIXct(Sys.time())) |>
  select(
    RefreshDate,
    property_skey,
    property_status,
    closed_date,
    property_size,
    property_size_uom,
    state_province_code,
    address_line1,
    client_property_name,
    client_property_id,
    property_master_id,
    alternate_property_id,
    property_unique_id,
    cbre_standardized_property_skey,
    associated_project_number,
    property_type,
    region = user_defined_field_1,
    user_defined_field_2,
    reporting_code_1,
    reporting_code_2,
    reporting_code_3,
    reporting_code_4,
    reporting_code_5,
    reporting_code_6,
    reporting_code_7,
    cbre_standardized_property_type_name,
    market_skey,
    market_name,
    submarket_skey,
    submarket_name,
    cost_model_skey,
    cost_model_name,
    cost_location_skey,
    cost_location_name,
    cbre_standardized_property_type_skey,
    source_system_code,
    source_unique_id,
    edp_create_ts,
    edp_update_ts
  )

# dbRemoveTable(con, TARGET_TABLE)
if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- paste0(
    "CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    TABLE_NAME,
    " (
      RefreshDate                          DATETIME2(3)    NOT NULL,
      property_skey                        NVARCHAR(15)    NOT NULL,
      property_status                      NVARCHAR(15)    NULL,
      closed_date                          DATETIME2(3)    NULL,
      property_size                        DECIMAL(18,4)   NULL,
      property_size_uom                    NVARCHAR(15)    NULL,
      state_province_code                  NVARCHAR(25)    NULL,
      address_line1                        NVARCHAR(60)    NULL,
      client_property_name                 NVARCHAR(120)   NULL,
      client_property_id                   NVARCHAR(15)    NULL,
      property_master_id                   NVARCHAR(100)   NULL,
      alternate_property_id                NVARCHAR(25)    NULL,
      property_unique_id                   NVARCHAR(100)   NULL,
      cbre_standardized_property_skey      NVARCHAR(10)    NULL,
      associated_property_id               NVARCHAR(20)    NULL,
      property_type                        NVARCHAR(35)    NULL,
      region                               NVARCHAR(50)    NULL,
      user_defined_field_2                 NVARCHAR(45)    NULL,
      associated_project_number            NVARCHAR(15)    NULL,
      reporting_code_1                     NVARCHAR(15)    NULL,
      reporting_code_2                     NVARCHAR(5)     NULL,
      reporting_code_3                     NVARCHAR(15)    NULL,
      reporting_code_4                     NVARCHAR(15)    NULL,
      reporting_code_5                     NVARCHAR(15)    NULL,
      reporting_code_6                     NVARCHAR(5)     NULL,
      reporting_code_7                     NVARCHAR(5)     NULL,
      cbre_standardized_property_type_name NVARCHAR(35)    NULL,
      market_skey                          NVARCHAR(12)    NULL,
      market_name                          NVARCHAR(45)    NULL,
      submarket_skey                       NVARCHAR(12)    NULL,
      submarket_name                       NVARCHAR(35)    NULL,
      cost_model_skey                      NVARCHAR(5)     NULL,
      cost_model_name                      NVARCHAR(50)    NULL,
      cost_location_skey                   NVARCHAR(5)     NULL,
      cost_location_name                   NVARCHAR(30)    NULL,
      cbre_standardized_property_type_skey NVARCHAR(5)     NULL,
      source_system_code                   NVARCHAR(20)    NULL,
      source_unique_id                     NVARCHAR(70)    NULL,
      edp_create_ts                        DATETIME2(3)    NULL,
      edp_update_ts                        DATETIME2(3)    NULL
    );"
  )

  dbExecute(con, sql)
}

# Database Transaction ####
etl_error <- NULL

# Control database transaction to ensure all steps done together or not at all
dbBegin(con)

# Begin error handling and rollback of transaction on failure
tryCatch(
  {
    if (dbExistsTable(con, TEMP_TABLE)) {
      dbRemoveTable(con, TEMP_TABLE)
    }

    # Create temp table to hold new data
    dbExecute(
      con,
      paste0(
        "CREATE TABLE ",
        TEMP_TABLE,
        " (
          RefreshDate                          DATETIME2(3)    NOT NULL,
          property_skey                        NVARCHAR(15)    NOT NULL,
          property_status                      NVARCHAR(15)    NULL,
          closed_date                          DATETIME2(3)    NULL,
          property_size                        DECIMAL(18,4)   NULL,
          property_size_uom                    NVARCHAR(15)    NULL,
          state_province_code                  NVARCHAR(25)    NULL,
          address_line1                        NVARCHAR(60)    NULL,
          client_property_name                 NVARCHAR(120)   NULL,
          client_property_id                   NVARCHAR(15)    NULL,
          property_master_id                   NVARCHAR(100)   NULL,
          alternate_property_id                NVARCHAR(25)    NULL,
          property_unique_id                   NVARCHAR(100)   NULL,
          cbre_standardized_property_skey      NVARCHAR(10)    NULL,
          associated_property_id               NVARCHAR(20)    NULL,
          property_type                        NVARCHAR(35)    NULL,
          region                               NVARCHAR(50)    NULL,
          user_defined_field_2                 NVARCHAR(45)    NULL,
          associated_project_number            NVARCHAR(15)    NULL,
          reporting_code_1                     NVARCHAR(15)    NULL,
          reporting_code_2                     NVARCHAR(5)     NULL,
          reporting_code_3                     NVARCHAR(15)    NULL,
          reporting_code_4                     NVARCHAR(15)    NULL,
          reporting_code_5                     NVARCHAR(15)    NULL,
          reporting_code_6                     NVARCHAR(5)     NULL,
          reporting_code_7                     NVARCHAR(5)     NULL,
          cbre_standardized_property_type_name NVARCHAR(35)    NULL,
          market_skey                          NVARCHAR(12)    NULL,
          market_name                          NVARCHAR(45)    NULL,
          submarket_skey                       NVARCHAR(12)    NULL,
          submarket_name                       NVARCHAR(35)    NULL,
          cost_model_skey                      NVARCHAR(5)     NULL,
          cost_model_name                      NVARCHAR(50)    NULL,
          cost_location_skey                   NVARCHAR(5)     NULL,
          cost_location_name                   NVARCHAR(30)    NULL,
          cbre_standardized_property_type_skey NVARCHAR(5)     NULL,
          source_system_code                   NVARCHAR(20)    NULL,
          source_unique_id                     NVARCHAR(70)    NULL,
          edp_create_ts                        DATETIME2(3)    NULL,
          edp_update_ts                        DATETIME2(3)    NULL
          );"
      )
    )

    # Write the current tibble into the temp table
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

    # Update existing rows in the target table that have changed
    n_updated <- dbExecute(
      con,
      paste0(
        "
    UPDATE tgt
      SET
         tgt.RefreshDate                          = src.RefreshDate,
         tgt.property_status                      = src.property_status,
         tgt.closed_date                          = src.closed_date,
         tgt.property_size                        = src.property_size,
         tgt.property_size_uom                    = src.property_size_uom,
         tgt.state_province_code                  = src.state_province_code,
         tgt.address_line1                        = src.address_line1,
         tgt.client_property_name                 = src.client_property_name,
         tgt.client_property_id                   = src.client_property_id,
         tgt.property_master_id                   = src.property_master_id,
         tgt.alternate_property_id                = src.alternate_property_id,
         tgt.property_unique_id                   = src.property_unique_id,
         tgt.cbre_standardized_property_skey      = src.cbre_standardized_property_skey,
         tgt.associated_project_number            = src.associated_project_number,
         tgt.property_type                        = src.property_type,
         tgt.region                               = src.region,
         tgt.user_defined_field_2                 = src.user_defined_field_2,
         tgt.reporting_code_1                     = src.reporting_code_1,
         tgt.reporting_code_2                     = src.reporting_code_2,
         tgt.reporting_code_3                     = src.reporting_code_3,
         tgt.reporting_code_4                     = src.reporting_code_4,
         tgt.reporting_code_5                     = src.reporting_code_5,
         tgt.reporting_code_6                     = src.reporting_code_6,
         tgt.reporting_code_7                     = src.reporting_code_7,
         tgt.cbre_standardized_property_type_name = src.cbre_standardized_property_type_name,
         tgt.market_skey                          = src.market_skey,
         tgt.market_name                          = src.market_name,
         tgt.submarket_skey                       = src.submarket_skey,
         tgt.submarket_name                       = src.submarket_name,
         tgt.cost_model_skey                      = src.cost_model_skey,
         tgt.cost_model_name                      = src.cost_model_name,
         tgt.cost_location_skey                   = src.cost_location_skey,
         tgt.cost_location_name                   = src.cost_location_name,
         tgt.cbre_standardized_property_type_skey = src.cbre_standardized_property_type_skey,
         tgt.source_system_code                   = src.source_system_code,
         tgt.source_unique_id                     = src.source_unique_id,
         tgt.edp_create_ts                        = src.edp_create_ts,
         tgt.edp_update_ts                        = src.edp_update_ts
       FROM ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
       JOIN ",
        TEMP_TABLE,
        " src
         ON tgt.property_skey = src.property_skey;"
      )
    )

    # Insert data into the SQL table
    n_inserted <- dbExecute(
      con,
      paste0(
        "
    INSERT INTO ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " (
        RefreshDate,
           property_skey,
           property_status,
           closed_date,
           property_size,
           property_size_uom,
           state_province_code,
           address_line1,
           client_property_name,
           client_property_id,
           property_master_id,
           alternate_property_id,
           property_unique_id,
           cbre_standardized_property_skey,
           associated_project_number,
           property_type,
           region,
           user_defined_field_2,
           reporting_code_1,
           reporting_code_2,
           reporting_code_3,
           reporting_code_4,
           reporting_code_5,
           reporting_code_6,
           reporting_code_7,
           cbre_standardized_property_type_name,
           market_skey,
           market_name,
           submarket_skey,
           submarket_name,
           cost_model_skey,
           cost_model_name,
           cost_location_skey,
           cost_location_name,
           cbre_standardized_property_type_skey,
           source_system_code,
           source_unique_id,
           edp_create_ts,
           edp_update_ts
         )
         SELECT
           src.RefreshDate,
           src.property_skey,
           src.property_status,
           src.closed_date,
           src.property_size,
           src.property_size_uom,
           src.state_province_code,
           src.address_line1,
           src.client_property_name,
           src.client_property_id,
           src.property_master_id,
           src.alternate_property_id,
           src.property_unique_id,
           src.cbre_standardized_property_skey,
           src.associated_property_id,
           src.property_type,
           src.region,
           src.user_defined_field_2,
           src.reporting_code_1,
           src.reporting_code_2,
           src.reporting_code_3,
           src.reporting_code_4,
           src.reporting_code_5,
           src.reporting_code_6,
           src.reporting_code_7,
           src.cbre_standardized_property_type_name,
           src.market_skey,
           src.market_name,
           src.submarket_skey,
           src.submarket_name,
           src.cost_model_skey,
           src.cost_model_name,
           src.cost_location_skey,
           src.cost_location_name,
           src.cbre_standardized_property_type_skey,
           src.source_system_code,
           src.source_unique_id,
           src.edp_create_ts,
           src.edp_update_ts
         FROM ",
        TEMP_TABLE,
        " AS src
         LEFT JOIN ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " AS tgt
           ON tgt.property_skey = src.property_skey
         WHERE tgt.property_skey IS NULL;"
      )
    )

    # Complete the transaction
    dbCommit(con)

    # Hoist counts to outer scope for logging
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
