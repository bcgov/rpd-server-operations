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
TABLE_NAME <- "es_fact_invoice"
CBRE_TABLE_NAME <- "es_fact_invoice_vw"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
API_NAME <- "CBRE"
SCRIPT_NAME <- "es_fact_invoice"

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
  # purrr::pluck("data") |>
  # comment out these after initial data analysis as risk of
  # losing columns in small data loads
  # select_if(~ !all(is.na(.))) |>
  # select_if(~ !all(. == 0)) |>
  # select_if(~ !all(. == '-1')) |>
  # select_if(~ !all(. == "N/A")) |>
  # select_if(~ !all(. == "-")) |>
  select(
    property_skey,
    invoice_skey,
    invoice_item_skey,
    vendor_skey,
    service_account_skey,
    service_type,
    service_type_secondary,
    utility_type_group,
    service_start_date,
    service_end_date,
    actual_usage,
    actual_usage_uom,
    service_cost_optimized_amount,
    service_cost_tax_amount,
    source_unique_id,
    source_system_code,
    edp_update_ts,
    edp_create_ts
  ) |>
  mutate(RefreshDate = as.POSIXct(Sys.time()), .before = everything()) |>
  mutate(
    across(
      c(
        edp_update_ts,
        edp_create_ts,
        service_start_date,
        service_end_date
      ),
      ~ as.POSIXct(.x, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC")
    )
  ) |>
  mutate(
    across(
      c(
        actual_usage,
        service_cost_optimized_amount,
        service_cost_tax_amount
      ),
      as.double
    )
  ) |>
  mutate(
    across(
      c(
        property_skey,
        invoice_skey,
        invoice_item_skey,
        vendor_skey,
        service_account_skey
      ),
      as.character
    )
  )


# emissions <- raw_data |>
#   select(
#     utility_type_group,
#     service_type,
#     service_type_secondary,
#     uncertainty_factor,
#     # vendor_skey,
#     co2e_rate,
#     factor_co2,
#     factor_ch4,
#     factor_n2o,
#     gj_factor,
#     edp_create_ts,
#     edp_update_ts
#   ) |>
#   group_by(
#     utility_type_group,
#     service_type,
#     service_type_secondary,
#     # vendor_skey,
#     uncertainty_factor,
#     co2e_rate,
#     factor_co2,
#     factor_ch4,
#     factor_n2o,
#     gj_factor
#   ) |>
#   summarise(
#     edp_create_ts = max(edp_create_ts),
#     edp_update_ts = max(edp_update_ts),
#     count = n(),
#     .groups = "drop_last"
#   ) |>
#   ungroup() |>
#   mutate(
#     across(
#       c(
#         uncertainty_factor,
#         co2e_rate,
#         factor_co2,
#         factor_ch4,
#         factor_n2o,
#         gj_factor
#       ),
#       as.double
#     )
#   ) |>
#   mutate(RefreshDate = as.POSIXct(Sys.time()), .before = everything()) |>
#   mutate(
#     across(
#       c(
#         edp_create_ts,
#         edp_update_ts
#       ),
#       ~ as.POSIXct(.x, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC")
#     )
#   )

# dbRemoveTable(con, TARGET_TABLE)
if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- paste0(
    "CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    TABLE_NAME,
    " (
      RefreshDate                   DATETIME2(3)   NOT NULL,
      property_skey                 NVARCHAR(15)   NOT NULL,
      invoice_skey                  NVARCHAR(15)   NOT NULL,
      invoice_item_skey             NVARCHAR(15)   NULL,
      vendor_skey                   NVARCHAR(15)   NULL,
      service_account_skey          NVARCHAR(12)   NULL,
      service_type                  NVARCHAR(35)   NULL,
      service_type_secondary        NVARCHAR(60)   NULL,
      utility_type_group            NVARCHAR(25)   NULL,
      service_start_date            DATETIME2(3)   NULL,
      service_end_date              DATETIME2(3)   NULL,
      actual_usage                  DECIMAL(18,4)  NULL,
      actual_usage_uom              NVARCHAR(15)   NULL,
      service_cost_optimized_amount DECIMAL(18,2)  NULL,
      service_cost_tax_amount       DECIMAL(18,2)  NULL,
      source_unique_id              NVARCHAR(115)  NULL,
      source_system_code            NVARCHAR(10)   NULL,
      edp_update_ts                 DATETIME2(3)   NULL,
      edp_create_ts                 DATETIME2(3)   NULL
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
          RefreshDate                   DATETIME2(3)   NOT NULL,
          property_skey                 NVARCHAR(15)   NOT NULL,
          invoice_skey                  NVARCHAR(15)   NOT NULL,
          invoice_item_skey             NVARCHAR(15)   NULL,
          vendor_skey                   NVARCHAR(15)   NULL,
          service_account_skey          NVARCHAR(12)   NULL,
          service_type                  NVARCHAR(35)   NULL,
          service_type_secondary        NVARCHAR(60)   NULL,
          utility_type_group            NVARCHAR(25)   NULL,
          service_start_date            DATETIME2(3)   NULL,
          service_end_date              DATETIME2(3)   NULL,
          actual_usage                  DECIMAL(18,4)  NULL,
          actual_usage_uom              NVARCHAR(15)   NULL,
          service_cost_optimized_amount DECIMAL(18,2)  NULL,
          service_cost_tax_amount       DECIMAL(18,2)  NULL,
          source_unique_id              NVARCHAR(115)  NULL,
          source_system_code            NVARCHAR(10)   NULL,
          edp_update_ts                 DATETIME2(3)   NULL,
          edp_create_ts                 DATETIME2(3)   NULL
  );
  "
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
           SELECT property_skey, invoice_skey
           FROM ",
        TEMP_TABLE,
        "
           GROUP BY property_skey, invoice_skey
           HAVING COUNT(*) > 1
         ) dupes;"
      )
    )$n

    if (dup_count > 0) {
      stop(paste0(
        "Duplicate property_skey and invoice_skey values detected in source data (",
        dup_count,
        " keys affected). Rolling back."
      ))
    }

    # Update existing rows in the target table
    n_updated <- dbExecute(
      con,
      paste0(
        "UPDATE tgt
         SET
           tgt.RefreshDate                   = src.RefreshDate,
           tgt.invoice_item_skey             = src.invoice_item_skey,
           tgt.vendor_skey                   = src.vendor_skey,
           tgt.service_account_skey          = src.service_account_skey,
           tgt.service_type                  = src.service_type,
           tgt.service_type_secondary        = src.service_type_secondary,
           tgt.utility_type_group            = src.utility_type_group,
           tgt.service_start_date            = src.service_start_date,
           tgt.service_end_date              = src.service_end_date,
           tgt.actual_usage                  = src.actual_usage,
           tgt.actual_usage_uom              = src.actual_usage_uom,
           tgt.service_cost_optimized_amount = src.service_cost_optimized_amount,
           tgt.service_cost_tax_amount       = src.service_cost_tax_amount,
           tgt.source_unique_id              = src.source_unique_id,
           tgt.source_system_code            = src.source_system_code,
           tgt.edp_update_ts                 = src.edp_update_ts,
           tgt.edp_create_ts                 = src.edp_create_ts
         FROM ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
         JOIN ",
        TEMP_TABLE,
        " src
           ON  tgt.property_skey = src.property_skey
           AND tgt.invoice_skey  = src.invoice_skey;"
      )
    )

    # Insert new rows not already in the target
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
           invoice_skey,
           invoice_item_skey,
           vendor_skey,
           service_account_skey,
           service_type,
           service_type_secondary,
           utility_type_group,
           service_start_date,
           service_end_date,
           actual_usage,
           actual_usage_uom,
           service_cost_optimized_amount,
           service_cost_tax_amount,
           source_unique_id,
           source_system_code,
           edp_update_ts,
           edp_create_ts
         )
         SELECT
           src.RefreshDate,
           src.property_skey,
           src.invoice_skey,
           src.invoice_item_skey,
           src.vendor_skey,
           src.service_account_skey,
           src.service_type,
           src.service_type_secondary,
           src.utility_type_group,
           src.service_start_date,
           src.service_end_date,
           src.actual_usage,
           src.actual_usage_uom,
           src.service_cost_optimized_amount,
           src.service_cost_tax_amount,
           src.source_unique_id,
           src.source_system_code,
           src.edp_update_ts,
           src.edp_create_ts
         FROM ",
        TEMP_TABLE,
        " AS src
         LEFT JOIN ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " AS tgt
           ON  tgt.property_skey = src.property_skey
           AND tgt.invoice_skey  = src.invoice_skey
         WHERE tgt.property_skey IS NULL
            OR tgt.invoice_skey  IS NULL;"
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
