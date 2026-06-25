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
TABLE_NAME <- "pjm_dim_invoice"
CBRE_TABLE_NAME <- "pjm_dim_invoice_vw"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
API_NAME <- "CBRE"
SCRIPT_NAME <- "pjm_dim_invoice"

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
        source_modified_ts,
        source_created_ts,
        edp_update_ts
      ),
      ~ as.POSIXct(.x, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC")
    )
  ) |>
  mutate(
    across(
      c(
        date_submitted,
        date_approved,
        payment_date,
        period_from,
        period_to
      ),
      ~ as.POSIXct(.x, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC")
    )
  ) |>
  mutate(
    across(
      c(
        contract_original_amount,
        contract_approved_change_amount
      ),
      as.double
    )
  ) |>
  mutate(
    across(
      c(
        invoice_skey,
        invoice_id
      ),
      as.character
    )
  ) |>
  select(
    RefreshDate,
    invoice_skey,
    invoice_id,
    invoice_approval_status,
    date_submitted,
    date_approved,
    payment_date,
    period_from,
    period_to,
    contract_original_amount,
    contract_approved_change_amount,
    check_number,
    vendor_po_number,
    vendor_invoice_number,
    source_unique_id,
    source_partition_id,
    source_system_code,
    source_created_ts,
    source_modified_ts,
    edp_update_ts
  )

# Database Transaction ####
# dbRemoveTable(con, TARGET_TABLE)
if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- paste0(
    "CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    TABLE_NAME,
    " (
      RefreshDate                     DATETIME2(3)   NOT NULL,
      invoice_skey                    NVARCHAR(20)   NOT NULL,
      invoice_id                      NVARCHAR(20)   NULL,
      invoice_approval_status         NVARCHAR(20)   NULL,
      date_submitted                  DATETIME2(3)   NULL,
      date_approved                   DATETIME2(3)   NULL,
      payment_date                    DATETIME2(3)   NULL,
      period_from                     DATETIME2(3)   NULL,
      period_to                       DATETIME2(3)   NULL,
      contract_original_amount        DECIMAL(18,2)  NULL,
      contract_approved_change_amount DECIMAL(18,2)  NULL,
      check_number                    NVARCHAR(20)   NULL,
      vendor_po_number                NVARCHAR(100)  NULL,
      vendor_invoice_number           NVARCHAR(200)  NULL,
      source_unique_id                NVARCHAR(100)  NOT NULL,
      source_partition_id             NVARCHAR(20)   NULL,
      source_system_code              NVARCHAR(20)   NULL,
      source_created_ts               DATETIME2(3)   NULL,
      source_modified_ts              DATETIME2(3)   NULL,
      edp_update_ts                   DATETIME2(3)   NOT NULL
    );"
  )

  dbExecute(con, sql)
}

etl_error <- NULL

# Control database transaction to ensure all steps done together or not at all
dbBegin(con)

# Begin error handling and rollback of transaction on failure
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
        RefreshDate                     DATETIME2(3)   NOT NULL,
        invoice_skey                    NVARCHAR(20)   NOT NULL,
        invoice_id                      NVARCHAR(20)   NULL,
        invoice_approval_status         NVARCHAR(20)   NULL,
        date_submitted                  DATETIME2(3)   NULL,
        date_approved                   DATETIME2(3)   NULL,
        payment_date                    DATETIME2(3)   NULL,
        period_from                     DATETIME2(3)   NULL,
        period_to                       DATETIME2(3)   NULL,
        contract_original_amount        DECIMAL(18,2)  NULL,
        contract_approved_change_amount DECIMAL(18,2)  NULL,
        check_number                    NVARCHAR(20)   NULL,
        vendor_po_number                NVARCHAR(100)  NULL,
        vendor_invoice_number           NVARCHAR(200)  NULL,
        source_unique_id                NVARCHAR(100)  NOT NULL,
        source_partition_id             NVARCHAR(20)   NULL,
        source_system_code              NVARCHAR(20)   NULL,
        source_created_ts               DATETIME2(3)   NULL,
        source_modified_ts              DATETIME2(3)   NULL,
        edp_update_ts                   DATETIME2(3)   NOT NULL
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
           SELECT invoice_skey, invoice_id
           FROM ",
        TEMP_TABLE,
        "
           GROUP BY invoice_skey, invoice_id
           HAVING COUNT(*) > 1
         ) dupes;"
      )
    )$n

    if (dup_count > 0) {
      stop(paste0(
        "Duplicate invoice_skey, invoice_id values detected in source data (",
        dup_count,
        " keys affected). Rolling back."
      ))
    }

    # -- Update matched rows --
    n_updated <- dbExecute(
      con,
      paste0(
        "
      UPDATE tgt
      SET
        tgt.RefreshDate                     = src.RefreshDate,
        tgt.invoice_approval_status         = src.invoice_approval_status,
        tgt.date_submitted                  = src.date_submitted,
        tgt.date_approved                   = src.date_approved,
        tgt.payment_date                    = src.payment_date,
        tgt.period_from                     = src.period_from,
        tgt.period_to                       = src.period_to,
        tgt.contract_original_amount        = src.contract_original_amount,
        tgt.contract_approved_change_amount = src.contract_approved_change_amount,
        tgt.check_number                    = src.check_number,
        tgt.vendor_po_number                = src.vendor_po_number,
        tgt.vendor_invoice_number           = src.vendor_invoice_number,
        tgt.source_unique_id                = src.source_unique_id,
        tgt.source_partition_id             = src.source_partition_id,
        tgt.source_system_code              = src.source_system_code,
        tgt.source_created_ts               = src.source_created_ts,
        tgt.source_modified_ts              = src.source_modified_ts,
        tgt.edp_update_ts                   = src.edp_update_ts
      FROM ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
      JOIN ",
        TEMP_TABLE,
        " src
        ON  tgt.invoice_skey = src.invoice_skey
        AND tgt.invoice_id = src.invoice_id;"
      )
    )

    # Insert new rows that don't exist in the SQL table
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
        invoice_skey,
        invoice_id,
        invoice_approval_status,
        date_submitted,
        date_approved,
        payment_date,
        period_from,
        period_to,
        contract_original_amount,
        contract_approved_change_amount,
        check_number,
        vendor_po_number,
        vendor_invoice_number,
        source_unique_id,
        source_partition_id,
        source_system_code,
        source_created_ts,
        source_modified_ts,
        edp_update_ts
      )
      SELECT
        src.RefreshDate,
        src.invoice_skey,
        src.invoice_id,
        src.invoice_approval_status,
        src.date_submitted,
        src.date_approved,
        src.payment_date,
        src.period_from,
        src.period_to,
        src.contract_original_amount,
        src.contract_approved_change_amount,
        src.check_number,
        src.vendor_po_number,
        src.vendor_invoice_number,
        src.source_unique_id,
        src.source_partition_id,
        src.source_system_code,
        src.source_created_ts,
        src.source_modified_ts,
        src.edp_update_ts
      FROM ",
        TEMP_TABLE,
        " src
      LEFT JOIN ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
        ON  tgt.invoice_skey = src.invoice_skey
        AND tgt.invoice_id = src.invoice_skey
      WHERE tgt.invoice_skey IS NULL
        AND tgt.invoice_id IS NULL;
      "
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
