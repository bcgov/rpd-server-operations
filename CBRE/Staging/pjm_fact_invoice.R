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
TABLE_NAME <- "pjm_fact_invoice"
CBRE_TABLE_NAME <- "pjm_fact_invoice_vw"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
API_NAME <- "CBRE"
SCRIPT_NAME <- "pjm_fact_invoice"

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
      ~ as.POSIXct(.x, format = "%Y-%m-%d %H:%M:%S %z", tz = "UTC")
    )
  ) |>
  mutate(
    across(
      c(
        balance_to_finish,
        balance_to_finish_with_retainage,
        current_payment_due,
        payables_billed_total,
        payables_remaining_total,
        payables_remitted_total,
        payables_withheld_total,
        previous_total_earned,
        previous_work_completed,
        scheduled_value,
        total_to_date,
        total_to_date_percent,
        total_retainage,
        total_earned_to_date,
        work_completed_to_date,
        work_completed_this_period,
        work_retainage,
        work_retainage_percent
      ),
      as.double
    )
  ) |>
  mutate(
    across(
      c(
        project_skey,
        project_activity_skey,
        invoice_skey,
        invoice_item_skey,
        change_order_skey,
        change_order_item_skey,
        invoice_item_id,
        from_contact_skey,
        to_company_skey,
        from_company_skey,
        change_order_item_id,
        line_number,
        source_unique_id,
        source_partition_id
      ),
      as.character
    )
  ) |>
  select(
    RefreshDate,
    project_skey,
    project_activity_skey,
    invoice_skey,
    invoice_item_skey,
    change_order_skey,
    change_order_item_skey,
    record_type,
    invoice_status,
    invoice_desc,
    invoice_item_id,
    contract_skey,
    contract_line_skey,
    to_contact_skey,
    from_contact_skey,
    to_company_skey,
    from_company_skey,
    change_order_item_id,
    line_number,
    work_completed_this_period,
    work_completed_to_date,
    work_retainage,
    work_retainage_percent,
    payables_billed_total,
    payables_withheld_total,
    payables_remitted_total,
    balance_to_finish,
    balance_to_finish_with_retainage,
    previous_total_earned,
    previous_work_completed,
    total_to_date,
    total_earned_to_date,
    total_to_date_percent,
    total_retainage,
    scheduled_value,
    current_payment_due,
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
      RefreshDate                      DATETIME2(3)    NOT NULL,
      project_skey                     NVARCHAR(20)    NOT NULL,
      project_activity_skey            NVARCHAR(20)    NULL,
      invoice_skey                     NVARCHAR(20)    NOT NULL,
      invoice_item_skey                NVARCHAR(20)    NULL,
      change_order_skey                NVARCHAR(20)    NULL,
      change_order_item_skey           NVARCHAR(20)    NULL,
      record_type                      NVARCHAR(20)    NULL,
      invoice_status                   NVARCHAR(20)    NULL,
      invoice_desc                     NVARCHAR(MAX)   NULL,
      invoice_item_id                  NVARCHAR(20)    NULL,
      contract_skey                    NVARCHAR(20)    NULL,
      contract_line_skey               NVARCHAR(20)    NULL,
      to_contact_skey                  NVARCHAR(20)    NULL,
      from_contact_skey                NVARCHAR(20)    NULL,
      to_company_skey                  NVARCHAR(20)    NULL,
      from_company_skey                NVARCHAR(20)    NULL,
      change_order_item_id             NVARCHAR(20)    NULL,
      line_number                      NVARCHAR(20)    NULL,
      work_completed_this_period       DECIMAL(18,2)   NULL,
      work_completed_to_date           DECIMAL(18,2)   NULL,
      work_retainage                   DECIMAL(18,2)   NULL,
      work_retainage_percent           DECIMAL(18,2)   NULL,
      payables_billed_total            DECIMAL(18,2)   NULL,
      payables_withheld_total          DECIMAL(18,2)   NULL,
      payables_remitted_total          DECIMAL(18,2)   NULL,
      balance_to_finish                DECIMAL(18,2)   NULL,
      balance_to_finish_with_retainage DECIMAL(18,2)   NULL,
      previous_total_earned            DECIMAL(18,2)   NULL,
      previous_work_completed          DECIMAL(18,2)   NULL,
      total_to_date                    DECIMAL(18,2)   NULL,
      total_earned_to_date             DECIMAL(18,2)   NULL,
      total_to_date_percent            DECIMAL(18,2)   NULL,
      total_retainage                  DECIMAL(18,2)   NULL,
      scheduled_value                  DECIMAL(18,2)   NULL,
      current_payment_due              DECIMAL(18,2)   NULL,
      source_unique_id                 NVARCHAR(20)    NULL,
      source_partition_id              NVARCHAR(20)    NULL,
      source_system_code               NVARCHAR(50)    NULL,
      source_created_ts                DATETIME2(3)    NULL,
      source_modified_ts               DATETIME2(3)    NULL,
      edp_update_ts                    DATETIME2(3)    NULL
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
        RefreshDate                      DATETIME2(3)    NOT NULL,
        project_skey                     NVARCHAR(20)    NOT NULL,
        project_activity_skey            NVARCHAR(20)    NULL,
        invoice_skey                     NVARCHAR(20)    NOT NULL,
        invoice_item_skey                NVARCHAR(20)    NULL,
        change_order_skey                NVARCHAR(20)    NULL,
        change_order_item_skey           NVARCHAR(20)    NULL,
        record_type                      NVARCHAR(20)    NULL,
        invoice_status                   NVARCHAR(20)    NULL,
        invoice_desc                     NVARCHAR(MAX)   NULL,
        invoice_item_id                  NVARCHAR(20)    NULL,
        contract_skey                    NVARCHAR(20)    NULL,
        contract_line_skey               NVARCHAR(20)    NULL,
        to_contact_skey                  NVARCHAR(20)    NULL,
        from_contact_skey                NVARCHAR(20)    NULL,
        to_company_skey                  NVARCHAR(20)    NULL,
        from_company_skey                NVARCHAR(20)    NULL,
        change_order_item_id             NVARCHAR(20)    NULL,
        line_number                      NVARCHAR(20)    NULL,
        work_completed_this_period       DECIMAL(18,2)   NULL,
        work_completed_to_date           DECIMAL(18,2)   NULL,
        work_retainage                   DECIMAL(18,2)   NULL,
        work_retainage_percent           DECIMAL(18,2)   NULL,
        payables_billed_total            DECIMAL(18,2)   NULL,
        payables_withheld_total          DECIMAL(18,2)   NULL,
        payables_remitted_total          DECIMAL(18,2)   NULL,
        balance_to_finish                DECIMAL(18,2)   NULL,
        balance_to_finish_with_retainage DECIMAL(18,2)   NULL,
        previous_total_earned            DECIMAL(18,2)   NULL,
        previous_work_completed          DECIMAL(18,2)   NULL,
        total_to_date                    DECIMAL(18,2)   NULL,
        total_earned_to_date             DECIMAL(18,2)   NULL,
        total_to_date_percent            DECIMAL(18,2)   NULL,
        total_retainage                  DECIMAL(18,2)   NULL,
        scheduled_value                  DECIMAL(18,2)   NULL,
        current_payment_due              DECIMAL(18,2)   NULL,
        source_unique_id                 NVARCHAR(20)    NULL,
        source_partition_id              NVARCHAR(20)    NULL,
        source_system_code               NVARCHAR(50)    NULL,
        source_created_ts                DATETIME2(3)    NULL,
        source_modified_ts               DATETIME2(3)    NULL,
        edp_update_ts                    DATETIME2(3)    NULL
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
           SELECT project_skey, invoice_skey, invoice_item_skey
           FROM ",
        TEMP_TABLE,
        "
           GROUP BY project_skey, invoice_skey, invoice_item_skey
           HAVING COUNT(*) > 1
         ) dupes;"
      )
    )$n

    if (dup_count > 0) {
      stop(paste0(
        "Duplicate project_skey, invoice_skey, invoice_item_skey values detected in source data (",
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
        tgt.RefreshDate                      = src.RefreshDate,
        tgt.project_activity_skey            = src.project_activity_skey,
        tgt.change_order_skey                = src.change_order_skey,
        tgt.change_order_item_skey           = src.change_order_item_skey,
        tgt.record_type                      = src.record_type,
        tgt.invoice_status                   = src.invoice_status,
        tgt.invoice_desc                     = src.invoice_desc,
        tgt.invoice_item_id                  = src.invoice_item_id,
        tgt.contract_skey                    = src.contract_skey,
        tgt.contract_line_skey               = src.contract_line_skey,
        tgt.to_contact_skey                  = src.to_contact_skey,
        tgt.from_contact_skey                = src.from_contact_skey,
        tgt.to_company_skey                  = src.to_company_skey,
        tgt.from_company_skey                = src.from_company_skey,
        tgt.change_order_item_id             = src.change_order_item_id,
        tgt.line_number                      = src.line_number,
        tgt.work_completed_this_period       = src.work_completed_this_period,
        tgt.work_completed_to_date           = src.work_completed_to_date,
        tgt.work_retainage                   = src.work_retainage,
        tgt.work_retainage_percent           = src.work_retainage_percent,
        tgt.payables_billed_total            = src.payables_billed_total,
        tgt.payables_withheld_total          = src.payables_withheld_total,
        tgt.payables_remitted_total          = src.payables_remitted_total,
        tgt.balance_to_finish                = src.balance_to_finish,
        tgt.balance_to_finish_with_retainage = src.balance_to_finish_with_retainage,
        tgt.previous_total_earned            = src.previous_total_earned,
        tgt.previous_work_completed          = src.previous_work_completed,
        tgt.total_to_date                    = src.total_to_date,
        tgt.total_earned_to_date             = src.total_earned_to_date,
        tgt.total_to_date_percent            = src.total_to_date_percent,
        tgt.total_retainage                  = src.total_retainage,
        tgt.scheduled_value                  = src.scheduled_value,
        tgt.current_payment_due              = src.current_payment_due,
        tgt.source_unique_id                 = src.source_unique_id,
        tgt.source_partition_id              = src.source_partition_id,
        tgt.source_system_code               = src.source_system_code,
        tgt.source_created_ts                = src.source_created_ts,
        tgt.source_modified_ts               = src.source_modified_ts,
        tgt.edp_update_ts                    = src.edp_update_ts
      FROM ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
      JOIN ",
        TEMP_TABLE,
        " src
        ON  tgt.project_skey = src.project_skey
        AND tgt.invoice_skey = src.invoice_skey
        AND tgt.invoice_item_skey = src.invoice_item_skey;"
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
        project_skey,
        project_activity_skey,
        invoice_skey,
        invoice_item_skey,
        change_order_skey,
        change_order_item_skey,
        record_type,
        invoice_status,
        invoice_desc,
        invoice_item_id,
        contract_skey,
        contract_line_skey,
        to_contact_skey,
        from_contact_skey,
        to_company_skey,
        from_company_skey,
        change_order_item_id,
        line_number,
        work_completed_this_period,
        work_completed_to_date,
        work_retainage,
        work_retainage_percent,
        payables_billed_total,
        payables_withheld_total,
        payables_remitted_total,
        balance_to_finish,
        balance_to_finish_with_retainage,
        previous_total_earned,
        previous_work_completed,
        total_to_date,
        total_earned_to_date,
        total_to_date_percent,
        total_retainage,
        scheduled_value,
        current_payment_due,
        source_unique_id,
        source_partition_id,
        source_system_code,
        source_created_ts,
        source_modified_ts,
        edp_update_ts
      )
      SELECT
        src.RefreshDate,
        src.project_skey,
        src.project_activity_skey,
        src.invoice_skey,
        src.invoice_item_skey,
        src.change_order_skey,
        src.change_order_item_skey,
        src.record_type,
        src.invoice_status,
        src.invoice_desc,
        src.invoice_item_id,
        src.contract_skey,
        src.contract_line_skey,
        src.to_contact_skey,
        src.from_contact_skey,
        src.to_company_skey,
        src.from_company_skey,
        src.change_order_item_id,
        src.line_number,
        src.work_completed_this_period,
        src.work_completed_to_date,
        src.work_retainage,
        src.work_retainage_percent,
        src.payables_billed_total,
        src.payables_withheld_total,
        src.payables_remitted_total,
        src.balance_to_finish,
        src.balance_to_finish_with_retainage,
        src.previous_total_earned,
        src.previous_work_completed,
        src.total_to_date,
        src.total_earned_to_date,
        src.total_to_date_percent,
        src.total_retainage,
        src.scheduled_value,
        src.current_payment_due,
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
        ON  tgt.project_skey      = src.project_skey
        AND tgt.invoice_skey      = src.invoice_skey
        AND tgt.invoice_item_skey = src.invoice_item_skey
      WHERE tgt.project_skey IS NULL
        AND tgt.invoice_skey IS NULL
        AND tgt.invoice_item_skey IS NULL;
      "
      )
    )
    # Complete the transaction
    dbCommit(con)

    # Hoist counts to outer scope for logging
    n_inserted <<- n_inserted
    n_updated <<- n_updated

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
