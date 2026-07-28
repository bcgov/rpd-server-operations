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
TABLE_NAME <- "archibus_cost_tran_recur"
CBRE_TABLE_NAME <- "archibus_cost_tran_recur"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
API_NAME <- "CBRE"
SCRIPT_NAME <- "archibus_cost_tran_recur"

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
  # clean_data <- cost_recur_raw_data |>
  purrr::pluck("data") |>
  # select_if(~ !all(is.na(.))) |>
  # select_if(~ !all(. == 0)) |>
  # select_if(~ !all(. == '-1')) |>
  # select_if(~ !all(. == "N/A")) |>
  # select_if(~ !all(. == "-")) |>
  select(
    cost_tran_recur_cost_tran_recur_id_key,
    cost_tran_recur_cost_tran_recur_id,
    cost_tran_recur_description,
    cost_tran_recur_status_active,
    cost_tran_recur_period,
    cost_tran_recur_amount_expense,
    cost_tran_recur_amount_expense_base_payment,
    cost_tran_recur_amount_expense_base_budget,
    cost_tran_recur_amount_expense_vat_payment,
    cost_tran_recur_amount_expense_vat_budget,
    cost_tran_recur_amount_income,
    cost_tran_recur_amount_income_base_payment,
    cost_tran_recur_amount_income_base_budget,
    cost_tran_recur_amount_income_vat_payment,
    cost_tran_recur_amount_income_total_payment,
    cost_tran_recur_area,
    cost_tran_recur_unit,
    cost_tran_recur_cost_sqm,
    cost_tran_recur_project_id,
    cost_tran_recur_bl_id,
    cost_tran_recur_ls_id,
    cost_tran_recur_option1,
    cost_tran_recur_option2,
    cost_tran_recur_ba_id,
    cost_tran_recur_cost_cat_id,
    cost_tran_recur_cam_cost,
    cost_tran_recur_charge_source,
    cost_tran_recur_date_amort_start,
    cost_tran_recur_date_amort_end,
    cost_tran_recur_date_start,
    cost_tran_recur_date_seasonal_start,
    cost_tran_recur_date_trans_created,
    cost_tran_recur_entered_by,
    cost_tran_recur_exchange_rate_budget,
    cost_tran_recur_exchange_rate_override,
    cost_tran_recur_exchange_rate_payment,
    cost_tran_recur_funding_type,
    cost_tran_recur_int_rate,
    cost_tran_recur_is_secure,
    cost_tran_recur_parking_type,
    cost_tran_recur_parking_stalls,
    cost_tran_recur_period,
    cost_tran_recur_remit_to,
    cost_tran_recur_resp_type,
    cost_tran_recur_space_type,
    cost_tran_recur_tax_clr,
    cost_tran_recur_tax_period_in_months,
    cost_tran_recur_vat_percent_override,
    cost_tran_recur_vat_percent_value,
    cost_tran_recur_vendor,
    cost_tran_recur_yearly_factor,
    md5_hash,
    source_system,
    edp_last_updated_timestamp,
    edp_update_ts
  ) |>
  mutate(
    across(
      c(
        cost_tran_recur_date_amort_start,
        cost_tran_recur_date_amort_end,
        cost_tran_recur_date_start,
        cost_tran_recur_date_seasonal_start,
        cost_tran_recur_date_trans_created,
        edp_last_updated_timestamp,
        edp_update_ts
      ),
      ~ as.POSIXct(.x, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC")
    )
  ) |>
  mutate(
    across(
      c(
        cost_tran_recur_amount_expense,
        cost_tran_recur_amount_expense_base_payment,
        cost_tran_recur_amount_expense_base_budget,
        cost_tran_recur_amount_expense_vat_payment,
        cost_tran_recur_amount_expense_vat_budget,
        cost_tran_recur_amount_income,
        cost_tran_recur_amount_income_base_payment,
        cost_tran_recur_amount_income_base_budget,
        cost_tran_recur_amount_income_vat_payment,
        cost_tran_recur_amount_income_total_payment,
        cost_tran_recur_area,
        cost_tran_recur_cost_sqm
      ),
      as.double
    )
  ) |>
  mutate(RefreshDate = as.POSIXct(Sys.time()), .before = everything())

# Database Transaction ####
# dbRemoveTable(con, Id(schema = "CbreStaging", table = TABLE_NAME))
if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- paste0(
    "CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    TABLE_NAME,
    " (
        RefreshDate                                   DATETIME2(3)   NOT NULL,
        cost_tran_recur_cost_tran_recur_id_key        NVARCHAR(20)   NOT NULL,
        cost_tran_recur_cost_tran_recur_id            NVARCHAR(20)   NULL,
        cost_tran_recur_description                   NVARCHAR(1000) NULL,
        cost_tran_recur_status_active                 NVARCHAR(5)    NULL,
        cost_tran_recur_period                        NVARCHAR(20)   NULL,
        cost_tran_recur_amount_expense                DECIMAL(18,2)  NULL,
        cost_tran_recur_amount_expense_base_payment   DECIMAL(18,2)  NULL,
        cost_tran_recur_amount_expense_base_budget    DECIMAL(18,2)  NULL,
        cost_tran_recur_amount_expense_vat_payment    DECIMAL(18,2)  NULL,
        cost_tran_recur_amount_expense_vat_budget     DECIMAL(18,2)  NULL,
        cost_tran_recur_amount_income                 DECIMAL(18,2)  NULL,
        cost_tran_recur_amount_income_base_payment    DECIMAL(18,2)  NULL,
        cost_tran_recur_amount_income_base_budget     DECIMAL(18,2)  NULL,
        cost_tran_recur_amount_income_vat_payment     DECIMAL(18,2)  NULL,
        cost_tran_recur_amount_income_total_payment   DECIMAL(18,2)  NULL,
        cost_tran_recur_area                          DECIMAL(18,5)  NULL,
        cost_tran_recur_unit                          NVARCHAR(10)   NULL,
        cost_tran_recur_cost_sqm                      DECIMAL(18,5)  NULL,
        cost_tran_recur_project_id                    NVARCHAR(30)   NULL,
        cost_tran_recur_bl_id                         NVARCHAR(20)   NULL,
        cost_tran_recur_ls_id                         NVARCHAR(30)   NULL,
        cost_tran_recur_option1                       NVARCHAR(20)   NULL,
        cost_tran_recur_option2                       NVARCHAR(30)   NULL,
        cost_tran_recur_ba_id                         NVARCHAR(20)   NULL,
        cost_tran_recur_cost_cat_id                   NVARCHAR(50)   NULL,
        cost_tran_recur_cam_cost                      NVARCHAR(20)   NULL,
        cost_tran_recur_charge_source                 NVARCHAR(30)   NULL,
        cost_tran_recur_date_amort_start              DATETIME2(3)   NULL,
        cost_tran_recur_date_amort_end                DATETIME2(3)   NULL,
        cost_tran_recur_date_start                    DATETIME2(3)   NULL,
        cost_tran_recur_date_seasonal_start           DATETIME2(3)   NULL,
        cost_tran_recur_date_trans_created            DATETIME2(3)   NULL,
        cost_tran_recur_entered_by                    NVARCHAR(50)   NULL,
        cost_tran_recur_exchange_rate_budget          NVARCHAR(10)   NULL,
        cost_tran_recur_exchange_rate_override        NVARCHAR(10)   NULL,
        cost_tran_recur_exchange_rate_payment         NVARCHAR(10)   NULL,
        cost_tran_recur_funding_type                  NVARCHAR(30)   NULL,
        cost_tran_recur_int_rate                      NVARCHAR(10)   NULL,
        cost_tran_recur_is_secure                     NVARCHAR(5)    NULL,
        cost_tran_recur_parking_type                  NVARCHAR(10)   NULL,
        cost_tran_recur_parking_stalls                NVARCHAR(10)   NULL,
        cost_tran_recur_remit_to                      NVARCHAR(50)   NULL,
        cost_tran_recur_resp_type                     NVARCHAR(10)   NULL,
        cost_tran_recur_space_type                    NVARCHAR(50)   NULL,
        cost_tran_recur_tax_clr                       NVARCHAR(5)    NULL,
        cost_tran_recur_tax_period_in_months          NVARCHAR(10)   NULL,
        cost_tran_recur_vat_percent_override           NVARCHAR(10)  NULL,
        cost_tran_recur_vat_percent_value             NVARCHAR(20)   NULL,
        cost_tran_recur_vendor                        NVARCHAR(30)   NULL,
        cost_tran_recur_yearly_factor                 NVARCHAR(10)   NULL,
        md5_hash                                      NVARCHAR(40)   NOT NULL,
        source_system                                 NVARCHAR(20)   NULL,
        edp_last_updated_timestamp                    DATETIME2(3)   NULL,
        edp_update_ts                                 DATETIME2(3)   NOT NULL
      );"
  )
  dbExecute(con, sql)
}

etl_error <- NULL

# Control database transaction to ensure all steps done together or not at all
dbBegin(con)

# Begin error handling and roll back on transaction failure
tryCatch(
  {
    if (dbExistsTable(con, TEMP_TABLE)) {
      dbRemoveTable(con, TEMP_TABLE)
    }

    # Create temp table to hold new data
    dbExecute(
      con,
      paste0(
        "
    CREATE TABLE ",
        SCHEMA_NAME,
        ".",
        TEMP_TABLE,
        " (
      RefreshDate                                   DATETIME2(3)   NOT NULL,
      cost_tran_recur_cost_tran_recur_id_key        NVARCHAR(20)   NOT NULL,
      cost_tran_recur_cost_tran_recur_id            NVARCHAR(20)   NULL,
      cost_tran_recur_description                   NVARCHAR(1000) NULL,
      cost_tran_recur_status_active                 NVARCHAR(5)    NULL,
      cost_tran_recur_period                        NVARCHAR(20)   NULL,
      cost_tran_recur_amount_expense                DECIMAL(18,2)  NULL,
      cost_tran_recur_amount_expense_base_payment   DECIMAL(18,2)  NULL,
      cost_tran_recur_amount_expense_base_budget    DECIMAL(18,2)  NULL,
      cost_tran_recur_amount_expense_vat_payment    DECIMAL(18,2)  NULL,
      cost_tran_recur_amount_expense_vat_budget     DECIMAL(18,2)  NULL,
      cost_tran_recur_amount_income                 DECIMAL(18,2)  NULL,
      cost_tran_recur_amount_income_base_payment    DECIMAL(18,2)  NULL,
      cost_tran_recur_amount_income_base_budget     DECIMAL(18,2)  NULL,
      cost_tran_recur_amount_income_vat_payment     DECIMAL(18,2)  NULL,
      cost_tran_recur_amount_income_total_payment   DECIMAL(18,2)  NULL,
      cost_tran_recur_area                          DECIMAL(18,5)  NULL,
      cost_tran_recur_unit                          NVARCHAR(10)   NULL,
      cost_tran_recur_cost_sqm                      DECIMAL(18,5)  NULL,
      cost_tran_recur_project_id                    NVARCHAR(30)   NULL,
      cost_tran_recur_bl_id                         NVARCHAR(20)   NULL,
      cost_tran_recur_ls_id                         NVARCHAR(30)   NULL,
      cost_tran_recur_option1                       NVARCHAR(20)   NULL,
      cost_tran_recur_option2                       NVARCHAR(30)   NULL,
      cost_tran_recur_ba_id                         NVARCHAR(20)   NULL,
      cost_tran_recur_cost_cat_id                   NVARCHAR(50)   NULL,
      cost_tran_recur_cam_cost                      NVARCHAR(20)   NULL,
      cost_tran_recur_charge_source                 NVARCHAR(30)   NULL,
      cost_tran_recur_date_amort_start              DATETIME2(3)   NULL,
      cost_tran_recur_date_amort_end                DATETIME2(3)   NULL,
      cost_tran_recur_date_start                    DATETIME2(3)   NULL,
      cost_tran_recur_date_seasonal_start           DATETIME2(3)   NULL,
      cost_tran_recur_date_trans_created            DATETIME2(3)   NULL,
      cost_tran_recur_entered_by                    NVARCHAR(50)   NULL,
      cost_tran_recur_exchange_rate_budget          NVARCHAR(10)   NULL,
      cost_tran_recur_exchange_rate_override        NVARCHAR(10)   NULL,
      cost_tran_recur_exchange_rate_payment         NVARCHAR(10)   NULL,
      cost_tran_recur_funding_type                  NVARCHAR(30)   NULL,
      cost_tran_recur_int_rate                      NVARCHAR(10)   NULL,
      cost_tran_recur_is_secure                     NVARCHAR(5)    NULL,
      cost_tran_recur_parking_type                  NVARCHAR(10)   NULL,
      cost_tran_recur_parking_stalls                NVARCHAR(10)   NULL,
      cost_tran_recur_remit_to                      NVARCHAR(50)   NULL,
      cost_tran_recur_resp_type                     NVARCHAR(10)   NULL,
      cost_tran_recur_space_type                    NVARCHAR(50)   NULL,
      cost_tran_recur_tax_clr                       NVARCHAR(5)    NULL,
      cost_tran_recur_tax_period_in_months          NVARCHAR(10)   NULL,
      cost_tran_recur_vat_percent_override           NVARCHAR(10)  NULL,
      cost_tran_recur_vat_percent_value             NVARCHAR(20)   NULL,
      cost_tran_recur_vendor                        NVARCHAR(30)   NULL,
      cost_tran_recur_yearly_factor                 NVARCHAR(10)   NULL,
      md5_hash                                      NVARCHAR(40)   NOT NULL,
      source_system                                 NVARCHAR(20)   NULL,
      edp_last_updated_timestamp                    DATETIME2(3)   NULL,
      edp_update_ts                                 DATETIME2(3)   NOT NULL
    );
  "
      )
    )

    dbWriteTable(
      con,
      name = TEMP_TABLE,
      value = clean_data,
      append = TRUE,
      overwrite = FALSE
    )

    dbExecute(
      con,
      paste0(
        "DELETE FROM ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        ";"
      )
    )

    n_inserted <- dbExecute(
      con,
      paste0(
        "INSERT INTO ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        "(
        RefreshDate,
        cost_tran_recur_cost_tran_recur_id_key,
        cost_tran_recur_cost_tran_recur_id,
        cost_tran_recur_description,
        cost_tran_recur_status_active,
        cost_tran_recur_period,
        cost_tran_recur_amount_expense,
        cost_tran_recur_amount_expense_base_payment,
        cost_tran_recur_amount_expense_base_budget,
        cost_tran_recur_amount_expense_vat_payment,
        cost_tran_recur_amount_expense_vat_budget,
        cost_tran_recur_amount_income,
        cost_tran_recur_amount_income_base_payment,
        cost_tran_recur_amount_income_base_budget,
        cost_tran_recur_amount_income_vat_payment,
        cost_tran_recur_amount_income_total_payment,
        cost_tran_recur_area,
        cost_tran_recur_unit,
        cost_tran_recur_cost_sqm,
        cost_tran_recur_project_id,
        cost_tran_recur_bl_id,
        cost_tran_recur_ls_id,
        cost_tran_recur_option1,
        cost_tran_recur_option2,
        cost_tran_recur_ba_id,
        cost_tran_recur_cost_cat_id,
        cost_tran_recur_cam_cost,
        cost_tran_recur_charge_source,
        cost_tran_recur_date_amort_start,
        cost_tran_recur_date_amort_end,
        cost_tran_recur_date_start,
        cost_tran_recur_date_seasonal_start,
        cost_tran_recur_date_trans_created,
        cost_tran_recur_entered_by,
        cost_tran_recur_exchange_rate_budget,
        cost_tran_recur_exchange_rate_override,
        cost_tran_recur_exchange_rate_payment,
        cost_tran_recur_funding_type,
        cost_tran_recur_int_rate,
        cost_tran_recur_is_secure,
        cost_tran_recur_parking_type,
        cost_tran_recur_parking_stalls,
        cost_tran_recur_remit_to,
        cost_tran_recur_resp_type,
        cost_tran_recur_space_type,
        cost_tran_recur_tax_clr,
        cost_tran_recur_tax_period_in_months,
        cost_tran_recur_vat_percent_override,
        cost_tran_recur_vat_percent_value,
        cost_tran_recur_vendor,
        cost_tran_recur_yearly_factor,
        md5_hash,
        source_system,
        edp_last_updated_timestamp,
        edp_update_ts
      )
       SELECT * FROM ",
        TEMP_TABLE,
        ";"
      )
    )

    # Complete the transaction
    dbCommit(con)

    # Hoist to main environment
    n_inserted <<- n_inserted
    cat("ETL complete — inserted:", n_inserted, "\n")
    # Rollback transaction on failure
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
    n_updated = NA,
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
