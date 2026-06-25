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
TABLE_NAME <- "fm_fact_workorder"
CBRE_TABLE_NAME <- "fm_fact_workorder_vw"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
API_NAME <- "CBRE"
SCRIPT_NAME <- "fm_fact_workorder"

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
  select(
    workorder_creation_date_ts,
    closed_date,
    workorder_skey,
    workorder_number,
    workorder_desc,
    workorder_comments,
    parent_workorder_skey,
    parent_workorder_number,
    status_code_skey,
    status_code,
    status_code_desc,
    priority_code_skey,
    priority_code,
    priority_code_desc,
    type_code,
    category_code,
    category_code_desc,
    sub_category_code,
    sub_category_code_desc,
    activity_name,
    activity_desc,
    asset_category_code,
    asset_category_code_desc,
    asset_sub_category_code,
    asset_sub_category_code_desc,
    bid_amount,
    actual_cost,
    total_labor_logged_hours,
    problem_code_skey,
    repair_code,
    repair_code_desc,
    repair_code_skey,
    repair_category_code,
    repair_category_code_desc,
    maintenance_skey,
    maintenance_plan_skey,
    worker_skey,
    property_skey,
    property_hierarchy_skey,
    vendor_skey,
    edp_update_ts,
    cbre_standardized_workorder_type_code,
    cbre_standardized_workorder_type_skey,
    cbre_standardized_cost_category_code,
    cbre_standardized_cost_category_skey,
    cbre_standardized_workorder_category_code,
    cbre_standardized_workorder_category_skey
  ) |>
  mutate(RefreshDate = as.POSIXct(Sys.time()), .before = everything()) |>
  mutate(
    across(
      c(
        workorder_skey,
        parent_workorder_skey,
        status_code_skey,
        priority_code_skey,
        problem_code_skey,
        repair_code_skey,
        maintenance_skey,
        maintenance_plan_skey,
        worker_skey,
        property_skey,
        property_hierarchy_skey,
        vendor_skey,
        cbre_standardized_workorder_type_skey,
        cbre_standardized_cost_category_skey,
        cbre_standardized_workorder_category_skey
      ),
      as.character
    )
  ) |>
  mutate(
    across(
      c(
        edp_update_ts,
        workorder_creation_date_ts,
        closed_date
      ),
      ~ as.POSIXct(.x, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC")
    )
  ) |>
  mutate(
    across(
      c(
        actual_cost,
        total_labor_logged_hours,
        bid_amount
      ),
      as.double
    )
  ) |>
  mutate(
    FYCreation = fiscal_year_label(workorder_creation_date_ts),
    .after = RefreshDate
  )

# dbRemoveTable(con, TARGET_TABLE)
if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- paste0(
    "CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    TABLE_NAME,
    " (
      RefreshDate                                DATETIME2(3)   NOT NULL,
      FYCreation                                 NVARCHAR(50)   NULL,
      workorder_creation_date_ts                 DATETIME2(3)   NULL,
      closed_date                                DATETIME2(3)   NULL,
      workorder_skey                             NVARCHAR(50)   NOT NULL,
      workorder_number                           NVARCHAR(50)   NOT NULL,
      workorder_desc                             NVARCHAR(MAX)  NULL,
      workorder_comments                         NVARCHAR(MAX)  NULL,
      parent_workorder_skey                      NVARCHAR(50)   NULL,
      parent_workorder_number                    NVARCHAR(50)   NULL,
      status_code_skey                           NVARCHAR(50)   NULL,
      status_code                                NVARCHAR(50)   NULL,
      status_code_desc                           NVARCHAR(255)  NULL,
      priority_code_skey                         NVARCHAR(50)   NULL,
      priority_code                              NVARCHAR(50)   NULL,
      priority_code_desc                         NVARCHAR(255)  NULL,
      type_code                                  NVARCHAR(50)   NULL,
      category_code                              NVARCHAR(50)   NULL,
      category_code_desc                         NVARCHAR(255)  NULL,
      sub_category_code                          NVARCHAR(50)   NULL,
      sub_category_code_desc                     NVARCHAR(255)  NULL,
      activity_name                              NVARCHAR(MAX)  NULL,
      activity_desc                              NVARCHAR(MAX)  NULL,
      asset_sub_category_code                    NVARCHAR(50)   NULL,
      asset_sub_category_code_desc               NVARCHAR(255)  NULL,
      asset_category_code                        NVARCHAR(50)   NULL,
      asset_category_code_desc                   NVARCHAR(255)  NULL,
      bid_amount                                 DECIMAL(18,2)  NULL,
      actual_cost                                DECIMAL(18,2)  NULL,
      total_labor_logged_hours                   DECIMAL(18,2)  NULL,
      problem_code_skey                          NVARCHAR(50)   NULL,
      repair_code                                NVARCHAR(50)   NULL,
      repair_code_desc                           NVARCHAR(255)  NULL,
      repair_code_skey                           NVARCHAR(50)   NULL,
      repair_category_code                       NVARCHAR(50)   NULL,
      repair_category_code_desc                  NVARCHAR(255)  NULL,
      maintenance_skey                           NVARCHAR(50)   NULL,
      maintenance_plan_skey                      NVARCHAR(50)   NULL,
      worker_skey                                NVARCHAR(50)   NULL,
      property_skey                              NVARCHAR(50)   NULL,
      property_hierarchy_skey                    NVARCHAR(50)   NULL,
      vendor_skey                                NVARCHAR(50)   NULL,
      edp_update_ts                              DATETIME2(3)   NULL,
      cbre_standardized_workorder_type_code      NVARCHAR(100)  NULL,
      cbre_standardized_workorder_type_skey      NVARCHAR(50)   NULL,
      cbre_standardized_cost_category_code       NVARCHAR(100)  NULL,
      cbre_standardized_cost_category_skey       NVARCHAR(50)   NULL,
      cbre_standardized_workorder_category_code  NVARCHAR(100)  NULL,
      cbre_standardized_workorder_category_skey  NVARCHAR(50)   NULL
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
          RefreshDate                                DATETIME2(3)   NOT NULL,
          FYCreation                                 NVARCHAR(50)   NULL,
          workorder_creation_date_ts                 DATETIME2(3)   NULL,
          closed_date                                DATETIME2(3)   NULL,
          workorder_skey                             NVARCHAR(50)   NOT NULL,
          workorder_number                           NVARCHAR(50)   NOT NULL,
          workorder_desc                             NVARCHAR(MAX)  NULL,
          workorder_comments                         NVARCHAR(MAX)  NULL,
          parent_workorder_skey                      NVARCHAR(50)   NULL,
          parent_workorder_number                    NVARCHAR(50)   NULL,
          status_code_skey                           NVARCHAR(50)   NULL,
          status_code                                NVARCHAR(50)   NULL,
          status_code_desc                           NVARCHAR(255)  NULL,
          priority_code_skey                         NVARCHAR(50)   NULL,
          priority_code                              NVARCHAR(50)   NULL,
          priority_code_desc                         NVARCHAR(255)  NULL,
          type_code                                  NVARCHAR(50)   NULL,
          category_code                              NVARCHAR(50)   NULL,
          category_code_desc                         NVARCHAR(255)  NULL,
          sub_category_code                          NVARCHAR(50)   NULL,
          sub_category_code_desc                     NVARCHAR(255)  NULL,
          activity_name                              NVARCHAR(MAX)  NULL,
          activity_desc                              NVARCHAR(MAX)  NULL,
          asset_sub_category_code                    NVARCHAR(50)   NULL,
          asset_sub_category_code_desc               NVARCHAR(255)  NULL,
          asset_category_code                        NVARCHAR(50)   NULL,
          asset_category_code_desc                   NVARCHAR(255)  NULL,
          bid_amount                                 DECIMAL(18,2)  NULL,
          actual_cost                                DECIMAL(18,2)  NULL,
          total_labor_logged_hours                   DECIMAL(18,2)  NULL,
          problem_code_skey                          NVARCHAR(50)   NULL,
          repair_code                                NVARCHAR(50)   NULL,
          repair_code_desc                           NVARCHAR(255)  NULL,
          repair_code_skey                           NVARCHAR(50)   NULL,
          repair_category_code                       NVARCHAR(50)   NULL,
          repair_category_code_desc                  NVARCHAR(255)  NULL,
          maintenance_skey                           NVARCHAR(50)   NULL,
          maintenance_plan_skey                      NVARCHAR(50)   NULL,
          worker_skey                                NVARCHAR(50)   NULL,
          property_skey                              NVARCHAR(50)   NULL,
          property_hierarchy_skey                    NVARCHAR(50)   NULL,
          vendor_skey                                NVARCHAR(50)   NULL,
          edp_update_ts                              DATETIME2(3)   NULL,
          cbre_standardized_workorder_type_code      NVARCHAR(100)  NULL,
          cbre_standardized_workorder_type_skey      NVARCHAR(50)   NULL,
          cbre_standardized_cost_category_code       NVARCHAR(100)  NULL,
          cbre_standardized_cost_category_skey       NVARCHAR(50)   NULL,
          cbre_standardized_workorder_category_code  NVARCHAR(100)  NULL,
          cbre_standardized_workorder_category_skey  NVARCHAR(50)   NULL
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
           SELECT workorder_skey
           FROM ",
        TEMP_TABLE,
        "
           GROUP BY workorder_skey
           HAVING COUNT(*) > 1
         ) dupes;"
      )
    )$n

    if (dup_count > 0) {
      stop(paste0(
        "Duplicate workorder_skey values detected in source data (",
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
           tgt.RefreshDate                                = src.RefreshDate,
           tgt.FYCreation                                 = src.FYCreation,
           tgt.workorder_creation_date_ts                 = src.workorder_creation_date_ts,
           tgt.closed_date                                = src.closed_date,
           tgt.workorder_number                           = src.workorder_number,
           tgt.workorder_desc                             = src.workorder_desc,
           tgt.workorder_comments                         = src.workorder_comments,
           tgt.parent_workorder_skey                      = src.parent_workorder_skey,
           tgt.parent_workorder_number                    = src.parent_workorder_number,
           tgt.status_code_skey                           = src.status_code_skey,
           tgt.status_code                                = src.status_code,
           tgt.status_code_desc                           = src.status_code_desc,
           tgt.priority_code                              = src.priority_code,
           tgt.priority_code_desc                         = src.priority_code_desc,
           tgt.priority_code_skey                         = src.priority_code_skey,
           tgt.type_code                                  = src.type_code,
           tgt.category_code                              = src.category_code,
           tgt.category_code_desc                         = src.category_code_desc,
           tgt.sub_category_code                          = src.sub_category_code,
           tgt.sub_category_code_desc                     = src.sub_category_code_desc,
           tgt.activity_name                              = src.activity_name,
           tgt.activity_desc                              = src.activity_desc,
           tgt.asset_category_code                        = src.asset_category_code,
           tgt.asset_category_code_desc                   = src.asset_category_code_desc,
           tgt.asset_sub_category_code                    = src.asset_sub_category_code,
           tgt.asset_sub_category_code_desc               = src.asset_sub_category_code_desc,
           tgt.bid_amount                                 = src.bid_amount,
           tgt.actual_cost                                = src.actual_cost,
           tgt.total_labor_logged_hours                   = src.total_labor_logged_hours,
           tgt.repair_code                                = src.repair_code,
           tgt.repair_code_desc                           = src.repair_code_desc,
           tgt.repair_code_skey                           = src.repair_code_skey,
           tgt.repair_category_code                       = src.repair_category_code,
           tgt.repair_category_code_desc                  = src.repair_category_code_desc,
           tgt.maintenance_skey                           = src.maintenance_skey,
           tgt.maintenance_plan_skey                      = src.maintenance_plan_skey,
           tgt.worker_skey                                = src.worker_skey,
           tgt.property_skey                              = src.property_skey,
           tgt.property_hierarchy_skey                    = src.property_hierarchy_skey,
           tgt.vendor_skey                                = src.vendor_skey,
           tgt.problem_code_skey                          = src.problem_code_skey,
           tgt.edp_update_ts                              = src.edp_update_ts,
           tgt.cbre_standardized_workorder_type_code      = src.cbre_standardized_workorder_type_code,
           tgt.cbre_standardized_workorder_type_skey      = src.cbre_standardized_workorder_type_skey,
           tgt.cbre_standardized_cost_category_code       = src.cbre_standardized_cost_category_code,
           tgt.cbre_standardized_cost_category_skey       = src.cbre_standardized_cost_category_skey,
           tgt.cbre_standardized_workorder_category_code  = src.cbre_standardized_workorder_category_code,
           tgt.cbre_standardized_workorder_category_skey  = src.cbre_standardized_workorder_category_skey
         FROM ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
         INNER JOIN ",
        TEMP_TABLE,
        " src
           ON tgt.workorder_skey = src.workorder_skey;"
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
          FYCreation,
          workorder_creation_date_ts,
          closed_date,
          workorder_skey,
          workorder_number,
          workorder_desc,
          workorder_comments,
          parent_workorder_skey,
          parent_workorder_number,
          status_code_skey,
          status_code,
          status_code_desc,
          priority_code_skey,
          priority_code,
          priority_code_desc,
          type_code,
          category_code,
          category_code_desc,
          sub_category_code,
          sub_category_code_desc,
          activity_name,
          activity_desc,
          asset_category_code,
          asset_category_code_desc,
          asset_sub_category_code,
          asset_sub_category_code_desc,
          bid_amount,
          actual_cost,
          total_labor_logged_hours,
          problem_code_skey,
          repair_code,
          repair_code_desc,
          repair_code_skey,
          repair_category_code,
          repair_category_code_desc,
          maintenance_skey,
          maintenance_plan_skey,
          worker_skey,
          property_skey,
          property_hierarchy_skey,
          vendor_skey,
          edp_update_ts,
          cbre_standardized_workorder_type_code,
          cbre_standardized_workorder_type_skey,
          cbre_standardized_cost_category_code,
          cbre_standardized_cost_category_skey,
          cbre_standardized_workorder_category_code,
          cbre_standardized_workorder_category_skey
        )
        SELECT
          src.RefreshDate,
          src.FYCreation,
          src.workorder_creation_date_ts,
          src.closed_date,
          src.workorder_skey,
          src.workorder_number,
          src.workorder_desc,
          src.workorder_comments,
          src.parent_workorder_skey,
          src.parent_workorder_number,
          src.status_code_skey,
          src.status_code,
          src.status_code_desc,
          src.priority_code_skey,
          src.priority_code,
          src.priority_code_desc,
          src.type_code,
          src.category_code,
          src.category_code_desc,
          src.sub_category_code,
          src.sub_category_code_desc,
          src.activity_name,
          src.activity_desc,
          src.asset_category_code,
          src.asset_category_code_desc,
          src.asset_sub_category_code,
          src.asset_sub_category_code_desc,
          src.bid_amount,
          src.actual_cost,
          src.total_labor_logged_hours,
          src.problem_code_skey,
          src.repair_code,
          src.repair_code_desc,
          src.repair_code_skey,
          src.repair_category_code,
          src.repair_category_code_desc,
          src.maintenance_skey,
          src.maintenance_plan_skey,
          src.worker_skey,
          src.property_skey,
          src.property_hierarchy_skey,
          src.vendor_skey,
          src.edp_update_ts,
          src.cbre_standardized_workorder_type_code,
          src.cbre_standardized_workorder_type_skey,
          src.cbre_standardized_cost_category_code,
          src.cbre_standardized_cost_category_skey,
          src.cbre_standardized_workorder_category_code,
          src.cbre_standardized_workorder_category_skey
        FROM ",
        TEMP_TABLE,
        " src
        LEFT JOIN ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
          ON tgt.workorder_skey = src.workorder_skey
        WHERE tgt.workorder_skey IS NULL;"
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
