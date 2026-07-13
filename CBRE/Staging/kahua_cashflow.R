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
TABLE_NAME <- "kahua_cashflow"
CBRE_TABLE_NAME <- "kahua_cashflow"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
API_NAME <- "CBRE"
SCRIPT_NAME <- "kahua_cashflow"

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
  # # comment out these after initial data analysis as risk of
  # # losing columns in small data loads
  select_if(~ !all(is.na(.))) |>
  select_if(~ !all(. == 0)) |>
  select_if(~ !all(. == '-1')) |>
  select_if(~ !all(. == "N/A")) |>
  select_if(~ !all(. == "-")) |>
  select(
    ProjectNumber = projectnumber,
    ClientProjectNumber = clientprojectnumber,
    ProjectId = project_id,
    CashflowId = cashflow_id,
    CashflowParentId = cashflow_parentid,
    Period = period_name,
    LineCategory = linecategory,
    ActivityCode = activitycode,
    ActivityCodeDesc = activitycodedesc,
    ItemAmount = itemamount,
    AllocationItemId = allocationitemid,
    AllocationAmount = allocationamount,
    AllocationDate = allocationdate,
    edp_update_ts
  ) |>
  mutate(RefreshDate = as.POSIXct(Sys.time()), .before = everything()) |>
  mutate(
    across(
      c(
        AllocationDate,
        edp_update_ts
      ),
      ~ as.POSIXct(.x, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC")
    )
  ) |>
  mutate(
    across(
      c(
        ItemAmount,
        AllocationAmount
      ),
      as.double
    )
  ) |>
  mutate(
    across(
      c(
        ProjectId,
        CashflowId,
        CashflowParentId,
        AllocationItemId
      ),
      as.character
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
      RefreshDate         DATETIME2(3)   NOT NULL,
      ProjectNumber       NVARCHAR(20)   NOT NULL,
      ClientProjectNumber NVARCHAR(20)   NULL,
      ProjectId           NVARCHAR(20)   NULL,
      CashflowId          NVARCHAR(20)   NULL,
      CashflowParentId    NVARCHAR(20)   NULL,
      Period              NVARCHAR(50)   NULL,
      LineCategory        NVARCHAR(20)   NULL,
      ActivityCode        NVARCHAR(20)   NULL,
      ActivityCodeDesc    NVARCHAR(50)   NULL,
      ItemAmount          DECIMAL(18,2)  NULL,
      AllocationItemId    NVARCHAR(20)   NOT NULL,
      AllocationAmount    DECIMAL(18,2)  NULL,
      AllocationDate      DATE           NOT NULL,
      edp_update_ts       DATETIME2(3)   NULL
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

    dbExecute(
      con,
      paste0(
        "CREATE TABLE ",
        TEMP_TABLE,
        " (
        RefreshDate         DATETIME2(3)   NOT NULL,
        ProjectNumber       NVARCHAR(20)   NOT NULL,
        ClientProjectNumber NVARCHAR(20)   NULL,
        ProjectId           NVARCHAR(20)   NULL,
        CashflowId          NVARCHAR(20)   NULL,
        CashflowParentId    NVARCHAR(20)   NULL,
        Period              NVARCHAR(50)   NULL,
        LineCategory        NVARCHAR(20)   NULL,
        ActivityCode        NVARCHAR(20)   NULL,
        ActivityCodeDesc    NVARCHAR(50)   NULL,
        ItemAmount          DECIMAL(18,2)  NULL,
        AllocationItemId    NVARCHAR(20)   NOT NULL,
        AllocationAmount    DECIMAL(18,2)  NULL,
        AllocationDate      DATE           NOT NULL,
        edp_update_ts       DATETIME2(3)   NULL
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
           SELECT ProjectNumber, AllocationDate, AllocationItemId
           FROM ",
        TEMP_TABLE,
        "
           GROUP BY ProjectNumber, AllocationDate, AllocationItemId
           HAVING COUNT(*) > 1
         ) dupes;"
      )
    )$n

    if (dup_count > 0) {
      stop(paste0(
        "Duplicate ProjectNumber & AllocationDate & AllocationItemId values detected in source data (",
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
           tgt.RefreshDate         = src.RefreshDate,
           tgt.ClientProjectNumber = src.ClientProjectNumber,
           tgt.ProjectId           = src.ProjectId,
           tgt.CashflowId          = src.CashflowId,
           tgt.CashflowParentId    = src.CashflowParentId,
           tgt.Period              = src.Period,
           tgt.LineCategory        = src.LineCategory,
           tgt.ActivityCode        = src.ActivityCode,
           tgt.ActivityCodeDesc    = src.ActivityCodeDesc,
           tgt.ItemAmount          = src.ItemAmount,
           tgt.AllocationAmount    = src.AllocationAmount,
           tgt.edp_update_ts       = src.edp_update_ts
         FROM ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
         INNER JOIN ",
        TEMP_TABLE,
        " src
           ON tgt.ProjectNumber    = src.ProjectNumber
           AND tgt.AllocationDate  = src.AllocationDate
           AND tgt.AllocationItemId = src.AllocationItemId;"
      )
    )

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
        ProjectNumber,
        ClientProjectNumber,
        ProjectId,
        CashflowId,
        CashflowParentId,
        Period,
        LineCategory,
        ActivityCode,
        ActivityCodeDesc,
        ItemAmount,
        AllocationItemId,
        AllocationAmount,
        AllocationDate,
        edp_update_ts
      )
      SELECT
        src.RefreshDate,
        src.ProjectNumber,
        src.ClientProjectNumber,
        src.ProjectId,
        src.CashflowId,
        src.CashflowParentId,
        src.Period,
        src.LineCategory,
        src.ActivityCode,
        src.ActivityCodeDesc,
        src.ItemAmount,
        src.AllocationItemId,
        src.AllocationAmount,
        src.AllocationDate,
        src.edp_update_ts
      FROM ",
        TEMP_TABLE,
        " src
      LEFT JOIN ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
        ON  tgt.ProjectNumber     = src.ProjectNumber
        AND tgt.AllocationDate    = src.AllocationDate
        AND tgt.AllocationItemId  = src.AllocationItemId
      WHERE tgt.ProjectNumber IS NULL
        AND tgt.AllocationDate IS NULL
        AND tgt.AllocationItemId IS NULL;
      "
      )
    )

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
