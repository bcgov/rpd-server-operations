#' @title ORG2000 Organization Reference Dataset
#'
#' @description
#' Produces an organizational reference dataset from Archibus division and
#' department information. The table combines division and department
#' hierarchies with customer classifications, fee-recovery attributes,
#' reconciliation settings, PAM participation indicators, and organizational
#' contacts to support reporting and operational analysis.
#'
#' @database BuildingIntelligence
#' @schema InfGold
#' @table ORG2000
#'
#' @source InfSilver.ArchibusDepartment Current-state department information
#' @source InfSilver.ArchibusDivision Current-state division information
#'
#' @load_type Full refresh
#' @grain One row per department within a division
#'
#' @filters No filter conditions
#'
#' @consumers TODO
#'
#' @column RefreshDate Timestamp when the dataset was refreshed
#' @column Division Division name
#' @column Department Department name
#' @column OrgType Organizational business-unit classification
#' @column DepartmentStatus Department status
#' @column SalesRepresentative Assigned sales representative
#' @column Collector Assigned collector
#' @column FeeRecovery Indicates whether fee recovery is enabled
#' @column Reconcile Indicates whether reconciliation is enabled
#' @column ParticipatesInPAM Indicates participation in PAM
#' @column DivisionId Division identifier
#' @column DepartmentId Department identifier
#' @column CustomerType Customer category classification
#'
#' @notes
#' Data is sourced from the current-state Silver layer and is intended for
#' business reporting rather than operational processing.
#'
#' Organizational indicators stored as source-system codes are transformed into
#' business-friendly values before publication.
#'
#' Each run builds the result in a temporary table and replaces the target
#' table inside a single database transaction. If the load fails, the
#' previously published dataset remains unchanged.
#'
#' @owner David Rattray
#' @last_reviewed 2026-11-23

# For server logging
# Begin timer
task_start <- Sys.time()
etl_error <- NULL

# Setup necessary variables ####
if (is.null(etl_error)) {
  tryCatch(
    {
      ETL_STATUS <- "DEV"
      SQL_SERVER <- if (ETL_STATUS == "PROD") {
        "dynamo.idir.bcgov\\CA_PRD"
      } else {
        "windfarm.idir.bcgov\\CA_TST"
      }
      DB_NAME <- "BuildingIntelligence"
      SCHEMA_NAME <- "InfGold"
      TABLE_NAME <- "ORG2000"
      TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
      TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
      SCRIPT_NAME <- "ORG2000"
      API_NAME <- "None"

      options(scipen = 999)
      options(digits = 7)
    },
    error = function(e) {
      log_etl_error(
        api_name = API_NAME,
        script_name = SCRIPT_NAME,
        table_name = TABLE_NAME,
        step = "setup_variables",
        condition = e
      )
      etl_error <<- e
    }
  )
}

# Connect to SQL database ####
if (is.null(etl_error)) {
  tryCatch(
    {
      con <- dbConnect(
        odbc(),
        driver = "ODBC Driver 17 for SQL Server",
        server = SQL_SERVER,
        database = DB_NAME,
        Trusted_Connection = "Yes"
      )
    },
    error = function(e) {
      log_etl_error(
        api_name = API_NAME,
        script_name = SCRIPT_NAME,
        table_name = TABLE_NAME,
        step = "database_connection",
        condition = e
      )
      etl_error <<- e
    }
  )
}

# Query SQL Datasets ####
if (is.null(etl_error)) {
  tryCatch(
    {
      query <- dbSendQuery(con, "SELECT * FROM InfSilver.ArchibusDepartment")
      DepartmentData <- dbFetch(query, n = -1)
      dbClearResult(query)

      query <- dbSendQuery(
        con,
        "SELECT DivisionId, DivisionName, BusinessId FROM InfSilver.ArchibusDivision"
      )
      DivisionData <- dbFetch(query, n = -1)
      dbClearResult(query)
    },
    error = function(e) {
      log_etl_error(
        api_name = API_NAME,
        script_name = SCRIPT_NAME,
        table_name = TABLE_NAME,
        step = "query_datasets",
        condition = e
      )
      etl_error <<- e
    }
  )
}

# Data Wrangling ####
if (is.null(etl_error)) {
  tryCatch(
    {
      Org <- DivisionData |>
        left_join(DepartmentData, by = join_by(DivisionId)) |>
        mutate(
          DepartmentStatus = case_when(
            DepartmentStatus == "A" ~ "Active",
            DepartmentStatus == "I" ~ "Inactive",
            TRUE ~ DepartmentStatus
          ),
          dp_recovery_fee = case_when(
            dp_recovery_fee == 0 ~ "No",
            dp_recovery_fee == 1 ~ "Yes"
          ),
          dp_reconciled = case_when(
            dp_reconciled == 0 ~ "No",
            dp_reconciled == 1 ~ "Yes"
          ),
          dp_pam = case_when(
            dp_pam == 0 ~ "No",
            dp_pam == 1 ~ "Yes"
          )
        ) |>
        select(
          Division = DivisionName,
          Department = DepartmentName,
          OrgType = BusinessId,
          DepartmentStatus,
          SalesRepresentative = dp_sales_rep,
          Collector = dp_collector,
          FeeRecovery = dp_recovery_fee,
          Reconcile = dp_reconciled,
          ParticipatesInPAM = dp_pam,
          DivisionId,
          DepartmentId,
          CustomerType = CustomerCategory,
        ) |>
        arrange(DivisionId, DepartmentId) |>
        mutate(
          RefreshDate = as.POSIXct(Sys.time(), tz = "UTC"),
          .before = everything()
        )
    },
    error = function(e) {
      log_etl_error(
        api_name = API_NAME,
        script_name = SCRIPT_NAME,
        table_name = TABLE_NAME,
        step = "data_wrangling",
        condition = e
      )
      etl_error <<- e
    }
  )
}

# dbRemoveTable(con, Id(schema = SCHEMA_NAME, table = TABLE_NAME))
if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- paste0(
    " CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    TABLE_NAME,
    " (
    RefreshDate             DATETIME2(3)    NOT NULL,
    Division                NVARCHAR(100)   NULL,
    Department              NVARCHAR(200)   NULL,
    OrgType                 NVARCHAR(20)    NULL,
    DepartmentStatus        NVARCHAR(20)    NULL,
    SalesRepresentative     NVARCHAR(50)    NULL,
    Collector               NVARCHAR(50)    NULL,
    FeeRecovery             NVARCHAR(10)    NULL,
    Reconcile               NVARCHAR(10)    NULL,
    ParticipatesInPAM       NVARCHAR(10)    NULL,
    DivisionId              NVARCHAR(20)    NULL,
    DepartmentId            NVARCHAR(50)    NULL,
    CustomerType            NVARCHAR(50)    NULL
  );
  "
  )

  dbExecute(con, sql)
}

# Database Transaction ####
etl_start_time <- Sys.time()

etl_error <- NULL
# Control database transaction to ensure all steps done together or not at all
dbBegin(con)

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
    CREATE TABLE  ",
        SCHEMA_NAME,
        ".",
        TEMP_TABLE,
        " (
          RefreshDate             DATETIME2(3)    NOT NULL,
          Division                NVARCHAR(100)   NULL,
          Department              NVARCHAR(200)   NULL,
          OrgType                 NVARCHAR(20)    NULL,
          DepartmentStatus        NVARCHAR(20)    NULL,
          SalesRepresentative     NVARCHAR(50)    NULL,
          Collector               NVARCHAR(50)    NULL,
          FeeRecovery             NVARCHAR(10)    NULL,
          Reconcile               NVARCHAR(10)    NULL,
          ParticipatesInPAM       NVARCHAR(10)    NULL,
          DivisionId              NVARCHAR(20)    NULL,
          DepartmentId            NVARCHAR(50)    NULL,
          CustomerType            NVARCHAR(50)    NULL
          );
  "
      )
    )

    dbWriteTable(
      con,
      name = TEMP_TABLE,
      value = Org,
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
        Division,
        Department,
        OrgType,
        DepartmentStatus,
        SalesRepresentative,
        Collector,
        FeeRecovery,
        Reconcile,
        ParticipatesInPAM,
        DivisionId,
        DepartmentId,
        CustomerType
      )
      SELECT * FROM ",
        TEMP_TABLE,
        ";"
      )
    )

    # Complete the transaction
    dbCommit(con)
    n_inserted <<- n_inserted
    cat("ETL complete — inserted:", n_inserted, "\n")
    # rollback transaction on fail, completion of error handling
  },
  error = function(e) {
    dbRollback(con)
    stop(e)
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
