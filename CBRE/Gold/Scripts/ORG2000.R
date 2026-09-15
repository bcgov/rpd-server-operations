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
SCHEMA_NAME <- "RealProperty"
TABLE_NAME <- "ORG2000"
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
SCRIPT_NAME <- "ORG2000"
API_NAME <- "None"

options(scipen = 999)
options(digits = 7)

# Connect to SQL database
con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = SQL_SERVER,
  database = DB_NAME,
  Trusted_Connection = "Yes"
)

# Query SQL Datasets ####
query <- dbSendQuery(con, "SELECT * FROM CbreSiler.archibus_dp")
DepartmentData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreSilver.archibus_dv")
DivisionData <- dbFetch(query, n = -1)
dbClearResult(query)

Org <- DivisionData |>
  left_join(DepartmentData, by = join_by(dv_dv_id == dp_dv_id)) |>
  mutate(
    dp_status = case_when(
      dp_status == "A" ~ "Active",
      dp_status == "I" ~ "Inactive"
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
    Division = dv_name,
    Department = dp_name,
    OrgType = dv_bu_id,
    Status = dp_status,
    SalesRepresentative = dp_sales_rep,
    Collector = dp_collector,
    FeeRecovery = dp_recovery_fee,
    Reconcile = dp_reconciled,
    ParticipatesInPAM = dp_pam,
    DivisionId = dv_dv_id,
    DepartmentId = dp_dp_id,
    CustomerType = dp_customer_category,
  ) |>
  arrange(DivisionId, DepartmentId) |>
  mutate(
    RefreshDate = as.POSIXct(Sys.time(), tz = "UTC"),
    .before = everything()
  )

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
    Status                  NVARCHAR(20)    NULL,
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
          Status                  NVARCHAR(20)    NULL,
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
        Status,
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
