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
TABLE_NAME <- "PORT_WorkOrders"
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
SCRIPT_NAME <- "PORT_WorkOrders"
API_NAME <- "None"

# Connect to SQL database
con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = SQL_SERVER,
  database = DB_NAME,
  Trusted_Connection = "Yes"
)

# Query SQL Datasets ####
query <- dbSendQuery(
  con,
  "SELECT property_skey, FYCreation, COUNT(*) AS WorkOrderCount
                     FROM CbreStaging.fm_fact_workorder
                     GROUP BY property_skey, FYCreation;"
)
WorkOrderData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(
  con,
  "SELECT property_skey, Identifier FROM CbreStaging.fm_dim_property_extended_attribute;"
)
DimProperty <- dbFetch(query, n = -1)
dbClearResult(query)

WorkOrders <- WorkOrderData |>
  left_join(DimProperty, by = join_by(property_skey)) |>
  filter(!is.na(Identifier)) |> # approx 4k work orders missing the information to match to a building/land
  filter(FYCreation != "FYNANA") |> # additional ~20 are missing a creation date
  arrange(desc(FYCreation)) |>
  pivot_wider(names_from = FYCreation, values_from = WorkOrderCount) |>
  mutate(across(starts_with("FY"), ~ replace_na(., 0))) |>
  pivot_longer(
    starts_with("FY"),
    names_to = "FYCreation",
    values_to = "WorkOrderCount"
  ) |>
  ungroup() |>
  mutate(RefreshDate = as.POSIXct(Sys.time(), .before = everything()))

# dbRemoveTable(con, TARGET_TABLE)
if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- paste0(
    " CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    TABLE_NAME,
    " (
      RefreshDate             DATETIME2(3)    NOT NULL,
      property_skey           NVARCHAR(20)    NOT NULL,
      Identifier              NVARCHAR(50)    NOT NULL,
      FYCreation              NVARCHAR(25)    NOT NULL,
      WorkOrderCount          INT             NOT NULL
  );
  "
  )

  dbExecute(con, sql)
}

# Database Transaction ####
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
          property_skey           NVARCHAR(20)    NOT NULL,
          Identifier              NVARCHAR(50)    NOT NULL,
          FYCreation              NVARCHAR(25)    NOT NULL,
          WorkOrderCount          INT             NOT NULL
          );
  "
      )
    )

    dbWriteTable(
      con,
      name = TEMP_TABLE,
      value = WorkOrders,
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
        property_skey,
        Identifier,
        FYCreation,
        WorkOrderCount
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
