#' @title PORT_WorkOrders
#'
#' @description
#' Project cost report at activity-code level for CBRE project management
#' data. For each project and detail activity code it brings together the
#' budget, commitment, forecast and payment figures, calculates the variance
#' against the current budget, and attaches the project manager.
#'
#' TODO: confirm and expand this description (intended use, who relies on it).
#'
#' @database  BuildingIntelligence
#' @schema    RealProperty
#' @table     ActivityCodeReport
#'
#' @source    CbreSilver.pjm_fact_project_activity  budget, cost, invoice and payment amounts
#' @source    CbreSilver.pjm_dim_project_activity  activity code, type, parent code and description
#' @source    CbreSilver.pjm_fact_project  project status and phase
#' @source    CbreSilver.pjm_dim_project  project number and name
#' @source    CbreSilver.pjm_fact_project_role  project manager role assignments
#' @source    CbreSilver.pjm_dim_project_role  role names
#' @source    CbreSilver.pjm_dim_contact  project manager name and email
#'
#' @load_type Full reload
#' @grain     One row per project and detail activity code
#'
#' @filters   Only activity codes with code type `detail`
#' @filters   Where two or more detail codes share a project, parent code and
#'   characters 5-6 of the activity code, the one without characters 7-9 is dropped
#' @filters   Rows where PreliminaryBudget, CurrentBudget, CurrentCommitted,
#'   Invoiced, Retained, Paid and Remaining are all zero are dropped
#' @filters   Project manager is taken from role records whose source_unique_id
#'   matches `<digit>_project_manager`
#'
#' @consumers TODO main report or dashboard this table was built for
#'
#' @column RefreshDate  Time the table was last reloaded
#' @column ProjectManager  First and last name of the project's manager
#' @column CurrentBudget  ApprovedBudget + Adjustments + ApprovedBudgetChanges
#' @column AnticipatedFinalCost  CurrentCommitted + ProjectedExposure
#' @column Variance  AnticipatedFinalCost - CurrentBudget
#' @column VariancePercent  Variance / CurrentBudget x 100; NULL where CurrentBudget is zero
#'
#' @notes    Each run builds the result in a temp table, then deletes all rows from the target and inserts the new rows inside one transaction, so a failed run leaves the previous data in place.
#'
#' @owner     TODO
#' @last_reviewed TODO
#'
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
                     FROM CbreSilver.fm_fact_workorder
                     GROUP BY property_skey, FYCreation;"
)
WorkOrderData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(
  con,
  "SELECT property_skey, Identifier FROM CbreSilver.fm_dim_property_extended_attribute;"
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
