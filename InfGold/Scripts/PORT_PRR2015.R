#' @title PORT_PRR2015
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
TABLE_NAME <- "PORT_PRR2015"
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
SCRIPT_NAME <- "PORT_PRR2015"
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
query <- dbSendQuery(con, "SELECT * FROM RealProperty.PRR2015")
PRR2015 <- dbFetch(query, n = -1)
dbClearResult(query)

fiscFile <- list.files(
  here::here("input"),
  pattern = "PRR2015"
) |>
  sort(decreasing = TRUE)

PRR2015Extract <- read_xlsx(
  here(
    "input/",
    # fiscFile[1]
    "PRR2015_20260429.xlsx"
  ),
  start_row = 3
) |>
  rename_with(~ gsub(" ", "", .x)) |>
  pivot_longer(
    cols = matches("^([0-9]{4})Year"),
    names_to = "raw_name",
    values_to = "value"
  ) |>
  mutate(
    FiscalYear = str_extract(raw_name, "^([0-9]{4})"),
    metric = str_extract(raw_name, "(?<=^[0-9]{4}Year)(.*)"),
    .keep = "unused"
  ) |>
  pivot_wider(names_from = metric, values_from = value) |>
  relocate(`Variance%`, .after = everything()) |>
  mutate(across(
    RentableArea:`Variance%`,
    ~ as.double(gsub(
      "[^0-9.-]",
      "",
      .x
    ))
  )) |>
  rename(Identifier = PrimaryLocation) |>
  filter(FiscalYear == min(FiscalYear))

dbWriteTable(
  con,
  name = Id(SCHEMA_NAME, TABLE_NAME),
  value = PRR2015,
  append = FALSE,
  overwrite = TRUE
)

test <- PRR2015 |>
  mutate(
    cost_check_total = rowSums(across(BaseRent:Parking), na.rm = TRUE),
    admin_check_total = rowSums(across(LLAdminFee:UtilitiesAdmin), na.rm = TRUE)
  ) |>
  relocate(
    TotalCost,
    cost_check_total,
    TotalAdmin,
    admin_check_total,
    .before = everything()
  )
