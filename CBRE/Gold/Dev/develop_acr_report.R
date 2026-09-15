# Load libraries
library(base64enc, quietly = TRUE, warn.conflicts = FALSE)
library(dplyr, quietly = TRUE, warn.conflicts = FALSE)
library(here, quietly = TRUE, warn.conflicts = FALSE)
library(httr2, quietly = TRUE, warn.conflicts = FALSE)
library(jsonlite, quietly = TRUE, warn.conflicts = FALSE)
library(lubridate, quietly = TRUE, warn.conflicts = FALSE)
library(purrr, quietly = TRUE, warn.conflicts = FALSE)
library(tibble, quietly = TRUE, warn.conflicts = FALSE)
library(tidyr, quietly = TRUE, warn.conflicts = FALSE)

library(odbc, quietly = TRUE, warn.conflicts = FALSE)
library(DBI, quietly = TRUE, warn.conflicts = FALSE)
# K1012013

# Load helper functions
source(here::here("utilities/R/event_logger.R"))
source(here::here("utilities/R/sql_helper_functions.R"))

ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "RealProperty"
TABLE_NAME <- "ActivityCodeReport"
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
SCRIPT_NAME <- "ActivityCodeReport"

options(scipen = 999)

# Connect to SQL database
con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = SQL_SERVER,
  database = DB_NAME,
  Trusted_Connection = "Yes"
)

# Query SQL Datasets ####
query <- dbSendQuery(con, "SELECT * FROM CbreStaging.pjm_dim_contact")
PjmDimContactData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.pjm_dim_project_role")
PjmDimProjectRoleData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.pjm_fact_project_role")
PjmFactProjectRoleData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.pjm_fact_project")
PjmFactProjectData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.pjm_dim_project")
PjmDimProjectData <- dbFetch(query, n = -1)
dbClearResult(query)

# query <- dbSendQuery(con, "SELECT * FROM CbreStaging.pjm_fact_budget")
# PjmFactBudgetData <- dbFetch(query, n = -1)
# dbClearResult(query)
#
# query <- dbSendQuery(con, "SELECT * FROM CbreStaging.pjm_dim_budget")
# PjmDimBudgetData <- dbFetch(query, n = -1)
# dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.pjm_dim_project_activity")
PjmDimProjectActivityData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.pjm_fact_project_activity")
PjmFactProjectActivityData <- dbFetch(query, n = -1)
dbClearResult(query)


# Prep data
ProjectRoleData <- PjmFactProjectRoleData |>
  filter(grepl("[0-9]_project_manager", source_unique_id)) |>
  select(project_skey, project_role_skey, contact_skey) |>
  left_join(PjmDimProjectRoleData, by = join_by(project_role_skey)) |>
  select(project_skey, project_role_skey, contact_skey, project_role) |>
  left_join(PjmDimContactData, by = join_by(contact_skey)) |>
  mutate(ProjectManager = paste(first_name, last_name)) |>
  select(project_skey, ProjectManager, ProjectManagerEmail = email_id)

# Review ACR Report and recreate
ProjectReview <- PjmFactProjectActivityData |>
  left_join(PjmDimProjectActivityData, by = join_by(project_activity_skey)) |>
  left_join(PjmFactProjectData, by = join_by(project_skey)) |>
  left_join(PjmDimProjectData, by = join_by(project_skey)) |>
  left_join(ProjectRoleData, by = join_by(project_skey)) |>
  mutate(
    CurrentBudget = round(
      budget_approved_total_value +
        budget_approved_adjustment_total_value +
        budget_approved_changes_total_value,
      digits = 2
    ),
    AnticipatedFinalCost = awarded_amount + cost_projected_changes_total_value,
    Variance = round(AnticipatedFinalCost - CurrentBudget, digits = 2),
    VariancePercent = round((Variance / CurrentBudget) * 100, digits = 2),
    code_sibling = stringr::str_sub(code, start = 5, end = 6),
    code_cousin = stringr::str_sub(code, start = 7, end = 9)
  ) |>
  select(
    ProjectSkey = project_skey,
    ProjectNumber = project_number,
    ProjectName = project_name,
    ProjectStatus = project_status,
    ProjectManager,
    ProjectManagerEmail,
    ParentCode = code_parent,
    code_sibling,
    code_cousin,
    ActivityCode = code,
    CodeType = code_type,
    Description = activity_desc,
    PreliminaryBudget = budget_estimated_total_value,
    ApprovedBudget = budget_approved_total_value,
    Adjustments = budget_approved_adjustment_total_value,
    ApprovedBudgetChanges = budget_approved_changes_total_value,
    CurrentBudget,
    OriginalCommitted = cost_original_total_value,
    ApprovedChanges = cost_approved_changes_total_value,
    CurrentCommitted = awarded_amount,
    PendingCommitments = cost_pending_commitments_total_value,
    PendingChanges = cost_pending_changes_total_value,
    ProjectedExposure = cost_projected_changes_total_value,
    AnticipatedFinalCost,
    Variance,
    VariancePercent,
    Invoiced = invoiced,
    Retained = retained,
    Paid = paid,
    Remaining = payables_remaining_total_value
  )


Test <- ProjectReview |>
  filter(CodeType == "detail") |>
  group_by(ProjectSkey, ParentCode, code_sibling) |>
  mutate(count = n()) |>
  filter(!(count >= 2 & code_cousin == "")) |>
  ungroup() |>
  filter(
    !if_all(
      c(
        PreliminaryBudget,
        CurrentBudget,
        CurrentCommitted,
        Invoiced,
        Retained,
        Paid,
        Remaining
      ),
      ~ .x == 0
    )
  ) |>
  arrange(
    ProjectNumber,
    ActivityCode
  )

ACR_Actual <- openxlsx2::read_xlsx(here(
  "input/ProjectForecast/Program ACR Report 2026-06-01.xlsx"
))

my_row_groups <- Test |>
  select(ProjectNumber) |>
  group_by(ProjectNumber) |>
  summarise(
    my_count = n()
  ) |>
  ungroup()

report_row_groups <- ACR_Actual |>
  select(ProjectNumber = `Project #`) |>
  group_by(ProjectNumber) |>
  summarise(
    report_count = n()
  ) |>
  ungroup()

compare <- report_row_groups |>
  full_join(my_row_groups, by = join_by(ProjectNumber)) |>
  mutate(
    Flag = case_when(report_count == my_count ~ TRUE, .default = FALSE)
  )

Check <- ProjectReview |>
  filter(ProjectNumber == "K1009489")
