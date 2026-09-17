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
source(here::here("./utilities/R/cbre_api_function.R"))
source(here::here("./utilities/R/event_logger.R"))
source(here::here("./utilities/R/sql_helper_functions.R"))

ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "CbreStaging"

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
query <- dbSendQuery(con, "SELECT * FROM CbreStaging.dim_project")
DimProjectData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.fact_project")
FactProjectData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.dim_budget")
DimBudgetData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.fact_budget")
FactBudgetData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.dim_project_activity")
DimProjectActivityData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.fact_project_activity")
FactProjectActivityData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.dim_project_role")
DimProjectRoleData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.fact_project_role")
FactProjectRoleData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.dim_contact")
DimContactData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.pjm_dim_project")
PjmDimProjectData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.pjm_fact_project")
PjmFactProjectData <- dbFetch(query, n = -1)
dbClearResult(query)

test <- PjmDimProjectData |>
  filter(
    project_number %in%
      c(
        "K1009600",
        "K1010408",
        "K1010993",
        "K1011138",
        "K1012076",
        "K1012778",
        "K1012821",
        "K1012911",
        "K1012912",
        "K1012916",
        "K1012920",
        "K1012935",
        "K1012958",
        "K1012961",
        "K1012962",
        "K1012963",
        "K1012969"
      )
  )

ProjectRoleData <- FactProjectRoleData |>
  filter(grepl("[0-9]_project_manager", source_unique_id)) |>
  select(project_skey, project_role_skey, contact_skey) |>
  left_join(DimProjectRoleData, by = join_by(project_role_skey)) |>
  select(project_skey, project_role_skey, contact_skey, project_role) |>
  left_join(DimContactData, by = join_by(contact_skey)) |>
  mutate(ProjectManager = paste(first_name, last_name)) |>
  select(project_skey, ProjectManager, ProjectManagerEmail = email_id)

# Review ACR Report and recreate
ProjectReview <- DimProjectData |>
  # filter(project_number == "K1000191") |>
  filter(
    project_number %in%
      c(
        "K1009600",
        "K1010408",
        "K1010993",
        "K1011138",
        "K1012076",
        "K1012778",
        "K1012821",
        "K1012911",
        "K1012912",
        "K1012916",
        "K1012920",
        "K1012935",
        "K1012958",
        "K1012961",
        "K1012962",
        "K1012963",
        "K1012969"
      )
  ) |>
  select(
    project_number,
    project_skey,
    project_name,
    scope_desc
  ) |>
  left_join(FactProjectData, by = join_by(project_skey)) |>
  select(
    project_number,
    project_skey,
    project_name,
    scope_desc,
    project_phase
  ) |>
  left_join(FactBudgetData, by = join_by(project_skey)) # |>
# select(
#   project_number,
#   project_skey,
#   project_name,
#   scope_desc,
#   project_phase,
#   budget_status,
#   budget_skey,
#   project_activity_skey,
#   budget_desc,
#   budget_item_skey,
#   budget_item_id,
#   approved_total_budget_value,
#   estimated_budget_total_value,
#   approved_budget_capital_total,
#   estimated_budget_capital,
#   budget_capital_value,
#   approved_budget_expense,
#   estimated_budget_expense,
#   budget_expense_amount
# ) |>
filter(budget_status == "Approved") |>
  arrange(project_number, budget_desc) |>
  mutate(
    across(
      c(
        project_skey,
        project_activity_skey
      ),
      as.character
    )
  ) |>
  left_join(
    FactProjectActivityData,
    by = join_by(project_skey, project_activity_skey)
  ) |>
  left_join(DimProjectActivityData, by = join_by(project_activity_skey)) |>
  # select(
  #   project_number,
  #   project_skey,
  #   project_name,
  #   scope_desc,
  #   project_phase,
  #   budget_status,
  #   budget_skey,
  #   project_activity_skey,
  #   budget_desc,
  #   budget_item_skey,
  #   budget_item_id,
  #   approved_total_budget_value,
  #   estimated_budget_total_value,
  #   approved_budget_capital_total,
  #   estimated_budget_capital,
  #   budget_capital_value,
  #   approved_budget_expense,
  #   estimated_budget_expense,
  #   budget_expense_amount,
  #   record_type,
  #   paid,
  #   retained,
  #   invoiced,
  #   code,
  #   code_type,
  #   code_parent,
  #   activity_desc,
  #   budget_adjustments_total_value,
  #   budget_approved_changes_total_value,
  #   budget_approved_adjustment_total_value,
  #   budget_approved_total_value,
  #   budget_estimated_total_value,
  #   budget_contract_approved_total_value,
  #   cost_approved_total_value,
  #   cost_projected_changes_total_value,
  #   cost_pending_changes_total_value,
  #   cost_pending_commitments_total_value,
  #   cost_approved_changes_total_value,
  #   cost_original_total_value,
  #   cost_estimated_total_value,
  #   balance_to_complete,
  #   awarded_amount,
  #   payables_remaining_total_value
  # ) |>
  left_join(
    ProjectRoleData,
    by = join_by(project_skey)
  ) #|>
select(
  ProjectNumber = project_number,
  ProjectName = project_name,
  # Project Status,
  ProjectManager,
  ProjectManagerEmail,
  ActivityCode = code,
  Description = activity_desc,
  budget_desc,
  # PreliminaryBudget, # this one is a bad example as all zeroes.
  ApprovedBudget = budget_approved_total_value,
  Adjustments = budget_approved_adjustment_total_value,
  ApprovedBudgetChanges = budget_approved_changes_total_value,
  CurrentBudget = cost_approved_total_value,
  OriginalCommitted = cost_original_total_value,
  ApprovedChanges = cost_approved_changes_total_value,
  CurrentCommitted = awarded_amount, # Unsure if this makes sense as the correct column
  # PendingCommitments,
  # PendingChanges,
  # ProjectedExposure,
  # AnticipatedFinalCost,
  # Variance,
  # VariancePercentage,
  Invoiced = invoiced,
  Retained = retained,
  Paid = paid,
  Remaining = payables_remaining_total_value
) |>
  filter(
    !if_all(
      c(
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
