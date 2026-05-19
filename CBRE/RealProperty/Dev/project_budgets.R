ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "CbreStaging"

options(scipen = 999)

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

# Review ACR Report and recreate
Project_K1000191 <- DimProjectData |>
  filter(project_number == "K1000191") |>
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
  left_join(FactBudgetData, by = join_by(project_skey)) |>
  select(
    project_number,
    project_skey,
    project_name,
    scope_desc,
    project_phase,
    budget_status,
    budget_skey,
    project_activity_skey,
    budget_desc,
    budget_item_skey,
    budget_item_id,
    approved_total_budget_value,
    estimated_budget_total_value,
    approved_budget_capital_total,
    estimated_budget_capital,
    budget_capital_value,
    approved_budget_expense,
    estimated_budget_expense,
    budget_expense_amount
  ) |>
  filter(budget_status == "Approved") |>
  arrange(project_number, budget_desc) |>
  left_join(FactProjectActivityData, by = join_by(pro))
