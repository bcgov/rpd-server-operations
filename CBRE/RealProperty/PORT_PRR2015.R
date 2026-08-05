# For server logging
# Begin timer
task_start <- Sys.time()

# Load helper functions
source(here::here("utilities/R/utilities.R"))

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
library(stringr, quietly = TRUE, warn.conflicts = FALSE)
library(openxlsx2, quietly = TRUE, warn.conflicts = FALSE)
library(odbc, quietly = TRUE, warn.conflicts = FALSE)
library(DBI, quietly = TRUE, warn.conflicts = FALSE)

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
query <- dbSendQuery(con, "SELECT * FROM CbreStaging.archibus_bl")
BuildingData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.archibus_ls")
LeasingData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.archibus_property")
PropertyData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.archibus_budget_asset")
BudgetAssetData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.archibus_budget_asset_ar")
BudgetAssetArData <- dbFetch(query, n = -1)
dbClearResult(query)


BudgetAssetAr <- BudgetAssetArData |>
  select(
    FiscalYear = budget_asset_ar_budget_id,
    BuildingId = budget_asset_ar_bl_id,
    PropertyId = budget_asset_ar_pr_id,
    LeaseId = budget_asset_ar_ls_id,
    TotalAmount = budget_asset_ar_amt_total,
    CostCategory = budget_asset_ar_ar_cost_cat
  ) |>
  pivot_wider(
    id_cols = c(FiscalYear, BuildingId, PropertyId, LeaseId),
    names_from = CostCategory,
    values_from = TotalAmount
  )

test1 <- BudgetAssetData |>
  filter(budget_asset_bl_id == "B0010223") |>
  filter(budget_asset_budget_id %in% c("2526", "2627"))

test2 <- BudgetAssetArData |>
  filter(budget_asset_ar_bl_id == "B0010223") |>
  filter(budget_asset_ar_budget_id %in% c("2526", "2627"))

Start <- BudgetAssetData |>
  select(
    FiscalYear = budget_asset_budget_id,
    BuildingId = budget_asset_bl_id,
    RentableArea = budget_asset_area_space,
    ParkingAmount = budget_asset_amt_parking_no_admin,
    ParkingStalls = budget_asset_parking_stalls
  )
