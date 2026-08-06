# For server logging
# Begin timer
task_start <- Sys.time()

# Load helper functions
source(here::here("utilities/R/utilities.R"))

options(digits = 15)

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

# Budget Asset AR ####
BudgetAssetAr <- BudgetAssetArData |>
  select(
    FiscalYear = budget_asset_ar_budget_id,
    BuildingId = budget_asset_ar_bl_id,
    PropertyId = budget_asset_ar_pr_id,
    LeaseId = budget_asset_ar_ls_id,
    TotalAmount = budget_asset_ar_amt_total,
    CostCategory = budget_asset_ar_ar_cost_cat
  ) |>
  mutate(
    CostCategory = gsub("[ _&]", "", stringr::str_to_title(CostCategory))
  ) |>
  pivot_wider(
    id_cols = c(FiscalYear, BuildingId, PropertyId, LeaseId),
    names_from = CostCategory,
    values_from = TotalAmount
  ) |>
  select(
    FiscalYear,
    BuildingId,
    PropertyId,
    LeaseId,
    BaseRent,
    OperationsMaintenance,
    Utilities,
    LandLordOperationsMaintenance = LandlordProvidedOM,
    PropertyTax,
    Parking,
    AdminFee = AdministrationFee,
    LLAdminFee = LeaseAdministrationFee,
    TaxAdmin,
    OMAdmin,
    UtilityAdmin
  ) |>
  mutate(across(where(is.double), ~ replace_na(., 0))) |>
  mutate(
    TotalAdmin = AdminFee + LLAdminFee + TaxAdmin + OMAdmin + UtilityAdmin
  ) |>
  mutate(
    TotalCost = BaseRent +
      OperationsMaintenance +
      Utilities +
      LandLordOperationsMaintenance +
      PropertyTax +
      Parking
  )


ArTest <- BudgetAssetAr |>
  # filter(BuildingId == "B0010231") |>
  filter(LeaseId == "L1023") |>
  filter(FiscalYear %in% c("2526", "2627"))

# Budget Asset ####
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

# Leasing ####
Leasing <- LeasingData |>
  select(
    ls_ls_id,
    ls_id_key,
    ls_bl_id,
    ls_pr_id,
    ls_lease_sublease
  ) |>
  filter(ls_lease_sublease %in% c("L", "P"))

BudgetAssetLeases <- BudgetAssetAr |>
  filter(!is.na(LeaseId)) |>
  select(LeaseId)

assertthat::assert_that(
  length(setdiff(
    BudgetAssetAr |> filter(!is.na(LeaseId)) |> pull(LeaseId),
    Leasing$ls_ls_id
  )) ==
    0
)

# Building ####
Building <- BuildingData |>
  select(
    BuildingId,
    Tenure,
    PricingMethod,
    bl_area_rentable,
    linkCity
  )


BudgetBuilding <- BudgetAssetAr |>
  filter(!is.na(BuildingId)) |>
  select(BuildingId, PropertyId, LeaseId, FiscalYear) |>
  filter(FiscalYear == "2425") |>
  left_join(Building, by = join_by(BuildingId))

sum(!is.na(BudgetBuilding$PropertyId))
sum(!is.na(BudgetBuilding$LeaseId)) # different fiscal years will have or not have leaseids for a handful
sum(BudgetBuilding$Tenure == "LEASED") # but it seems to match to leased buildings, except for one 2425 year record

test <- BudgetBuilding |>
  filter(Tenure == "LEASED" & is.na(LeaseId))

test <- BudgetBuilding |>
  group_by(BuildingId) |>
  mutate(count = n()) |>
  filter(count > 1)

assertthat::assert_that(
  length(setdiff(
    BudgetAssetAr |> filter(!is.na(BuildingId)) |> pull(BuildingId),
    Building$BuildingId
  )) ==
    0
)


# Property ####
Property <- PropertyData |>
  select(
    PropertyId,
    Tenure,
    PricingMethod,
    TotalRentableLand,
    linkAddress,
    linkCity
  )

# Create Report ####

PRR2015 <- BudgetAssetAr |>
  left_join(Building, by = join_by(BuildingId))

test <- BudgetAssetAr |>
  filter(!is.na(LeaseId))

# Column mapping ####
# Contract Name
# Primary Location
# Pricing Method
# City
# Rentable Area
# Parking Stalls
# Base Rent - Budget_asset
# Operations and Maintenance - Budget_asset
# Utilities - Budget_asset
# Landlord Operations and Maintenance - Budget_asset
# Property Tax - Budget_asset
# Parking - Budget_asset
# Landlord Admin fee - Budget_asset
# Tax Admin - Budget_asset
# Operations and Maintenance Admin - Budget_asset
# Utilities Admin - Budget_asset
# Total Admin - Calculated (Landlord Admin + Tax Admin + Operations Admin + Utilities Admin)
# Total Cost - Calculated (Base Rent + OperationsMaintenance + Utilities + Parking)
# Cost Rate - Calculated (Total Cost by Area)
# Variance - Calculated (year over year comparison)
