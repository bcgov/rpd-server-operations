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
    LLOperationsMaintenance = LandlordProvidedOM,
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
      LLOperationsMaintenance +
      PropertyTax +
      Parking
  ) |>
  mutate(
    ContractName = case_when(
      # logic states if only one exists use that, however if both building and lease exist use lease.
      # seems to hold when spot checking
      !is.na(BuildingId) & is.na(PropertyId) & is.na(LeaseId) ~ BuildingId,
      !is.na(PropertyId) & is.na(BuildingId) & is.na(LeaseId) ~ PropertyId,
      !is.na(LeaseId) & is.na(PropertyId) & is.na(BuildingId) ~ LeaseId,
      !is.na(LeaseId) & !is.na(BuildingId) & is.na(PropertyId) ~ LeaseId,
      .default = "weird"
    ),
    .before = everything()
  )

assertthat::assert_that(
  sum(BudgetAssetAr$ContractName == "weird") == 0
)

# Budget Asset ####
BudgetAsset <- BudgetAssetData |>
  select(
    ContractName = budget_asset_asset_id,
    FiscalYear = budget_asset_budget_id,
    BuildingId = budget_asset_bl_id,
    RentableArea = budget_asset_area_space,
    ParkingStalls = budget_asset_parking_stalls
  )

# Leasing ####
Leasing <- LeasingData |>
  select(
    ls_ls_id,
    ls_status,
    ls_id_key,
    ls_bl_id,
    ls_pr_id,
    ls_lease_sublease,
    ls_ls_parent_id,
    ls_option1,
    ls_version,
    ls_area_negotiated,
    ls_date_start,
    ls_date_end,
    ls_date_terminated
  ) |>
  # filter(ls_lease_sublease %in% c("L", "P"))
  mutate(
    LeaseGroup = case_when(
      ls_lease_sublease %in% c("L", "P") ~ gsub("-V\\d+", "", ls_ls_id),
      ls_lease_sublease %in% c("A") & !is.na(ls_ls_parent_id) ~ gsub(
        "-V\\d+",
        "",
        ls_ls_parent_id
      ),
      .default = ls_ls_id
    )
  ) |>
  group_by(LeaseGroup) |>
  filter(ls_version == max(ls_version)) |>
  relocate(LeaseGroup, .after = ls_ls_id)


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
    PR_Tenure = Tenure,
    PR_PricingMethod = PricingMethod,
    PR_TotalRentableLand = TotalRentableLand,
    PR_linkAddress = linkAddress,
    PR_linkCity = linkCity
  )

# Create Report ####
PRR2015 <- BudgetAssetAr |>
  full_join(BudgetAsset, by = join_by(ContractName, FiscalYear)) |>
  mutate(
    BuildingId = case_when(
      is.na(BuildingId.x) & !is.na(BuildingId.y) ~ BuildingId.y,
      is.na(BuildingId.y) & !is.na(BuildingId.x) ~ BuildingId.x,
      BuildingId.x == BuildingId.y ~ BuildingId.x,
      is.na(BuildingId.x) & is.na(BuildingId.y) ~ NA_character_,
      .default = "weird"
    ),
    .keep = "unused",
    .after = FiscalYear
  ) |>
  relocate(
    RentableArea,
    ParkingStalls,
    .before = BaseRent
  ) |>
  # If its a parking contractname the rentable area is the # of stalls, if its land the hectares, building sqm
  # need to find the right join and setup conditions to get all the details in there
  left_join(Leasing, by = join_by(ContractName == ls_ls_id)) |>
  mutate(
    PrimaryLocation = case_when(
      (grepl("^P\\d+", ContractName) | startsWith(ContractName, "L")) &
        !is.na(ls_bl_id) ~ ls_bl_id,
      (grepl("^P\\d+", ContractName) | startsWith(ContractName, "L")) &
        !is.na(ls_pr_id) ~ ls_pr_id,
      startsWith(ContractName, "B") |
        startsWith(ContractName, "N") ~ ContractName,
      .default = "weird"
    ),
    .after = ContractName
  ) |>
  left_join(Building, by = join_by(PrimaryLocation == BuildingId)) |>
  left_join(Property, by = join_by(PrimaryLocation == PropertyId)) |>
  mutate(
    PricingMethod = case_when(
      !is.na(PricingMethod) ~ PricingMethod,
      is.na(PricingMethod) & !is.na(PR_PricingMethod) ~ PR_PricingMethod
    )
  ) |>
  relocate(PricingMethod, .after = FiscalYear) |>
  mutate(
    City = case_when(
      !is.na(linkCity) ~ linkCity,
      is.na(linkCity) & !is.na(PR_linkCity) ~ PR_linkCity
    ),
    .after = PricingMethod
  ) |>
  mutate(
    RentableArea = case_when(
      startsWith(ContractName, "P") ~ ParkingStalls,
      startsWith(PrimaryLocation, "N") ~ PR_TotalRentableLand,
      .default = RentableArea
    )
  ) |>
  mutate(
    CostRate = round(TotalCost / RentableArea, digits = 2)
  ) |>
  # PrimaryLocation edge cases
  # L5637 - somehow has a PrimaryLocation defined, seems its pulling via an option1 clause in a PreActive agreement
  # L5913 - doesn't exist in PRR2015 extract
  filter(!ls_status %in% c("Rejected")) |> # deal with one edge case L5913
  select(
    -c(
      BuildingId,
      PropertyId,
      LeaseId,
      ls_id_key,
      ls_bl_id,
      ls_pr_id,
      ls_lease_sublease,
      Tenure,
      bl_area_rentable,
      linkCity,
      PR_Tenure,
      PR_PricingMethod,
      PR_TotalRentableLand,
      PR_linkAddress,
      PR_linkCity,
      ls_status
    )
  ) |>
  arrange(ContractName, desc(FiscalYear))

# L5637
# L5913
# sum(PRR2015$ls_status == "Rejected", na.rm = TRUE)
# sum(PRR2015$ls_status == "Terminated", na.rm = TRUE)
# sum(PRR2015$ls_status == "Draft", na.rm = TRUE)
#
ExtractPRR2015 <- openxlsx2::read_xlsx(here::here(
  "input/PortfolioPerformance/2026-06-29_PRR2015_2526_2627.xlsx"
))

compare <- ExtractPRR2015 |>
  select(
    ContractName = `Contract Name`,
    PrimaryLocation = `Primary Location`,
    PricingMethod = `Pricing Method`,
    City,
    RentableArea = `2526 Year Rentable Area`
  ) |>
  mutate(
    RentableArea = as.double(gsub(",", "", RentableArea))
  )

compare_to <- PRR2015 |>
  filter(FiscalYear == "2526") |>
  select(
    ContractName,
    PrimaryLocation,
    PricingMethod,
    City,
    RentableArea
  )

outcome <- setdiff(compare, compare_to)

# Column mapping ####
# Contract Name - Calculated column
# Primary Location - Calculated column
# Pricing Method - Building or Property table
# City - Building or Property table
# Rentable Area - BudgetAsset or Property table
# Parking Stalls - Budget_asset
# Base Rent - Budget_asset_ar
# Operations and Maintenance - Budget_asset_ar
# Utilities - Budget_asset_ar
# Landlord Operations and Maintenance - Budget_asset_ar
# Property Tax - Budget_asset_ar
# Parking - Budget_asset_ar
# Landlord Admin fee - Budget_asset_ar
# Tax Admin - Budget_asset_ar
# Operations and Maintenance Admin - Budget_asset_ar
# Utilities Admin - Budget_asset_ar
# Total Admin - Calculated (Landlord Admin + Tax Admin + Operations Admin + Utilities Admin)
# Total Cost - Calculated (Base Rent + OperationsMaintenance + Utilities + Parking)
# Cost Rate - Calculated (Total Cost by Area)
# Variance - Calculated (year over year comparison)
