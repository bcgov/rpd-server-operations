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
TABLE_NAME <- "PRR2015"
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
SCRIPT_NAME <- "PRR2015"
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

# assertthat::assert_that(
#   sum(BudgetAssetAr$ContractName == "weird") == 0
# )

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
  mutate(count = n()) |>
  filter(count > 4)
filter(ls_version == max(ls_version)) |>
  ungroup() |>
  mutate(ls_ls_id = gsub("-V//d+", "", ls_ls_id))

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

# assertthat::assert_that(
#   length(setdiff(
#     BudgetAssetAr |> filter(!is.na(BuildingId)) |> pull(BuildingId),
#     Building$BuildingId
#   )) ==
#     0
# )

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
      startsWith(ContractName, "L") &
        PR_Tenure == "LEASED" &
        ls_area_negotiated == 0 ~ PR_TotalRentableLand,
      startsWith(ContractName, "L") ~ ls_area_negotiated,
      startsWith(ContractName, "N") ~ PR_TotalRentableLand,
      .default = RentableArea
    )
  ) |>
  mutate(
    CostRate = case_when(
      RentableArea != 0 & TotalCost != 0 ~ round(
        TotalCost / RentableArea,
        digits = 2
      ),
      .default = 0
    )
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
      ls_status,
      ls_area_negotiated
    )
  ) |>
  mutate(
    across(
      where(is.numeric),
      ~ round(.x, digits = 2)
    )
  ) |>
  arrange(ContractName, desc(FiscalYear)) |>
  mutate(RefreshDate = as.POSIXct(Sys.time()), .before = everything())

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

# Database Transaction ####
# dbRemoveTable(con, TARGET_TABLE)
if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- paste0(
    "CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    TABLE_NAME,
    " (
        RefreshDate              DATETIME2(3)  NOT NULL,
        ContractName             NVARCHAR(20)  NOT NULL,
        PrimaryLocation          NVARCHAR(20)  NOT NULL,
        FiscalYear               NVARCHAR(10)  NOT NULL,
        PricingMethod            NVARCHAR(50)  NULL,
        City                     NVARCHAR(50)  NULL,
        RentableArea             DECIMAL(18,5) NULL,
        ParkingStalls            INT           NULL,
        BaseRent                 DECIMAL(18,2) NULL,
        OperationsMaintenance    DECIMAL(18,2) NULL,
        Utilities                DECIMAL(18,2) NULL,
        LLOperationsMaintenance  DECIMAL(18,2) NULL,
        PropertyTax              DECIMAL(18,2) NULL,
        Parking                  DECIMAL(18,2) NULL,
        AdminFee                 DECIMAL(18,2) NULL,
        LLAdminFee               DECIMAL(18,2) NULL,
        TaxAdmin                 DECIMAL(18,2) NULL,
        OMAdmin                  DECIMAL(18,2) NULL,
        UtilityAdmin             DECIMAL(18,2) NULL,
        TotalAdmin               DECIMAL(18,2) NULL,
        TotalCost                DECIMAL(18,2) NULL,
        CostRate                 DECIMAL(18,4) NULL
      );"
  )
  dbExecute(con, sql)
}

etl_error <- NULL

# Control database transaction to ensure all steps done together or not at all
dbBegin(con)

# Begin error handling and roll back on transaction failure
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
    CREATE TABLE ",
        SCHEMA_NAME,
        ".",
        TEMP_TABLE,
        " (
      RefreshDate              DATETIME2(3)  NOT NULL,
      ContractName             NVARCHAR(20)  NOT NULL,
      PrimaryLocation          NVARCHAR(20)  NOT NULL,
      FiscalYear               NVARCHAR(10)  NOT NULL,
      PricingMethod            NVARCHAR(50)  NULL,
      City                     NVARCHAR(50)  NULL,
      RentableArea             DECIMAL(18,5) NULL,
      ParkingStalls            INT           NULL,
      BaseRent                 DECIMAL(18,2) NULL,
      OperationsMaintenance    DECIMAL(18,2) NULL,
      Utilities                DECIMAL(18,2) NULL,
      LLOperationsMaintenance  DECIMAL(18,2) NULL,
      PropertyTax              DECIMAL(18,2) NULL,
      Parking                  DECIMAL(18,2) NULL,
      AdminFee                 DECIMAL(18,2) NULL,
      LLAdminFee               DECIMAL(18,2) NULL,
      TaxAdmin                 DECIMAL(18,2) NULL,
      OMAdmin                  DECIMAL(18,2) NULL,
      UtilityAdmin             DECIMAL(18,2) NULL,
      TotalAdmin               DECIMAL(18,2) NULL,
      TotalCost                DECIMAL(18,2) NULL,
      CostRate                 DECIMAL(18,4) NULL
    );
  "
      )
    )

    dbWriteTable(
      con,
      name = TEMP_TABLE,
      value = PRR2015,
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
        ContractName,
        PrimaryLocation,
        FiscalYear,
        PricingMethod,
        City,
        RentableArea,
        ParkingStalls,
        BaseRent,
        OperationsMaintenance,
        Utilities,
        LLOperationsMaintenance,
        PropertyTax,
        Parking,
        AdminFee,
        LLAdminFee,
        TaxAdmin,
        OMAdmin,
        UtilityAdmin,
        TotalAdmin,
        TotalCost,
        CostRate
      )
       SELECT * FROM ",
        TEMP_TABLE,
        ";"
      )
    )

    # Complete the transaction
    dbCommit(con)

    # Hoist to main environment
    n_inserted <<- n_inserted
    cat("ETL complete — inserted:", n_inserted, "\n")
    # Rollback transaction on failure
  },
  error = function(e) {
    dbRollback(con)
    etl_error <<- e
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
