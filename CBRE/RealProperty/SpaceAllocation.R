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

options(scipen = 999)
options(digits = 7)

# Load helper functions
source(here::here("./utilities/R/event_logger.R"))
source(here::here("./utilities/R/sql_helper_functions.R"))

ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "RealProperty"
TABLE_NAME <- "SpaceAllocation"
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
SCRIPT_NAME <- "SpaceAllocation"
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
query <- dbSendQuery(con, "SELECT * FROM CbreStaging.RoomAllocation")
RoomAllocatedData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.RoomTotal")
RoomTotalData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.Building")
BuildingData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.Property")
PropertyData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.Leasing")
LeasingData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.Division")
DivisionData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.Department")
DepartmentData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(
  con,
  "SELECT * FROM RealProperty.FacilityDetail"
)

FacilityDetail <- dbFetch(query, n = -1)
dbClearResult(query)

FacilityDetail <- FacilityDetail |>
  select(
    BuildingId,
    PropertyId,
    SiteId,
    GeoFlag,
    City,
    Address,
    linkAddress,
    linkCity,
    Precision,
    Score
  )


# Format Data Tables #########################################################

# Division ####
Division <- DivisionData |>
  select(
    dv_dv_id,
    dv_name
  )

# Department ####
Department <- DepartmentData |>
  select(
    dp_customer_category,
    dp_name,
    dp_dp_id,
    dp_dv_id
  )

# RoomAllocation ####
RoomAllocation <- RoomAllocatedData |>
  select(
    rmpct_bl_id,
    rmpct_fl_id,
    rmpct_rm_id,
    rmpct_status,
    rmpct_ls_id,
    rmpct_dv_id,
    rmpct_dp_id,
    rmpct_area_chargable,
    rmpct_date_end,
    rmpct_rm_cat
  )

# RoomTotal ###
RoomTotal <- RoomTotalData |>
  select(
    rm_bl_id,
    rm_fl_id,
    rm_rm_id,
    rm_ls_id,
    rm_dv_id,
    rm_dp_id,
    rm_area_chargable
  )

# Building ####
Building <- BuildingData |>
  filter(PobcStatus == "Active") |>
  filter(BuildingId != "TBD") |>
  filter(linkAddress != "Dummy Building") |>
  left_join(
    FacilityDetail,
    by = join_by(BuildingId, PropertyId, SiteId, linkAddress, linkCity)
  ) |>
  select(
    BuildingId,
    PropertyId,
    SiteId,
    GeoFlag,
    Address,
    City,
    Precision,
    Score,
    linkAddress,
    linkCity,
    Name,
    Tenure,
    PrimaryUse,
    StrategicClassification,
    TotalRentableArea = bl_area_rentable,
    TotalUsableArea = bl_area_usable,
    lat = bl_lat,
    lon = bl_lon
  )

# Property ####
Property <- PropertyData |>
  filter(PobcStatus == "Active") |>
  left_join(
    FacilityDetail,
    by = join_by(PropertyId, SiteId, linkAddress, linkCity)
  ) |>
  filter(is.na(BuildingId)) |>
  select(
    BuildingId,
    PropertyId,
    SiteId,
    GeoFlag,
    Address,
    City,
    Precision,
    Score,
    linkAddress,
    linkCity,
    Name,
    Tenure,
    PrimaryUse,
    StrategicClassification,
    TotalRentableLand,
    lat,
    lon
  )

# Leasing ####
Leasing <- LeasingData |>
  filter(ls_fasb_ls_type != "Non Prop") |>
  select(
    ls_status,
    ls_ls_id,
    ls_bl_id,
    ls_pr_id,
    ls_ls_parent_id,
    ls_option1,
    ls_date_end,
    ls_fasb_ls_type,
    ls_appropriated_hectares,
    ls_appropriated_parking_stalls,
    ls_area_negotiated,
    ls_appropriated_sqm,
    ls_area_common,
    ls_area_rentable,
    ls_area_usable,
    ls_lease_sublease,
    ls_multi_tenant,
    ls_non_standard,
    ls_op_cost_type,
    ls_tn_name,
    ls_version
  ) |>
  filter(ls_status %in% c("Active", "Holdover"))

# Assemble Tables ############################################################
Dual_Properties <- PropertyData |>
  filter(PobcStatus == "Active") |>
  select(PropertyId, TotalRentableLand)

Building_Start <- RoomTotal |>
  full_join(
    RoomAllocation,
    by = join_by(
      rm_bl_id == rmpct_bl_id,
      rm_fl_id == rmpct_fl_id,
      rm_rm_id == rmpct_rm_id
    )
  ) |>
  # keep rows without an end date or an end date that hasn't occurred,
  # which will cover duplicate rows that were ended for some reason (often retired)
  # filter(is.na(rmpct_date_end)) |>
  filter(rmpct_date_end > as.POSIXct(Sys.Date()) | is.na(rmpct_date_end)) |>
  # Filter out cases where the chargable area of a common room is not part of the agreement.
  filter(
    !(is.na(rmpct_ls_id) & rmpct_rm_cat %in% c("FLOOR COMMON", "BUILDING COM"))
  ) |>
  # Filter out anything not assigned to any of a lease, division, or department as this is not allocated
  # filter(
  #   !(is.na(rmpct_ls_id) & is.na(rmpct_dv_id) & is.na(rmpct_dp_id))
  # ) |>
  filter(!rmpct_status %in% c("3", "4")) |>
  group_by(rm_bl_id) |>
  mutate(
    TotalChargableArea = sum(rm_area_chargable, na.rm = TRUE),
    TotalParkingStalls = sum(
      grepl("^P", rm_rm_id, ignore.case = TRUE),
      na.rm = TRUE
    ),
    NumberOfFloors = n_distinct(rm_fl_id),
    NumberOfParkingFloors = n_distinct(
      rm_fl_id[grepl("^P", rm_fl_id, ignore.case = TRUE)],
      na.rm = TRUE
    )
  ) |>
  group_by(
    rm_bl_id,
    rm_ls_id,
    rmpct_ls_id,
    rmpct_dv_id,
    rmpct_dp_id,
    rmpct_status
  ) |>
  summarise(
    TotalChargableArea = first(TotalChargableArea),
    TotalParkingStalls = first(TotalParkingStalls),
    AllocatedChargableArea = sum(rmpct_area_chargable, na.rm = TRUE),
    AllocatedParkingStalls = sum(
      grepl("^P", rm_rm_id, ignore.case = TRUE),
      na.rm = TRUE
    ),
    NumberOfFloors = first(NumberOfFloors),
    NumberOfParkingFloors = first(NumberOfParkingFloors)
  ) |>
  ungroup() |>
  filter(
    !(if_all(c(rmpct_ls_id, rmpct_dv_id, rmpct_dp_id), is.na) &
      (AllocatedChargableArea == 0 & AllocatedParkingStalls == 0))
  ) |>
  left_join(Leasing, by = join_by(rmpct_ls_id == ls_ls_id)) |>
  full_join(Building, by = join_by(rm_bl_id == BuildingId)) |>
  left_join(Department, by = join_by(rmpct_dp_id == dp_dp_id)) |>
  left_join(Division, by = join_by(rmpct_dv_id == dv_dv_id)) |>
  mutate(
    CustomerCategory = dp_customer_category,
    DivisionName = dv_name,
    DepartmentName = dp_name,
    Source = "Building"
  ) |>
  filter(!is.na(Tenure)) |>
  left_join(Dual_Properties, by = join_by(rm_bl_id == PropertyId))


Building_Final <- Building_Start |>
  select(
    BuildingLandKey = rm_bl_id,
    Tenure,
    BuildingId = rm_bl_id,
    PropertyId,
    SiteId,
    GeoFlag,
    Address,
    City,
    PrimaryUse,
    StrategicClassification,
    TotalRentableArea,
    TotalChargableArea,
    TotalUsableArea,
    TotalParkingStalls,
    TotalRentableLand,
    SpaceStatus = rmpct_status,
    RmTotalLease = rm_ls_id,
    LeaseStatus = ls_status,
    ContractCode = rmpct_ls_id,
    ParentLease = ls_ls_parent_id,
    Option1 = ls_option1,
    LeaseType = ls_lease_sublease,
    AgreementType = ls_fasb_ls_type,
    LeaseExpiryDate = ls_date_end,
    LeaseVersion = ls_version,
    LeaseMultiTenant = ls_multi_tenant,
    LeaseOptionCostType = ls_op_cost_type,
    LeaseNonStandard = ls_non_standard,
    LeaseAreaNegotiated = ls_area_negotiated,
    LeaseParkingAppropriated = ls_appropriated_parking_stalls,
    AllocatedChargableArea,
    AllocatedParkingStalls,
    LeaseLandAppropriated = ls_appropriated_hectares,
    CustomerCategory,
    DivisionName,
    DepartmentName,
    lat,
    lon,
    Precision,
    Score,
    linkAddress,
    linkCity,
    Source
  ) |>
  # Handle weird edge case with B0029637 - N0001220 as part of dual property identifier for parking
  rowwise() |>
  mutate(
    # check_flag = if_else(is.na(TotalRentableLand), "YES", "NO"),
    TotalRentableLand = case_when(
      is.na(TotalRentableLand) ~ Dual_Properties$TotalRentableLand[
        Dual_Properties$PropertyId == PropertyId
      ][[1]],
      .default = TotalRentableLand
    )
  ) |>
  ungroup()

Property_Start <- Property |>
  filter(!PropertyId %in% Building_Start$rm_bl_id) |>
  left_join(Leasing, by = join_by(PropertyId == ls_pr_id)) |>
  left_join(Department, by = join_by(ls_tn_name == dp_dp_id)) |>
  left_join(Division, by = join_by(dp_dv_id == dv_dv_id)) |>
  mutate(
    CustomerCategory = dp_customer_category,
    DivisionName = dv_name,
    DepartmentName = dp_name,
    SpaceStatus = NA_character_,
    RmTotalLease = NA_character_,
    TotalChargableArea = NA_integer_,
    TotalRentableArea = NA_integer_,
    TotalUsableArea = NA_integer_,
    TotalParkingStalls = NA_integer_,
    AllocatedParkingStalls = NA_integer_,
    AllocatedChargableArea = NA_integer_,
    NumberOfFloors = NA_integer_,
    NumberOfParkingFloors = NA_integer_,
  ) |>
  mutate(Source = "Property")

Property_Final <- Property_Start |>
  select(
    BuildingLandKey = PropertyId,
    Tenure,
    BuildingId,
    PropertyId = PropertyId,
    SiteId,
    GeoFlag,
    Address,
    City,
    PrimaryUse,
    StrategicClassification,
    TotalRentableArea,
    TotalChargableArea,
    TotalUsableArea,
    TotalParkingStalls,
    TotalRentableLand,
    SpaceStatus,
    RmTotalLease,
    LeaseStatus = ls_status,
    ContractCode = ls_ls_id,
    ParentLease = ls_ls_parent_id,
    Option1 = ls_option1,
    LeaseType = ls_lease_sublease,
    AgreementType = ls_fasb_ls_type,
    LeaseExpiryDate = ls_date_end,
    LeaseVersion = ls_version,
    LeaseMultiTenant = ls_multi_tenant,
    LeaseOptionCostType = ls_op_cost_type,
    LeaseNonStandard = ls_non_standard,
    LeaseAreaNegotiated = ls_area_negotiated,
    LeaseParkingAppropriated = ls_appropriated_parking_stalls,
    AllocatedChargableArea,
    AllocatedParkingStalls,
    LeaseLandAppropriated = ls_appropriated_hectares,
    CustomerCategory,
    DivisionName,
    DepartmentName,
    lat,
    lon,
    Precision,
    Score,
    linkAddress,
    linkCity,
    Source
  )

# Create Final Report ########################################################

SpaceAllocation <- Building_Final |>
  union(Property_Final) |>
  group_by(PropertyId) |>
  tidyr::fill(TotalRentableLand, .direction = "updown") |>
  mutate(
    BCPNumber = case_when(
      Tenure == "MANAGED" & Source == "Building" ~ BuildingId,
      Tenure == "MANAGED" & Source == "Property" ~ PropertyId,
      Tenure == "OWNED" & !is.na(ParentLease) ~ ParentLease,
      Tenure == "OWNED" &
        is.na(ParentLease) &
        Source == "Building" ~ BuildingId,
      Tenure == "OWNED" &
        is.na(ParentLease) &
        Source == "Property" ~ PropertyId,
      Tenure == "LEASED" & !is.na(ParentLease) ~ ParentLease,
      Tenure == "LEASED" &
        is.na(ParentLease) &
        !is.na(ContractCode) ~ ContractCode,
      .default = NA
    ),
    .after = Tenure
  ) |>
  mutate(
    PercentAreaAllocated = case_when(
      is.na(AllocatedChargableArea) ~ 0,
      TotalRentableArea == 0 & AllocatedChargableArea == 0 ~ 0,
      (TotalRentableArea == 0 | is.na(TotalRentableArea)) &
        AllocatedChargableArea > 0 ~ 999,
      !is.na(
        AllocatedChargableArea
      ) ~ round(
        (AllocatedChargableArea / TotalRentableArea) * 100,
        digits = 2
      )
    ),
    .after = AllocatedChargableArea
  ) |>
  mutate(
    PercentLandAllocated = case_when(
      is.na(LeaseLandAppropriated) ~ 0,
      TotalRentableLand == 0 & LeaseLandAppropriated == 0 ~ 0,
      (is.na(TotalRentableLand) | TotalRentableLand == 0) &
        LeaseLandAppropriated > 0 ~ 999,
      !is.na(
        LeaseLandAppropriated
      ) ~ round((LeaseLandAppropriated / TotalRentableLand) * 100, digits = 2)
    ),
    .after = LeaseLandAppropriated
  ) |>
  mutate(
    PercentParkingAllocated = case_when(
      is.na(AllocatedParkingStalls) ~ 0,
      TotalParkingStalls == 0 & AllocatedParkingStalls == 0 ~ 0,
      (is.na(TotalParkingStalls) | TotalParkingStalls == 0) &
        AllocatedParkingStalls > 0 ~ 999,
      !is.na(
        AllocatedParkingStalls
      ) ~ round(
        (AllocatedParkingStalls / TotalParkingStalls) * 100,
        digits = 2
      )
    ),
    .after = AllocatedParkingStalls
  ) |>
  mutate(
    DataIntegrityFlag = case_when(
      PercentAreaAllocated > 100 ~ "Suspect Area Calc",
      PercentLandAllocated > 100 ~ "Suspect Land Calc",
      PercentParkingAllocated > 100 ~ "Suspect Parking Calc",
      Tenure == "MANAGED" & Source == "Building" ~ "Good",
      Tenure == "MANAGED" & Source == "Property" ~ "Good",
      Tenure == "OWNED" & !is.na(ParentLease) ~ "Good",
      Tenure == "OWNED" & is.na(ParentLease) & Source == "Building" ~ "Good",
      Tenure == "OWNED" & is.na(ParentLease) & Source == "Property" ~ "Good",
      Tenure == "LEASED" & !is.na(LeaseStatus) & !is.na(ParentLease) ~ "Good",
      Tenure == "LEASED" & !is.na(LeaseStatus) & !is.na(ContractCode) ~ "Good",
      Tenure == "LEASED" & is.na(LeaseStatus) ~ "Suspect Lease Status",
      is.na(Tenure) ~ "Suspect Tenure",
      .default = "Unidentified but Suspect"
    ),
    .after = BCPNumber
  ) |>
  mutate(
    RefreshDate = as.POSIXct(Sys.time(), format = "%Y-%m-%dT%H:%M:%OS%z"),
    .before = everything()
  ) |>
  select(-c(Source))

# dbRemoveTable(con, Id(schema = SCHEMA_NAME, table = TABLE_NAME))
if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- paste0(
    " CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    TABLE_NAME,
    " (
        RefreshDate               DATETIME2(3)    NOT NULL,
        BuildingLandKey           NVARCHAR(50)    NULL,
        Tenure                    NVARCHAR(20)    NULL,
        BCPNumber                 NVARCHAR(50)    NULL,
        DataIntegrityFlag         NVARCHAR(20)    NULL,
        BuildingId                NVARCHAR(50)    NULL,
        PropertyId                NVARCHAR(50)    NULL,
        SiteId                    NVARCHAR(50)    NULL,
        GeoFlag                   BIT             NULL,
        Address                   NVARCHAR(150)   NULL,
        City                      NVARCHAR(100)   NULL,
        PrimaryUse                NVARCHAR(50)    NULL,
        StrategicClassification   NVARCHAR(50)    NULL,
        TotalRentableArea         DECIMAL(18,5)   NULL,
        TotalChargableArea        DECIMAL(18,5)   NULL,
        TotalUsableArea           DECIMAL(18,5)   NULL,
        TotalParkingStalls        INT             NULL,
        TotalRentableLand         DECIMAL(18,5)   NULL,
        SpaceStatus               NVARCHAR(10)    NULL,
        RmTotalLease              NVARCHAR(50)    NULL,
        LeaseStatus               NVARCHAR(25)    NULL,
        ContractCode              NVARCHAR(50)    NULL,
        ParentLease               NVARCHAR(50)    NULL,
        Option1                   NVARCHAR(50)    NULL,
        LeaseType                 NVARCHAR(25)    NULL,
        AgreementType             NVARCHAR(50)    NULL,
        LeaseExpiryDate           DATETIME2(3)    NULL,
        LeaseVersion              NVARCHAR(10)    NULL,
        LeaseMultiTenant          NVARCHAR(10)    NULL,
        LeaseOptionCostType       NVARCHAR(10)    NULL,
        LeaseNonStandard          NVARCHAR(10)    NULL,
        LeaseAreaNegotiated       DECIMAL(18,5)   NULL,
        LeaseParkingAppropriated  DECIMAL(18,5)   NULL,
        AllocatedChargableArea    DECIMAL(18,5)   NULL,
        PercentAreaAllocated      DECIMAL(10,2)   NULL,
        AllocatedParkingStalls    INT             NULL,
        PercentParkingAllocated   DECIMAL(10,2)   NULL,
        LeaseLandAppropriated     DECIMAL(18,5)   NULL,
        PercentLandAllocated      DECIMAL(10,2)   NULL,
        CustomerCategory          NVARCHAR(100)   NULL,
        DivisionName              NVARCHAR(100)   NULL,
        DepartmentName            NVARCHAR(100)   NULL,
        lat                       NVARCHAR(30)    NULL,
        lon                       NVARCHAR(30)    NULL,
        Precision                 DECIMAL(5,2)    NULL,
        Score                     DECIMAL(5,2)    NULL,
        linkAddress               NVARCHAR(150)   NULL,
        linkCity                  NVARCHAR(100)   NULL
  );
  "
  )

  dbExecute(con, sql)
}

# Database Transaction ####
etl_start_time <- Sys.time()

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
        RefreshDate               DATETIME2(3)    NOT NULL,
        BuildingLandKey           NVARCHAR(50)    NULL,
        Tenure                    NVARCHAR(20)    NULL,
        BCPNumber                 NVARCHAR(50)    NULL,
        DataIntegrityFlag         NVARCHAR(20)    NULL,
        BuildingId                NVARCHAR(50)    NULL,
        PropertyId                NVARCHAR(50)    NULL,
        SiteId                    NVARCHAR(50)    NULL,
        GeoFlag                   BIT             NULL,
        Address                   NVARCHAR(150)   NULL,
        City                      NVARCHAR(100)   NULL,
        PrimaryUse                NVARCHAR(50)    NULL,
        StrategicClassification   NVARCHAR(50)    NULL,
        TotalRentableArea         DECIMAL(18,5)   NULL,
        TotalChargableArea        DECIMAL(18,5)   NULL,
        TotalUsableArea           DECIMAL(18,5)   NULL,
        TotalParkingStalls        INT             NULL,
        TotalRentableLand         DECIMAL(18,5)   NULL,
        SpaceStatus               NVARCHAR(10)    NULL,
        RmTotalLease              NVARCHAR(50)    NULL,
        LeaseStatus               NVARCHAR(25)    NULL,
        ContractCode              NVARCHAR(50)    NULL,
        ParentLease               NVARCHAR(50)    NULL,
        Option1                   NVARCHAR(50)    NULL,
        LeaseType                 NVARCHAR(25)    NULL,
        AgreementType             NVARCHAR(50)    NULL,
        LeaseExpiryDate           DATETIME2(3)    NULL,
        LeaseVersion              NVARCHAR(10)    NULL,
        LeaseMultiTenant          NVARCHAR(10)    NULL,
        LeaseOptionCostType       NVARCHAR(10)    NULL,
        LeaseNonStandard          NVARCHAR(10)    NULL,
        LeaseAreaNegotiated       DECIMAL(18,5)   NULL,
        LeaseParkingAppropriated  DECIMAL(18,5)   NULL,
        AllocatedChargableArea    DECIMAL(18,5)   NULL,
        PercentAreaAllocated      DECIMAL(10,2)   NULL,
        AllocatedParkingStalls    INT             NULL,
        PercentParkingAllocated   DECIMAL(10,2)   NULL,
        LeaseLandAppropriated     DECIMAL(18,5)   NULL,
        PercentLandAllocated      DECIMAL(10,2)   NULL,
        CustomerCategory          NVARCHAR(100)   NULL,
        DivisionName              NVARCHAR(100)   NULL,
        DepartmentName            NVARCHAR(100)   NULL,
        lat                       NVARCHAR(30)    NULL,
        lon                       NVARCHAR(30)    NULL,
        Precision                 DECIMAL(5,2)    NULL,
        Score                     DECIMAL(5,2)    NULL,
        linkAddress               NVARCHAR(150)   NULL,
        linkCity                  NVARCHAR(100)   NULL
    );
  "
      )
    )

    dbWriteTable(
      con,
      name = TEMP_TABLE,
      value = SpaceAllocation,
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
        BuildingLandKey,
        Tenure,
        BCPNumber,
        DataIntegrityFlag,
        BuildingId,
        PropertyId,
        SiteId,
        GeoFlag,
        Address,
        City,
        PrimaryUse,
        StrategicClassification,
        TotalRentableArea,
        TotalChargableArea,
        TotalUsableArea,
        TotalParkingStalls,
        TotalRentableLand,
        SpaceStatus,
        RmTotalLease,
        LeaseStatus,
        ContractCode,
        ParentLease,
        Option1,
        LeaseType,
        AgreementType,
        LeaseExpiryDate,
        LeaseVersion,
        LeaseMultiTenant,
        LeaseOptionCostType,
        LeaseNonStandard,
        LeaseAreaNegotiated,
        LeaseParkingAppropriated,
        AllocatedChargableArea,
        PercentAreaAllocated,
        AllocatedParkingStalls,
        PercentParkingAllocated,
        LeaseLandAppropriated,
        PercentLandAllocated,
        CustomerCategory,
        DivisionName,
        DepartmentName,
        lat,
        lon,
        Precision,
        Score,
        linkAddress,
        linkCity
      )
       SELECT * FROM ",
        TEMP_TABLE,
        ";"
      )
    )

    # Complete the transaction
    dbCommit(con)
    #     n_deleted <<- n_deleted
    n_inserted <<- n_inserted
    #     n_updated <<- n_updated
    #     # Rollback transaction on failure
  },
  error = function(e) {
    dbRollback(con)
    etl_error <<- e
    # stop(e)
  }
)

if (is.null(etl_error)) {
  log_daily_etl_run(
    api_name = API_NAME,
    script_name = SCRIPT_NAME,
    table_name = TABLE_NAME,
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
