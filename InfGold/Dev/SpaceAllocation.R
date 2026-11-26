#' @title Space Allocation Report
#'
#' @description
#' Produces a consolidated space-allocation dataset for buildings, properties,
#' leases, divisions, and departments within the Real Property portfolio. The
#' table combines occupancy assignments, room inventories, leasing information,
#' organizational ownership, and facility characteristics to calculate space,
#' land, and parking allocations across the portfolio.
#'
#' @database BuildingIntelligence
#' @schema InfGold
#' @table SpaceAllocation
#'
#' @source InfSilver.ArchibusRmpct Current room occupancy assignments
#' @source InfSilver.ArchibusRoom Current room inventory
#' @source InfSilver.ArchibusBuilding Current building inventory
#' @source InfSilver.ArchibusProperty Current property inventory
#' @source InfSilver.ArchibusLease Current lease inventory
#' @source InfSilver.ArchibusDivision Current division inventory
#' @source InfSilver.ArchibusDepartment Current department inventory
#' @source InfGold.FacilityDetail Address-validated facility information
#'
#' @load_type Full refresh
#' @grain One row per facility allocation relationship
#'
#' @filters Only active buildings and properties are included
#' @filters Buildings marked as TBD or dummy facilities are excluded
#' @filters Only active and holdover leases are included
#' @filters Room allocations with retired or rejected room statuses are excluded
#' @filters Common rooms not associated with allocation agreements are excluded
#'
#' @consumers Portfolio space allocation reporting
#' @consumers Occupancy analysis
#' @consumers Lease utilization reporting
#' @consumers Internal property portfolio dashboards
#'
#' @column RefreshDate Timestamp when the dataset was refreshed
#' @column BuildingLandKey Unified facility allocation key
#' @column Tenure Property ownership status
#' @column BCPNumber Business identifier derived from tenure and lease relationships
#' @column DataIntegrityFlag Allocation quality assessment status
#' @column BuildingId Building identifier
#' @column PropertyId Property identifier
#' @column SiteId Site identifier
#' @column GeoFlag Indicates whether geocoder-derived location information was used
#' @column Address Facility address
#' @column City Facility city
#' @column PrimaryUse Primary facility use classification
#' @column StrategicClassification Strategic property classification
#' @column TotalRentableArea Total rentable building area
#' @column TotalChargableArea Total chargeable building area
#' @column TotalUsableArea Total usable building area
#' @column TotalParkingStalls Total parking stalls
#' @column TotalRentableLand Total rentable land area
#' @column SpaceStatus Occupancy allocation status
#' @column LeaseStatus Lease status
#' @column ContractCode Lease or contract identifier
#' @column ParentLease Parent lease identifier
#' @column AgreementType Lease agreement classification
#' @column LeaseExpiryDate Lease expiry date
#' @column AllocatedChargableArea Chargeable area allocated to the relationship
#' @column PercentAreaAllocated Percentage of total rentable area allocated
#' @column AllocatedParkingStalls Parking stalls allocated to the relationship
#' @column PercentParkingAllocated Percentage of parking allocated
#' @column LeaseLandAppropriated Land appropriated by lease agreement
#' @column PercentLandAllocated Percentage of land allocated
#' @column CustomerCategory Department customer category
#' @column DivisionName Division name
#' @column DepartmentName Department name
#' @column Latitude Facility latitude
#' @column Longitude Facility longitude
#'
#' @notes
#' Data is sourced from both Silver and Gold layers and is intended for
#' business reporting and allocation analysis.
#'
#' The dataset combines room occupancy assignments, lease agreements,
#' organizational ownership structures, and facility reference information to
#' determine how building area, parking inventory, and land holdings are
#' allocated across the portfolio.
#'
#' Allocation percentages are calculated independently for building area,
#' parking stalls, and land holdings.
#'
#' DataIntegrityFlag provides business-rule validation checks intended to
#' identify allocation scenarios requiring review, including over-allocation,
#' missing lease relationships, and unexpected tenure configurations.
#'
#' Facility addresses and geographic coordinates are inherited from
#' InfGold.FacilityDetail.
#'
#' @owner David Rattray
#' @last_reviewed 2026-11-26

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
SCHEMA_NAME <- "InfGold"
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
query <- dbSendQuery(con, "SELECT * FROM InfSilver.ArchibusRoomPct")
RoomAllocatedData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM InfSilver.ArchibusRoom")
RoomTotalData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM InfSilver.ArchibusBuilding")
BuildingData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM InfSilver.ArchibusProperty")
PropertyData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM InfSilver.ArchibusLease")
LeasingData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM InfSilver.ArchibusDivision")
DivisionData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM InfSilver.ArchibusDepartment")
DepartmentData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM InfGold.FacilityDetail")
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
    DivisionId,
    DivisionName
  )

# Department ####
Department <- DepartmentData |>
  select(
    CustomerCategory,
    DepartmentName,
    DepartmentId,
    DivisionId
  )

# RoomAllocation ####
RoomAllocation <- RoomAllocatedData |>
  select(
    RoomPctId,
    BuildingId,
    FloorId,
    RoomId,
    RoomStatus,
    RoomPctLeaseId = LeaseId,
    DivisionId,
    DepartmentId,
    RoomAreaChargable,
    DateEnd,
    RoomCategory
  )

# RoomTotal ###
RoomTotal <- RoomTotalData |>
  select(
    BuildingId,
    FloorId,
    RoomId,
    RoomLeaseId = LeaseId,
    # DivisionId,
    # DepartmentId,
    ChargableArea
  )

# Building ####
Building <- BuildingData |>
  rename(
    linkAddress = Address,
    linkCity = City
  ) |>
  filter(PoBCStatus == "Active") |>
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
    StrategicClassification = StrategicClass,
    TotalRentableArea = AreaRentable,
    TotalUsableArea = AreaUsable,
    Latitude,
    Longitude
  )

# Property ####
Property <- PropertyData |>
  rename(
    linkAddress = Address,
    linkCity = City,
    PoBCStatus = Status
  ) |>
  filter(PoBCStatus == "Active") |>
  left_join(
    FacilityDetail,
    by = join_by(PropertyId, SiteId, linkAddress, linkCity)
  ) |>
  filter(is.na(BuildingId)) |>
  select(
    PoBCStatus,
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
    Name = PropertyName,
    Tenure,
    PrimaryUse,
    StrategicClassification = StrategicClass,
    TotalRentableLand = LandArea,
    Latitude,
    Longitude
  )

# Leasing ####
Leasing <- LeasingData |>
  filter(LeaseType != "Non Prop") |>
  select(
    LeaseStatus,
    LeaseId,
    LeaseBuildingId,
    LeasePropertyId,
    LeaseParentId,
    Option1,
    DateEnd,
    LeaseType,
    AppropriatedHectares,
    AppropriatedParkingStalls,
    AreaNegotiated,
    AppropriatedSqM,
    AreaCommon,
    AreaRentable,
    AreaUsable,
    Sublease,
    MultiTenant,
    NonStandard,
    OperationCostType,
    LeaseTenantName,
    LeaseVersion
  ) |>
  filter(LeaseStatus %in% c("Active", "Holdover"))

# Assemble Tables ############################################################
Dual_Properties <- PropertyData |>
  filter(Status == "Active") |>
  select(PropertyId, TotalRentableLand = LandArea)

Building_Start <- RoomTotal |>
  full_join(
    RoomAllocation,
    by = join_by(
      BuildingId,
      FloorId,
      RoomId
    )
  ) |>
  # keep rows without an end date or an end date that hasn't occurred,
  # which will cover duplicate rows that were ended for some reason (often retired)
  # filter(is.na(rmpct_date_end)) |>
  filter(DateEnd > as.POSIXct(Sys.Date()) | is.na(DateEnd)) |>
  # Filter out cases where the chargable area of a common room is not part of the agreement.
  filter(
    !(is.na(RoomPctLeaseId) &
      RoomCategory %in% c("FLOOR COMMON", "BUILDING COM"))
  ) |>
  # Filter out anything not assigned to any of a lease, division, or department as this is not allocated
  # filter(
  #   !(is.na(rmpct_ls_id) & is.na(rmpct_dv_id) & is.na(rmpct_dp_id))
  # ) |>
  filter(!RoomStatus %in% c("3", "4")) |>
  group_by(BuildingId) |>
  mutate(
    TotalChargableArea = sum(RoomAreaChargable, na.rm = TRUE),
    TotalParkingStalls = sum(
      grepl("^P", RoomId, ignore.case = TRUE),
      na.rm = TRUE
    ),
    NumberOfFloors = n_distinct(FloorId),
    NumberOfParkingFloors = n_distinct(
      FloorId[grepl("^P", FloorId, ignore.case = TRUE)],
      na.rm = TRUE
    )
  ) |>
  group_by(
    BuildingId,
    RoomLeaseId,
    RoomPctLeaseId,
    DivisionId,
    DepartmentId,
    RoomStatus
  ) |>
  summarise(
    TotalChargableArea = first(TotalChargableArea),
    TotalParkingStalls = first(TotalParkingStalls),
    AllocatedChargableArea = sum(RoomAreaChargable, na.rm = TRUE),
    AllocatedParkingStalls = sum(
      grepl("^P", RoomId, ignore.case = TRUE),
      na.rm = TRUE
    ),
    NumberOfFloors = first(NumberOfFloors),
    NumberOfParkingFloors = first(NumberOfParkingFloors),
    .groups = "drop_last"
  ) |>
  ungroup() |>
  filter(
    !(if_all(c(RoomPctLeaseId, DivisionId, DepartmentId), is.na) &
      (AllocatedChargableArea == 0 & AllocatedParkingStalls == 0))
  ) |>
  left_join(Leasing, by = join_by(RoomPctLeaseId == LeaseId)) |>
  full_join(Building, by = join_by(BuildingId)) |>
  left_join(Division, by = join_by(DivisionId)) |>
  left_join(Department, by = join_by(DivisionId, DepartmentId)) |>
  mutate(
    Source = "Building"
  ) |>
  filter(!is.na(Tenure)) |>
  left_join(Dual_Properties, by = join_by(BuildingId == PropertyId))


Building_Final <- Building_Start |>
  select(
    Tenure,
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
    SpaceStatus = RoomStatus,
    RmTotalLease = RoomLeaseId,
    LeaseStatus,
    ContractCode = RoomPctLeaseId,
    ParentLease = LeaseParentId,
    Option1,
    LeaseType = Sublease,
    AgreementType = LeaseType,
    LeaseExpiryDate = DateEnd,
    LeaseVersion,
    LeaseMultiTenant = MultiTenant,
    LeaseOptionCostType = OperationCostType,
    LeaseNonStandard = NonStandard,
    LeaseAreaNegotiated = AreaNegotiated,
    LeaseParkingAppropriated = AppropriatedParkingStalls,
    AllocatedChargableArea,
    AllocatedParkingStalls,
    LeaseLandAppropriated = AppropriatedHectares,
    CustomerCategory,
    DivisionName,
    DepartmentName,
    Latitude,
    Longitude,
    Precision,
    Score,
    linkAddress,
    linkCity,
    Source
  ) |>
  mutate(BuildingLandKey = BuildingId, .before = everything()) |>
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
  ungroup() |>
  mutate(
    PropertyId = case_when(
      is.na(PropertyId) & startsWith(BuildingId, "N") ~ BuildingId,
      .default = PropertyId
    )
  )

Property_Start <- Property |>
  filter(!PropertyId %in% Building_Start$BuildingId) |>
  left_join(Leasing, by = join_by(PropertyId == LeasePropertyId)) |>
  left_join(Department, by = join_by(LeaseTenantName == DepartmentId)) |>
  left_join(Division, by = join_by(DivisionId)) |>
  mutate(
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
    Tenure,
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
    ContractCode = LeaseId,
    ParentLease = LeaseParentId,
    Option1,
    LeaseType = Sublease,
    AgreementType = LeaseType,
    LeaseExpiryDate = DateEnd,
    LeaseVersion,
    LeaseMultiTenant = MultiTenant,
    LeaseOptionCostType = OperationCostType,
    LeaseNonStandard = NonStandard,
    LeaseAreaNegotiated = AreaNegotiated,
    LeaseParkingAppropriated = AreaNegotiated,
    AllocatedChargableArea,
    AllocatedParkingStalls,
    LeaseLandAppropriated = AppropriatedHectares,
    CustomerCategory,
    DivisionName,
    DepartmentName,
    Latitude,
    Longitude,
    Precision,
    Score,
    linkAddress,
    linkCity,
    Source
  ) |>
  mutate(
    BuildingLandKey = PropertyId,
    .before = everything()
  )

# Create Final Report ########################################################

SpaceAllocation <- Building_Final |>
  union(Property_Final) |>
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
        DataIntegrityFlag         NVARCHAR(50)    NULL,
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
        LeaseMultiTenant          BIT             NULL,
        LeaseOptionCostType       NVARCHAR(10)    NULL,
        LeaseNonStandard          BIT             NULL,
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
        Latitude                  DECIMAL(9,6)    NULL,
        Longitude                 DECIMAL(9,6)    NULL,
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
        DataIntegrityFlag         NVARCHAR(50)    NULL,
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
        LeaseMultiTenant          BIT             NULL,
        LeaseOptionCostType       NVARCHAR(10)    NULL,
        LeaseNonStandard          BIT             NULL,
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
        Latitude                  DECIMAL(9,6)    NULL,
        Longitude                 DECIMAL(9,6)    NULL,
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
        Latitude,
        Longitude,
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
    cat("ETL complete — inserted:", n_inserted, "\n")
    #     n_updated <<- n_updated
    #     # Rollback transaction on failure
  },
  error = function(e) {
    dbRollback(con)
    etl_error <<- e
    # stop(e)
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
