ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "RealProperty"
TABLE_NAME <- "FacilityDetail"
STAGE_TABLE <- paste0(TABLE_NAME, "_Stage")
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
SCRIPT_NAME <- "FacilityDetail"
API_NAME <- "BC Geocoder"

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
query <- dbSendQuery(con, "SELECT * FROM CbreStaging.Building")
BuildingData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.RoomTotal")
RoomTotalData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.Property")
PropertyData <- dbFetch(query, n = -1)
dbClearResult(query)

Building <- BuildingData |>
  filter(PobcStatus == "Active") |>
  filter(BuildingId != "TBD") |>
  filter(linkAddress != "Dummy Building") |>
  mutate(Identifier = BuildingId, PropertyArea = NA) |>
  mutate(
    across(
      c(
        bl_area_usable,
        bl_area_rentable
      ),
      as.double
    )
  ) |>
  mutate(
    across(
      c(
        BuildingDate
      ),
      as.POSIXct
    )
  ) |>
  select(
    Identifier,
    BuildingId,
    PropertyId,
    SiteId,
    Name,
    linkAddress,
    linkCity,
    Tenure,
    PrimaryUse,
    FacilityType,
    StrategicClassification,
    BuildingUsableArea = bl_area_usable,
    BuildingRentableArea = bl_area_rentable,
    BuildingDate,
    PropertyArea,
    lat = bl_lat,
    lon = bl_lon
  )

Property <- PropertyData |>
  mutate(
    Identifier = PropertyId,
    BuildingId = NA,
    FacilityType = NA,
    BuildingUsableArea = NA,
    BuildingRentableArea = NA,
    BuildingDate = NA
  ) |>
  select(
    Identifier,
    BuildingId,
    PropertyId,
    SiteId,
    Name,
    linkAddress,
    linkCity,
    Tenure,
    PrimaryUse,
    FacilityType,
    StrategicClassification,
    BuildingUsableArea,
    BuildingRentableArea,
    BuildingDate,
    PropertyArea = TotalRentableLand,
    lat,
    lon
  )

Table <- Building |>
  union(Property) |>
  mutate(
    PropertyId = case_when(
      startsWith(BuildingId, "N") & is.na(PropertyId) ~ BuildingId,
      .default = PropertyId
    )
  ) |>
  group_by(PropertyId) |>
  tidyr::fill(PropertyArea, .direction = "updown") |>
  ungroup()

AddressList <- Table |>
  select(linkAddress, linkCity) |>
  distinct() |>
  mutate(
    geo_name = "",
    score = "",
    precision = ""
  )

# Use geocoder to improve addresses
API_KEY <- keyring::key_get(service = "BCGEOCODER_API")

query_url = 'https://geocoder.api.gov.bc.ca/addresses.geojson?addressString='

for (ii in 1:nrow(AddressList)) {
  location <- paste0(
    stringr::str_replace_all(AddressList[ii, "linkAddress"], " ", "%20"),
    "%20",
    stringr::str_replace_all(AddressList[ii, "linkCity"], " ", "%20")
  )
  req <- request(paste0(query_url, location)) |>
    req_headers(API_KEY = API_KEY) |>
    apply_proxy_if_needed() |>
    req_perform()
  resp <- req |> resp_body_json()
  AddressList$geo_name[ii] <- resp$features[[1]]$properties$fullAddress
  AddressList$precision[ii] <- resp$features[[1]]$properties$precisionPoints
  AddressList$score[ii] <- resp$features[[1]]$properties$score
}

AddressListFinal <- AddressList |>
  separate_wider_delim(
    geo_name,
    delim = ",",
    names = c("geoAddress", "geoCity", "Province"),
    too_few = "align_start"
  ) |>
  mutate(
    geoAddress = trimws(geoAddress),
    geoCity = trimws(geoCity),
    Province = trimws(Province),
    score = as.numeric(score),
    precision = as.numeric(precision)
  ) |>
  mutate(
    geoAddress = gsub("--", "-", geoAddress),
  ) |>
  mutate(
    Address = case_when(
      score >= 85 & precision >= 99 ~ geoAddress,
      .default = linkAddress
    ),
    City = case_when(
      score >= 85 & precision >= 99 ~ geoCity,
      .default = linkCity
    ),
    GeoFlag = case_when(
      score >= 85 & precision >= 99 ~ TRUE,
      .default = FALSE
    )
  )

FacilityDetail <- Table |>
  left_join(AddressListFinal, by = join_by(linkAddress, linkCity)) |>
  mutate(
    RefreshDate = as.POSIXct(Sys.time())
  ) |>
  select(
    RefreshDate,
    Identifier,
    BuildingId,
    PropertyId,
    SiteId,
    Name,
    GeoFlag,
    Address,
    City,
    Tenure,
    PrimaryUse,
    FacilityType,
    StrategicClassification,
    BuildingUsableArea,
    BuildingRentableArea,
    BuildingDate,
    PropertyArea,
    linkCity,
    linkAddress,
    geoAddress,
    geoCity,
    Precision = precision,
    Score = score,
    lat,
    lon
  ) |>
  group_by(Identifier) |>
  mutate(count = n()) |>
  ungroup() |>
  filter(count == 1 | (count == 2 & !is.na(BuildingId))) |>
  select(-count)

# dbRemoveTable(con, Id(schema = SCHEMA_NAME, table = TABLE_NAME))
if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- paste0(
    " CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    TABLE_NAME,
    " (
    RefreshDate             DATETIME2(3)    NOT NULL,
    Identifier              NVARCHAR(50)    NOT NULL,
    BuildingId              NVARCHAR(50)    NULL,
    PropertyId              NVARCHAR(50)    NULL,
    SiteId                  NVARCHAR(50)    NULL,
    Name                    NVARCHAR(255)   NULL,
    GeoFlag                 BIT             NULL,
    Address                 NVARCHAR(255)   NULL,
    City                    NVARCHAR(100)   NULL,
    Tenure                  NVARCHAR(50)    NULL,
    PrimaryUse              NVARCHAR(100)   NULL,
    FacilityType            NVARCHAR(100)   NULL,
    StrategicClassification NVARCHAR(100)   NULL,
    BuildingUsableArea      DECIMAL(18,2)   NULL,
    BuildingRentableArea    DECIMAL(18,2)   NULL,
    BuildingDate            DATETIME2(3)    NULL,
    PropertyArea            DECIMAL(18,2)   NULL,
    linkAddress             NVARCHAR(255)   NULL,
    linkCity                NVARCHAR(100)   NULL,
    geoAddress              NVARCHAR(255)   NULL,
    geoCity                 NVARCHAR(100)   NULL,
    Precision               DECIMAL(5,2)    NULL,
    Score                   DECIMAL(5,2)    NULL,
    lat                     NVARCHAR(32)    NULL,
    lon                     NVARCHAR(32)    NULL
  );
  "
  )

  dbExecute(con, sql)
}

# Database Transaction ####
# Control database transaction to ensure all steps done together or not at all
dbBegin(con)

# Begin error handling and rollback of transaction on failure
tryCatch(
  {
    stage_id <- DBI::Id(
      schema = SCHEMA_NAME,
      table = STAGE_TABLE
    )

    if (dbExistsTable(con, stage_id)) {
      dbRemoveTable(con, stage_id)
    }

    # Create temp table to hold new data
    dbExecute(
      con,
      paste0(
        "
    CREATE TABLE ",
        SCHEMA_NAME,
        ".",
        STAGE_TABLE,
        " (
        RefreshDate             DATETIME2(3)    NOT NULL,
        Identifier              NVARCHAR(50)    NOT NULL,
        BuildingId              NVARCHAR(50)    NULL,
        PropertyId              NVARCHAR(50)    NULL,
        SiteId                  NVARCHAR(50)    NULL,
        Name                    NVARCHAR(255)   NULL,
        GeoFlag                 BIT             NULL,
        Address                 NVARCHAR(255)   NULL,
        City                    NVARCHAR(100)   NULL,
        Tenure                  NVARCHAR(50)    NULL,
        PrimaryUse              NVARCHAR(100)   NULL,
        FacilityType            NVARCHAR(100)   NULL,
        StrategicClassification NVARCHAR(100)   NULL,
        BuildingUsableArea      DECIMAL(18,2)   NULL,
        BuildingRentableArea    DECIMAL(18,2)   NULL,
        BuildingDate            DATETIME2(3)    NULL,
        PropertyArea            DECIMAL(18,2)   NULL,
        linkAddress             NVARCHAR(255)   NULL,
        linkCity                NVARCHAR(100)   NULL,
        geoAddress              NVARCHAR(255)   NULL,
        geoCity                 NVARCHAR(100)   NULL,
        Precision               DECIMAL(5,2)    NULL,
        Score                   DECIMAL(5,2)    NULL,
        lat                     NVARCHAR(32)    NULL,
        lon                     NVARCHAR(32)    NULL
    );
    "
      )
    )

    # Write into staging table the current Issues
    dbWriteTable(
      con,
      name = stage_id,
      value = FacilityDetail,
      append = TRUE,
      overwrite = FALSE
    )

    # Check staging table has data
    rows <- dbGetQuery(
      con,
      paste0(
        "
  SELECT COUNT(*) AS n FROM ",
        SCHEMA_NAME,
        ".",
        STAGE_TABLE
      )
    )

    if (rows$n == 0) {
      stop('Staging table is empty — aborting swap')
    }

    n_inserted <<- rows |> pull(n)

    dbExecute(
      con,
      "
  -- Ensure the old table name does not already exist
  IF OBJECT_ID('RealProperty.FacilityDetail_Old', 'U') IS NOT NULL
    DROP TABLE RealProperty.FacilityDetail_Old;

  -- Rename current table out of the way
  EXEC sp_rename
    'RealProperty.FacilityDetail',
    'FacilityDetail_Old',
    'OBJECT';

  -- Rename staging table into place
  EXEC sp_rename
    'RealProperty.FacilityDetail_Stage',
    'FacilityDetail',
    'OBJECT';
"
    )

    # Complete the transaction
    dbCommit(con)

    # rollback transaction on fail, completion of error handling
  },
  error = function(e) {
    dbRollback(con)
    stop(e)
  }
)
