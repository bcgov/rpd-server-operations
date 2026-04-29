ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "RealProperty"
TABLE_NAME <- "FacilityZoning"
STAGE_TABLE <- paste0(TABLE_NAME, "_Stage")
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)

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

# Connect to SQL database
con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = SQL_SERVER,
  database = DB_NAME,
  Trusted_Connection = "Yes"
)

zbFile <- list.files(
  here::here("input"),
  pattern = "RPD_Buildings_Zoning"
) |>
  sort(decreasing = TRUE)

zoning_buildings <- read_xlsx(here(paste0("input/", zbFile[1]))) |>
  select(
    BuildingId = Building_Number,
    PropertyId = Land_Number,
    ZoneCode = ZONE_CODE,
    ZoneClass = ZONE_CLASS,
    ParcelName = PARCEL_NAME,
    ParcelClass = PARCEL_CLASS,
    ParcelStatus = PARCEL_STATUS,
    PlanNumber = PLAN_NUMBER,
    PID,
    PIN,
    OwnerType = OWNER_TYPE,
    Municipality = MUNICIPALITY,
    LegalMunicipality = MUNI_LEG,
    AbbrMunicipality = MUNI_ABBR,
    RegionalDistrict = REGIONAL_DISTRICT,
    BylawURL = BYLAW_URL,
    LastUpdateCycle = lastupdate,
    LastUpdated = WHEN_UPDATED
  )

zlFile <- list.files(
  here::here("input"),
  pattern = "RPD_Land_Zoning"
) |>
  sort(decreasing = TRUE)

zoning_lands <- read_xlsx(here::here("input", zlFile[1])) |>
  mutate(
    Building_Number = as.character(Building_Number),
    lastupdate = first(zoning_buildings$LastUpdateCycle)
  ) |>
  select(
    BuildingId = Building_Number,
    PropertyId = Land_Number,
    ZoneCode = ZONE_CODE,
    ZoneClass = ZONE_CLASS,
    ParcelName = PARCEL_NAME,
    ParcelClass = PARCEL_CLASS,
    ParcelStatus = PARCEL_STATUS,
    PlanNumber = PLAN_NUMBER,
    PID,
    PIN,
    OwnerType = OWNER_TYPE,
    Municipality = MUNICIPALITY,
    LegalMunicipality = MUNI_LEG,
    AbbrMunicipality = MUNI_ABBR,
    RegionalDistrict = REGIONAL_DISTRICT,
    BylawURL = BYLAW_URL,
    LastUpdateCycle = lastupdate,
    LastUpdated = WHEN_UPDATED
  )

FacilityZoning <- zoning_buildings |>
  union(zoning_lands)

dbWriteTable(
  con,
  name = Id(SCHEMA_NAME, TABLE_NAME),
  value = FacilityZoning,
  append = FALSE,
  overwrite = TRUE
)
