ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "RealProperty"
# TABLE_NAME <- "dim_budget"
# CBRE_TABLE_NAME <- "dim_budget_vw"

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
options(digits = 22)

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

# target_table <- Id(schema = SCHEMA_NAME, table = TABLE_NAME)
# temp_table <- paste0("#", TABLE_NAME, "Temp")
# raw_data <- extract_cbre_data(CBRE_TABLE_NAME)

query <- dbSendQuery(con, "SELECT * FROM RealProperty.FacilityDetail")
FacilityDetail <- dbFetch(query, n = -1)
dbClearResult(query)

test_b <- buildings_report |> select(BuildingCode) |> mutate(Buildings = TRUE)
test_f <- FacilityAddress |> select(BuildingId) |> mutate(Facility = TRUE)

test_a <- test_f |> full_join(test_b, by = join_by(BuildingId == BuildingCode))

test_p <- lands_report |> select(PropertyCode) |> mutate(Properties = TRUE)
test_fp <- FacilityAddress |> select(PropertyId) |> mutate(Facility = TRUE)

test_a <- test_fp |> full_join(test_p, by = join_by(PropertyId == PropertyCode))
# From Archibus, Report Builders, Portfolio Report Builders, grab the buildings tab
# Building Code & Name, Address 1, City/Site/Property Code, Tenure, Building Number, Facility type and Primary Use,
# Strategic classification, usable and rentable area (col says sqft, set profile to use sqM), lat and long.
blFile <- list.files(
  here::here("input"),
  pattern = "PortfolioReport_Buildings"
) |>
  sort(decreasing = TRUE)

buildings_report <- read_xlsx(
  here(paste0("input/", blFile[1])),
  start_row = 3
) |> # Clean up column names to be more R friendly
  rename_with(~ gsub(" ", "", .), cols = everything()) |>
  rename(
    LinkAddress = Address1
  ) |>
  rename_with(~ gsub("[\\.]?[?]?[-]?", "", .), cols = everything()) |>
  rename_with(~ gsub("m²", "", .), .cols = everything()) |>
  # Remove leased buildings other than strategic leases
  filter(
    Tenure == "OWNED" |
      LinkAddress %in% c("4000 Seymour Pl", "4001 Seymour Pl", "1011 4th Ave")
  ) |>
  # modify column values to not hurt my eyeballs.
  mutate(
    City = stringr::str_to_title(tolower(CityCode)),
    Tenure = stringr::str_to_title(tolower(Tenure)),
    .after = LinkAddress,
    .keep = "unused"
  ) |>
  mutate(
    # Update Name as it will be easier on geocoder and I think its known at this point
    # City = case_when(
    #   City == "Daajing Giids (Queen Charlotte)" ~ "Daajing Giids",
    #   .default = City
    # ),
    # clean up blank entries and replace with NA
    FacilityType = na_if(FacilityType, " "),
    StrategicClassification = na_if(StrategicClassification, " ")
  ) |>
  # Setup to join with lands
  select(
    Identifier = BuildingCode,
    Name = BuildingName,
    PropertyCode,
    SiteCode,
    LinkAddress,
    City,
    FacilityType,
    PrimaryUse,
    StrategicClassification,
    UsableArea,
    BuildingRentableArea = RentableArea,
    Latitude,
    Longitude
  ) |>
  # Remove properties, will collect with lands_report
  filter(
    !startsWith(Identifier, "N")
  ) |>
  # Add column to line up with lands_report
  mutate(LandArea = NA, .before = BuildingRentableArea)

prFile <- list.files(
  here::here("input"),
  pattern = "PortfolioReport_Properties"
) |>
  sort(decreasing = TRUE)

lands_report <- read_xlsx(
  here(paste0("input/", prFile[1])),
  start_row = 3
) |>
  # Clean up column names
  rename_with(~ gsub(" ", "", .), cols = everything()) |>
  rename(
    LinkAddress = Address1
  ) |>
  rename_with(~ gsub("[\\.]?[?]?[-]?", "", .), cols = everything()) |>
  rename_with(~ gsub("m²", "", .), .cols = everything()) |>
  # remove leased lands, strategic leases aren't in here
  filter(PropertyStatus == "OWNED") |>
  # make data prettier
  mutate(
    City = stringr::str_to_title(tolower(CityName)),
    Tenure = stringr::str_to_title(tolower(PropertyStatus)),
    .keep = "unused"
  ) |>
  # clean up name
  # mutate(
  #   City = case_when(
  #     City == "Daajing Giids (Queen Charlotte)" ~ "Daajing Giids",
  #     .default = City
  #   )
  # ) |>
  mutate(
    Identifier = PropertyCode,
    # replace blanks with NA values
    PrimaryUse = na_if(PrimaryUse, " "),
    StrategicClassification = na_if(StrategicClassification, " ")
  ) |>
  # Add columns to line up with building_report
  mutate(FacilityType = "Land", UsableArea = NA) |>
  select(
    Identifier,
    Name = PropertyName,
    PropertyCode,
    SiteCode,
    LinkAddress,
    City,
    FacilityType,
    PrimaryUse,
    StrategicClassification,
    LandArea,
    BuildingRentableArea = AreaBldgRentable,
    UsableArea,
    Latitude,
    Longitude
  )

# join both reports together
Facility_Table <- rbind(buildings_report, lands_report)
