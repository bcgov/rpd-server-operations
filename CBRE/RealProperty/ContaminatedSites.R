ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "RealProperty"
TABLE_NAME <- "ContaminatedSites"
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

query <- dbSendQuery(con, "SELECT * FROM RealProperty.FacilityDetail")
FacilityDetail <- dbFetch(query, n = -1)
dbClearResult(query)

csites <- read_xlsx(
  here("input/2025-05-16-Contaminated_sites_inventory.xlsx"),
  sheet = "Apr 2025"
)

contaminated_building <- csites |>
  rename_with(~ gsub(" ", "", .), .cols = everything()) |>
  select(
    -c(
      City,
      BuildingRentableArea,
      BuildingDescription,
      BuildingAddress,
      LandArea,
      LandDescription,
      LandAddress
    )
  ) |>
  mutate(
    Building = case_when(
      nchar(Building) == 7 ~ paste0("B", Building),
      nchar(Building) == 6 ~ paste0("B0", Building),
      nchar(Building) == 5 ~ paste0("B00", Building)
    )
  ) |>
  mutate(
    Identifier = case_when(
      !is.na(Building) ~ Building,
      is.na(Building) & !is.na(Land) ~ Land,
      .default = NA
    ),
    .before = everything()
  ) |>
  select(-c(Land, LandTenure, BuildingTenure, `Ministry/Non-Ministry`)) |>
  rename(
    SiteInvestigated = `SiteInvestigated?-ReportAvailable`,
    ContaminationStatus = `IsSiteContaminated?`,
    StatusOfRemediation = StatusofRemediation
  ) |>
  mutate(
    SiteInvestigated = case_when(
      SiteInvestigated == "no" ~ "No",
      SiteInvestigated == "sample" ~ "Sample",
      .default = SiteInvestigated
    ),
    ContaminationStatus = case_when(
      ContaminationStatus == "no" ~ "No",
      ContaminationStatus == "yes" ~ "Yes",
      ContaminationStatus == "unknown" ~ "Unknown",
      .default = ContaminationStatus
    )
  )

dbWriteTable(
  con,
  name = Id(SCHEMA_NAME, TABLE_NAME),
  value = contaminated_building,
  append = FALSE,
  overwrite = TRUE
)
