# Load libraries
library(dplyr, quietly = TRUE, warn.conflicts = FALSE)
library(here, quietly = TRUE, warn.conflicts = FALSE)
library(tibble, quietly = TRUE, warn.conflicts = FALSE)
library(tidyr, quietly = TRUE, warn.conflicts = FALSE)
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

sensitive_list <- openxlsx2::read_xlsx(here::here(
  "input/RPD_Buildings_All_Sens.xlsx"
))

test <- FacilityDetail |>
  filter(!GeoFlag) |>
  select(Address, City) |>
  unique()
# B1002685
output <- FacilityDetail |>
  filter(!is.na(BuildingId)) |>
  filter(Tenure != "MANAGED") |>
  filter(startsWith(Identifier, "B")) |>
  filter(!BuildingId %in% sensitive_list$Building_Number) |>
  filter(GeoFlag) |>
  select(Address, City, GeoFlag) |>
  group_by(Address, City) |>
  unique()


openxlsx2::write_xlsx(
  test,
  file = here::here("output/GeoFailedAddresses.xlsx")
)
