ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "RealProperty"
TABLE_NAME <- "VFA"
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

vfaFile <- list.files(
  here::here("input"),
  pattern = "VFA-AssetList"
) |>
  sort(decreasing = TRUE)

vfa <- read_xlsx(
  here(
    paste0("input/", vfaFile[1])
  )
) |>
  rename_with(
    .fn = ~ stringr::str_to_title(tolower(.x)),
    .cols = everything()
  ) |>
  mutate(
    Assetnumber = case_when(
      Assetnumber == "N2000527 / N2000497" ~ "N2000527",
      .default = Assetnumber
    )
  ) |>
  left_join(FacilityDetail, by = join_by(Assetnumber == Identifier)) |>
  filter(!is.na(Address)) |>
  select(
    AssetNumber = Assetnumber,
    RegionName = Regionname,
    Name,
    City,
    FCI = Fci,
    Address
  )

dbWriteTable(
  con,
  name = Id(SCHEMA_NAME, TABLE_NAME),
  value = vfa,
  append = FALSE,
  overwrite = TRUE
)
