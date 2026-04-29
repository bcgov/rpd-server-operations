ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "RealProperty"
TABLE_NAME <- "PRR2015"
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

fiscFile <- list.files(
  here::here("input"),
  pattern = "PRR2015"
) |>
  sort(decreasing = TRUE)

PRR2015 <- read_xlsx(
  here(
    "input/",
    fiscFile[1]
  ),
  start_row = 3
) |>
  rename_with(~ gsub(" ", "", .x)) |> # BROKEN AFTER THIS POINT, need a column name agnostic approach
  pivot_longer(
    cols = matches("^([0-9]{4})Year"),
    names_to = "raw_name",
    values_to = "value"
  ) |>
  mutate(
    FiscalYear = str_extract(raw_name, "^([0-9]{4})"),
    metric = str_extract(raw_name, "(?<=^[0-9]{4}Year)(.*)"),
    .keep = "unused"
  ) |>
  pivot_wider(names_from = metric, values_from = value) |>
  relocate(`Variance%`, .after = everything()) |>
  mutate(across(
    RentableArea:`Variance%`,
    ~ as.double(gsub(
      "[^0-9.-]",
      "",
      .x
    ))
  )) |>
  rename(Identifier = PrimaryLocation) |>
  filter(FiscalYear == min(FiscalYear))

dbWriteTable(
  con,
  name = Id(SCHEMA_NAME, TABLE_NAME),
  value = PRR2015,
  append = FALSE,
  overwrite = TRUE
)
