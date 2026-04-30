ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "RealProperty"
TABLE_NAME <- "FixedAssetByCity"
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

faFile <- list.files(
  here::here("input"),
  pattern = "Manually compiled with accruals"
) |>
  sort(decreasing = TRUE)

FixedAssetCity <- read_xlsx(
  here(
    "input/",
    faFile[1]
  ),
  sheet = "FAbyCity-Report",
  start_row = 9
) |>
  rename_with(~ gsub(" ", "", .x), everything()) |>
  mutate(
    Building = as.character(Building),
    Land = as.character(Land),
    MinistryInfo = as.character(MinistryInfo)
  ) |>
  mutate(
    Building = case_when(
      nchar(Building) == 7 ~ paste0("B", Building),
      nchar(Building) == 6 ~ paste0("B0", Building),
      nchar(Building) == 5 ~ paste0("B00", Building),
      is.na(Building) ~ Building,
      .default = "ERROR"
    ),
    Land = case_when(
      nchar(Land) == 7 ~ paste0("N", Land),
      nchar(Land) == 6 ~ paste0("N0", Land),
      nchar(Land) == 5 ~ paste0("N00", Land),
      nchar(Land) == 4 ~ paste0("N000", Land),
      is.na(Land) ~ Land,
      .default = "ERROR"
    )
  ) |>
  mutate(
    Identifier = case_when(
      is.na(Land) & !is.na(Building) ~ Building,
      !is.na(Land) & is.na(Building) ~ Land,
      !is.na(Land) & !is.na(Building) ~ Building,
      .default = "ERROR"
    ),
    .after = LastCloseDate
  ) |>
  filter(!(is.na(Land) & is.na(Building))) |>
  select(
    AsOfDate = AsofDate,
    LastCloseDate,
    Identifier,
    AssetNumber,
    AssetType,
    Building,
    Land,
    Site = Complex,
    Tenure = Ownership,
    CASMajor,
    CASMinor,
    ARESCategory,
    ARESMajor,
    ARESMinor,
    Project,
    PricingCode,
    PricingMethod,
    InServiceDate,
    Life,
    Rem.Life,
    CurrentCost,
    AccumulatedAmortization,
    MonthlyAmortization,
    NetBookValue,
    RetiredFlag,
    EndDate,
    OASResponsibility,
    OASServiceLine,
    RespSearch,
    PropertyType,
    PropertyLookup,
    Resp,
    SL,
    Key,
    CostSTOB,
    AASTOB,
    DeprSTOB,
    AccruedCosts,
    AccruedDepr,
    RevisedCost,
    RevisedAA,
    RevisedMonthllyAmort,
    RevisedNBV
  )

dbWriteTable(
  con,
  name = Id(SCHEMA_NAME, TABLE_NAME),
  value = FixedAssetCity,
  append = FALSE,
  overwrite = TRUE
)
