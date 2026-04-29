ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "RealProperty"
TABLE_NAME <- "CSR001"
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

carFile <- list.files(
  here::here("input"),
  pattern = "csr0001"
) |>
  sort(decreasing = TRUE)

car3 <- read_xlsx(
  here(
    "input/",
    carFile[3]
  ),
  start_row = 3
)

car2 <- read_xlsx(
  here(
    "input/",
    carFile[2]
  ),
  start_row = 3
)

car1 <- read_xlsx(
  here(
    "input/",
    carFile[1]
  ),
  start_row = 3
)

CSR001 <- rbind(car1, car2, car3) |>
  select(
    Identifier = Location,
    Client,
    AgreementNumber = `Agreement #`,
    AgreementStatus = `Agr Status`,
    FiscalYear = `Fiscal Year`,
    AgreementType = `Agreement Type`,
    Lease,
    LeaseStart = `Lease Start`,
    LeaseExpiry = `Lease Expiry`,
    AgreementDurationEndDate = `Agreement Duration End Date`,
    BuildingRentableArea = `Rentable Area - Building metric`,
    BuildingAppropriatedArea = `Appropriated Area - Building metric`,
    BuildingBillableArea = `Billable Area - Building metric`,
    LandArea = `Area - Land metric`,
    LandAppropriatedArea = `Appropriated Area - Land metric`,
    LandBillableArea = `Billable Area - Land metric`,
    ParkingTotalStalls = `Total Parking Stalls`,
    ParkingAppropriatedArea = `Appropriated Area - Parking`,
    ParkingBillableStalls = `Billable Parking Stalls`,
    BaseRent = `Base rent`,
    OM = `O&M`,
    Utilities,
    OMTotal = `O&M Total`,
    LLOM = `LL O&M`,
    Tax,
    Amort,
    PrkCost = `Prk Cost`,
    LLAdmin = `LL Admin`,
    OMAdmin = `O&M Admin`,
    UtilAdmin = `Util Admin`,
    TaxAdmin = `Tax Admin`,
    TotalAdmin = `Total Admin`,
    AdminFee = `Admin Fee`,
    AnnualCharge = `Annual Charge`
  ) |>
  filter(AgreementStatus == "Active") |>
  filter(AgreementType != "Non Prop") |>
  mutate(across(
    BuildingRentableArea:AnnualCharge,
    ~ as.numeric(gsub(
      "[^0-9.-]",
      "",
      .x
    ))
  ))

dbWriteTable(
  con,
  name = Id(SCHEMA_NAME, TABLE_NAME),
  value = CSR001,
  append = FALSE,
  overwrite = TRUE
)
