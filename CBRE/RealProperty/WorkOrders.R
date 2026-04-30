ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "RealProperty"
TABLE_NAME <- "WorkOrders"
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


woFile <- list.files(
  here::here("input"),
  pattern = "WO Detail List"
) |>
  sort(decreasing = TRUE)

for (ii in 1:length(woFile)) {
  col_name <- gsub(".csv", "", gsub("WO Detail List_", "", woFile[ii]))
  input <- read.csv(here(
    "input",
    woFile[ii]
  )) |>
    mutate(ImportChunk = col_name, .before = everything())
  if (ii == 1) {
    WorkOrder <- input
  } else {
    WorkOrder <- rbind(WorkOrder, input)
  }
}

WorkOrder_Table <- WorkOrder |>
  select(
    ImportChunk,
    Identifier = `Property.ID`,
    PriorityCode = `Priority.Code`,
    SLACompletionStatus = `SLA.Completion.Status`,
    WorkCategory = `Category.Code.Desc`,
    CreationDate = `Creation.Date`,
    CompletionDate = `Actual.Completed.Ts`
  ) |>
  # I think this is just to remove the timestamp portion of the date character string
  # maybe better ways to do this such as as.POSIXct()
  # mutate(CompletionDate = gsub("( [0-9]+:.*)", "", CompletionDate)) |>
  mutate(
    CompletionDate = as.POSIXct(CompletionDate, format = "%m/%d/%Y %I:%M:%S %p")
  ) |>
  mutate(CreationDate = as.POSIXct(CreationDate, format = "%m/%d/%Y")) |>
  mutate(FYCreation = fiscal_year_label(CreationDate)) |>
  # mutate(
  #   FYCreation = case_when(
  #     # Won't go that far but how am I going to update over increasing fiscal years?
  #     CreationDate |> timetk::between_time('2026-04-01', '2027-03-31') ~
  #       "FY2627",
  #     CreationDate |> timetk::between_time('2025-04-01', '2026-03-31') ~
  #       "FY2526",
  #     CreationDate |> timetk::between_time('2024-04-01', '2025-03-31') ~
  #       "FY2425",
  #     CreationDate |> timetk::between_time('2023-04-01', '2024-03-31') ~
  #       "FY2324",
  #     CreationDate |> timetk::between_time('2022-04-01', '2023-03-31') ~
  #       "FY2223",
  #     CreationDate |> timetk::between_time('2021-04-01', '2022-03-31') ~
  #       "FY2122",
  #     CreationDate |> timetk::between_time('2020-04-01', '2021-03-31') ~
  #       "FY2021",
  #   )
  # ) |>
  group_by(Identifier, FYCreation) |>
  summarise(
    WorkOrderCount = n(),
    .groups = "drop_last"
  ) |>
  pivot_wider(names_from = FYCreation, values_from = WorkOrderCount) |>
  mutate(across(starts_with("FY"), ~ replace_na(., 0))) |>
  pivot_longer(
    starts_with("FY"),
    names_to = "FYCreation",
    values_to = "WorkOrderCount"
  ) |>
  ungroup()

dbWriteTable(
  con,
  name = Id(SCHEMA_NAME, TABLE_NAME),
  value = WorkOrder_Table,
  append = FALSE,
  overwrite = TRUE
)
