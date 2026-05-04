ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "RealProperty"
TABLE_NAME <- "ProjectCosts"
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

projFile <- list.files(
  here::here("input"),
  pattern = "ProjectList"
) |>
  sort(decreasing = TRUE)

ProjectCosts <- read_xlsx(
  here("input/", projFile[1])
) |>
  rename_with(~ gsub(" ", "", .), everything()) |>
  rename_with(~ gsub("/[\\n]?", "", perl = TRUE, .), everything()) |>
  # rename_with(~ gsub("[/()#%]", "", .), everything()) |>
  rename_with(~ gsub("[/#%]", "", .), everything()) |>
  rename(Identifier = BuildingID) |>
  group_by(Identifier) |>
  summarise(
    Region = first(Region),
    N_Projects = n(),
    Total_Paid = sum(Paid, na.rm = TRUE)
  ) |>
  filter(
    !is.na(Identifier) & !Identifier %in% c("9999100", "9999200", "9999300")
  ) |>
  left_join(FacilityDetail, by = join_by(Identifier)) |>
  filter(!is.na(Address)) |>
  select(
    Identifier,
    Name,
    Address,
    City,
    Region,
    N_Projects,
    Total_Paid,
    BuildingRentableArea,
    PropertyArea
  ) |>
  mutate(Total_Paid = round(Total_Paid, digits = 2)) |>
  mutate(
    CostPerSqM = case_when(
      startsWith(Identifier, "B") & BuildingRentableArea > 0 ~ round(
        Total_Paid /
          BuildingRentableArea,
        digits = 2
      ),
      .default = NA
    ),
    CostPerHA = case_when(
      startsWith(Identifier, "N") & PropertyArea > 0 ~ round(
        Total_Paid /
          PropertyArea,
        digits = 2
      ),
      .default = NA
    )
  )

dbWriteTable(
  con,
  name = Id(SCHEMA_NAME, TABLE_NAME),
  value = ProjectCosts,
  append = FALSE,
  overwrite = TRUE
)
