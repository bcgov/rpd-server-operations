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

library(odbc, quietly = TRUE, warn.conflicts = FALSE)
library(DBI, quietly = TRUE, warn.conflicts = FALSE)

# Load helper functions
source(here::here("./utilities/R/api_helpers.R"))
source(here::here("./utilities/R/event_logger.R"))
source(here::here("./utilities/R/sql_helper_functions.R"))

ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "CbreStaging"
TABLE_NAME <- "pjm_dim_budget"
CBRE_TABLE_NAME <- "pjm_dim_project_vw"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")

# Query API
chunk_1 <- call_cbre_api(
  CBRE_TABLE_NAME,
  start_time = "2020-01-01T00:00:00Z",
  end_time = "2023-01-01T00:00:00Z"
)

chunk_2 <- call_cbre_api(
  CBRE_TABLE_NAME,
  start_time = "2023-01-01T00:00:00Z",
  end_time = "2023-06-01T00:00:00Z"
)

chunk_3 <- call_cbre_api(
  CBRE_TABLE_NAME,
  start_time = "2023-06-01T00:00:00Z",
  end_time = "2024-01-01T00:00:00Z"
)

chunk_4 <- call_cbre_api(
  CBRE_TABLE_NAME,
  start_time = "2024-01-01T00:00:00Z",
  end_time = "2024-06-01T00:00:00Z"
)

chunk_5 <- call_cbre_api(
  CBRE_TABLE_NAME,
  start_time = "2024-06-01T00:00:00Z",
  end_time = "2025-01-01T00:00:00Z"
)

chunk_6 <- call_cbre_api(
  CBRE_TABLE_NAME,
  start_time = "2025-01-01T00:00:00Z",
  end_time = "2025-06-01T00:00:00Z"
)

chunk_7 <- call_cbre_api(
  CBRE_TABLE_NAME,
  start_time = "2025-06-01T00:00:00Z",
  end_time = "2026-01-01T00:00:00Z"
)

chunk_8 <- call_cbre_api(
  CBRE_TABLE_NAME,
  start_time = "2026-01-01T00:00:00Z",
  end_time = "2026-06-01T00:00:00Z"
)

raw_data <- rbind(
  chunk_1$data,
  chunk_2$data,
  chunk_3$data,
  chunk_4$data,
  chunk_5$data,
  chunk_6$data,
  chunk_7$data,
  chunk_8$data
)
