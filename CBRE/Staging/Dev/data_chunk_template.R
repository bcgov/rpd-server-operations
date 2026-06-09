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
source(here::here("utilities/R/utilities.R"))

CBRE_TABLE_NAME <- "fm_fact_workorder_vw"
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
  end_time = "2024-09-01T00:00:00Z"
)

chunk_6 <- call_cbre_api(
  CBRE_TABLE_NAME,
  start_time = "2024-09-01T00:00:00Z",
  end_time = "2025-01-01T00:00:00Z"
)

chunk_7 <- call_cbre_api(
  CBRE_TABLE_NAME,
  start_time = "2025-01-01T00:00:00Z",
  end_time = "2025-06-01T00:00:00Z"
)

chunk_8 <- call_cbre_api(
  CBRE_TABLE_NAME,
  start_time = "2025-06-01T00:00:00Z",
  end_time = "2026-01-01T00:00:00Z"
)

chunk_9 <- call_cbre_api(
  CBRE_TABLE_NAME,
  start_time = "2026-01-01T00:00:00Z",
  end_time = "2026-06-04T00:00:00Z"
)

raw_data <- rbind(
  chunk_1$data,
  chunk_2$data,
  chunk_3$data,
  chunk_4$data,
  chunk_5$data,
  chunk_6$data,
  chunk_7$data,
  chunk_8$data,
  chunk_9$data
)
