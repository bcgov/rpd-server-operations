ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "CbreStaging"
TABLE_NAME <- "dim_project_activity"
CBRE_TABLE_NAME <- "dim_project_activity_vw"

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
source(here::here("./utilities/R/cbre_api_function.R"))
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

target_table <- Id(schema = SCHEMA_NAME, table = TABLE_NAME)
temp_table <- paste0("#", TABLE_NAME, "Temp")

# edp_tables <- list(
#   "dim_project_activity_vw",
#   "dim_project_role_vw",
#   "fact_budget_vw",
#   "fact_invoice_vw",
#   "fact_project_activity_vw"
# )
# ---- submit all exports ----
# jobs <- lapply(edp_tables, function(tbl) {
#   submit_edp_export(
#     edp_table = tbl
#   )
# })
results <- lapply(jobs, function(job) {
  retrieve_edp_export(job$file_id)
})

file_id <- jobs[[1]]$file_id

jobs[[1]]$edp_table
output <- retrieve_edp_export(file_id = file_id)

retrieve_edp_export <- function(
  file_id,
  base_url = "https://api.cbre.com:443/",
  file_endpoint = "/t/digitaltech_us_edp/vantageanalytics/prod/v1/file/",
  username = "kWNLCjcu05JmqrlSsmDtMoSreuUa",
  keyring_service = "CBRE_API",
  poll_interval = 60,
  timeout = 1200
) {
  credentials <- keyring::key_get(
    service = keyring_service,
    username = username
  )

  start_time <- Sys.time()

  # ---- wait until first file is available ----
  repeat {
    bearer_token <- get_bearer_token(base_url, username, credentials)

    probe <- try(
      request(base_url) |>
        httr2::req_url_path_append(file_endpoint) |>
        httr2::req_url_path_append(file_id) |>
        httr2::req_headers("file-num" = "1") |>
        httr2::req_auth_bearer_token(bearer_token) |>
        httr2::req_perform(),
      silent = TRUE
    )

    if (!inherits(probe, "try-error") && resp_status(probe) == 200) {
      print("successful response")
      break
    }

    if (difftime(Sys.time(), start_time, units = "secs") > timeout) {
      stop("Timed out waiting for EDP export to be ready")
    }
    print("Going for another loop")
    Sys.sleep(poll_interval)
  }

  # ---- retrieve all files ----
  all_data <- NULL
  fileNumber <- "1"
  lastFile <- "false"

  while (lastFile != "true") {
    bearer_token <- get_bearer_token(base_url, username, credentials)

    resp <- request(base_url) |>
      httr2::req_url_path_append(file_endpoint) |>
      httr2::req_url_path_append(file_id) |>
      httr2::req_headers("file-num" = fileNumber) |>
      httr2::req_auth_bearer_token(bearer_token) |>
      httr2::req_perform()

    chunk <- resp |>
      resp_body_string() |>
      readr::read_csv(col_types = readr::cols(.default = "c"))

    all_data <- dplyr::bind_rows(all_data, chunk)

    fileNumber <- resp$headers$`next-file-num`
    lastFile <- resp$headers$`last-file`
  }

  all_data
}
