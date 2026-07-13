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

source(here("utilities/R/utilities.R"))

# Setup necessary variables
ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "CbreStaging"

# Connect to SQL database
con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = SQL_SERVER,
  database = DB_NAME,
  Trusted_Connection = "Yes"
)

# edp_update_ts is not useful ####
query <- dbSendQuery(con, "SELECT * FROM CbreStaging.es_fact_invoice")
EsFactInvoice <- dbFetch(query, n = -1)
dbClearResult(query)

range(EsFactInvoice$edp_update_ts)
table(EsFactInvoice$edp_update_ts)

range(EsFactInvoice$service_end_date)
table(EsFactInvoice$service_end_date)


# No filter permissions ####
base_url = "https://api.cbre.com:443/"
username = "kWNLCjcu05JmqrlSsmDtMoSreuUa"
keyring_service = "CBRE_API"

credentials <- keyring::key_get(
  service = keyring_service,
  username = username,
  keyring = NULL
)

token_b64 <- base64enc::base64encode(
  charToRaw(paste0(username, ":", credentials))
)

req <- httr2::request(base_url) |>
  httr2::req_url_path_append("token") |>
  httr2::req_headers(Authorization = paste("Basic", token_b64)) |>
  httr2::req_url_query(grant_type = "client_credentials") |>
  httr2::req_method("POST") |>
  apply_proxy_if_needed() |>
  httr2::req_perform()

bearer_token <- req |> resp_body_json() |> purrr::pluck("access_token")

endpoint_url <- "t/digitaltech_us_edp/vantageanalytics/prod/v1/data/"
edp_table <- "es_fact_invoice_vw"

req <- httr2::request(base_url) |>
  httr2::req_url_path_append(endpoint_url) |>
  httr2::req_auth_bearer_token(bearer_token) |>
  httr2::req_method("POST") |>
  httr2::req_body_json(
    list(
      entity = edp_table,
      page = 1,
      startTime = "2025-01-01T00:00:00Z",
      endTime = "2026-07-14T00:00:00Z",
      format = "json",
      filters = list(
        list(
          args = 20725956,
          attribute = "property_skey",
          operator = "="
        )
      )
    )
  ) |>
  # httr2::req_dry_run()
  httr2::req_perform()

data <- req |> httr2::resp_body_json() |> purrr::pluck('data')

col_names <- unlist(data$columns)

EsFactPropertySkey <- data$rows |>
  purrr::map(~ purrr::map(.x, ~ if (is.null(.x)) NA else .x)) |>
  purrr::map(~ setNames(as.list(.x), col_names)) |>
  purrr::map_dfr(tibble::as_tibble)


# dim_property ####
edp_table <- "dim_property_vw"

req <- httr2::request(base_url) |>
  httr2::req_url_path_append(endpoint_url) |>
  httr2::req_auth_bearer_token(bearer_token) |>
  httr2::req_method("POST") |>
  httr2::req_body_json(
    list(
      entity = edp_table,
      page = 1,
      startTime = "2025-01-01T00:00:00Z",
      endTime = "2026-07-14T00:00:00Z",
      format = "json",
      filters = list(
        list(
          args = 20725956,
          attribute = "property_skey",
          operator = "="
        )
      )
    )
  ) |>
  # httr2::req_dry_run()
  httr2::req_perform()

data <- req |> httr2::resp_body_json() |> purrr::pluck('data')

col_names <- unlist(data$columns)

DimPropertySkey <- data$rows |>
  purrr::map(~ purrr::map(.x, ~ if (is.null(.x)) NA else .x)) |>
  purrr::map(~ setNames(as.list(.x), col_names)) |>
  purrr::map_dfr(tibble::as_tibble)

# com_dim_property ####
edp_table <- "com_dim_property_vw"

req <- httr2::request(base_url) |>
  httr2::req_url_path_append(endpoint_url) |>
  httr2::req_auth_bearer_token(bearer_token) |>
  httr2::req_method("POST") |>
  httr2::req_body_json(
    list(
      entity = edp_table,
      page = 1,
      startTime = "2025-01-01T00:00:00Z",
      endTime = "2026-07-14T00:00:00Z",
      format = "json",
      filters = list(
        list(
          args = 20725956,
          attribute = "property_skey",
          operator = "="
        )
      )
    )
  ) |>
  httr2::req_dry_run()
httr2::req_perform()

data <- req |> httr2::resp_body_json() |> purrr::pluck('data')

col_names <- unlist(data$columns)

ComDimPropertySkey <- data$rows |>
  purrr::map(~ purrr::map(.x, ~ if (is.null(.x)) NA else .x)) |>
  purrr::map(~ setNames(as.list(.x), col_names)) |>
  purrr::map_dfr(tibble::as_tibble)
