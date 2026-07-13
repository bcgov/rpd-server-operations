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
edp_table <- "kahua_cashflow"

req <- httr2::request(base_url) |>
  httr2::req_url_path_append(endpoint_url) |>
  httr2::req_auth_bearer_token(bearer_token) |>
  httr2::req_method("POST") |>
  httr2::req_body_json(list(
    entity = edp_table,
    page = 1,
    startTime = "2025-01-01T00:00:00Z",
    endTime = "2026-07-14T00:00:00Z",
    format = "json"
  )) |>
  httr2::req_perform()

resp <- req |> resp_body_json()

edp_table <- "archibus_bl"

req <- httr2::request(base_url) |>
  httr2::req_url_path_append(endpoint_url) |>
  httr2::req_auth_bearer_token(bearer_token) |>
  httr2::req_method("POST") |>
  httr2::req_body_json(list(
    entity = edp_table,
    page = 1,
    startTime = "2025-01-01T00:00:00Z",
    endTime = "2026-07-04T00:00:00Z",
    format = "json"
  )) |>
  # apply_proxy_if_needed() |>
  httr2::req_perform()

output <- req |> resp_body_json() |> purrr::pluck("data")

# Test Args for Envizi ####
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
      endTime = "2026-07-04T00:00:00Z",
      format = "json",
      filters = list(
        list(
          args = "20725956",
          attribute = "property_skey",
          operator = "="
        )
      )
    )
  ) |>
  httr2::req_dry_run()

resp <- req |>
  httr2::req_error(is_error = \(resp) FALSE) |>
  httr2::req_perform()

httr2::resp_status(resp)
httr2::resp_headers(resp)
httr2::resp_body_string(resp)

# Check dim_property_vw ####
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
  httr2::req_perform()

bearer_token <- req |> resp_body_json() |> purrr::pluck("access_token")

endpoint_url <- "t/digitaltech_us_edp/vantageanalytics/prod/v1/data/"
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
      format = "json"
    )
  ) |>
  httr2::req_perform()

resp <- req |>
  httr2:resp_body_json()

httr2::resp_status(resp)
httr2::resp_headers(resp)
httr2::resp_body_string(resp)
