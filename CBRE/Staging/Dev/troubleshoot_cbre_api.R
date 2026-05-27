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

source(here::here("./utilities/R/cbre_api_function.R"))

base_url = "https://api.cbre.com:443/"
username = "kWNLCjcu05JmqrlSsmDtMoSreuUa"
keyring_service = "CBRE_API"

credentials <- keyring::key_get(
  service = keyring_service,
  username = username,
  keyring = NULL)

token_b64 <- base64enc::base64encode(
  charToRaw(paste0(username, ":", credentials))
)

Sys.getenv("ETL_ENV")

req <- httr2::request(base_url) |>
  httr2::req_url_path_append("token") |>
  httr2::req_headers(Authorization = paste("Basic", token_b64)) |>
  httr2::req_url_query(grant_type = "client_credentials") |>
  httr2::req_method("POST") |>
  apply_proxy_if_needed() |>
  httr2::req_perform(verbosity = 3)
