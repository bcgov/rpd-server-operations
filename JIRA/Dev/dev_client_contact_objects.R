library(base64enc, quietly = TRUE, warn.conflicts = FALSE)
library(dplyr, quietly = TRUE, warn.conflicts = FALSE)
library(httr2, quietly = TRUE, warn.conflicts = FALSE)
library(jsonlite, quietly = TRUE, warn.conflicts = FALSE)
library(lubridate, quietly = TRUE, warn.conflicts = FALSE)
library(purrr, quietly = TRUE, warn.conflicts = FALSE)
library(tibble, quietly = TRUE, warn.conflicts = FALSE)
library(tidyr, quietly = TRUE, warn.conflicts = FALSE)

library(odbc, quietly = TRUE, warn.conflicts = FALSE)
library(DBI, quietly = TRUE, warn.conflicts = FALSE)

# Load helper functions
source(here::here("utilities/utilities.R"))

email <- "david.rattray@gov.bc.ca"
api_key <- keyring::key_get(
  service = "JIRA_API",
  username = email,
  keyring = NULL
)

token <- base64encode(charToRaw(paste0(email, ":", api_key)))
token_string <- paste("Basic", token)

# discover dev workspace ####
query_url <- "https://inf-dev.atlassian.net/rest/servicedeskapi/assets/workspace"

resp <- request(query_url) |>
  req_headers(
    Authorization = token_string,
    Accept = "application/json"
  ) |>
  apply_proxy_if_needed() |>
  req_error(is_error = \(resp) FALSE) |>
  req_perform()

if (resp_status(resp) == "200") {
  workspace_id <- resp |>
    resp_body_json() |>
    purrr::pluck("values") |>
    tibble::enframe() |>
    pull(value) |>
    unlist()
}

# discover object types ####
query_url <- paste0(
  "https://api.atlassian.com/jsm/assets/workspace/",
  workspace_id,
  "/v1/objectschema/138/objecttypes"
)

resp <- request(query_url) |>
  req_headers(
    Authorization = token_string,
    Accept = "application/json"
  ) |>
  apply_proxy_if_needed() |>
  req_perform()

resp |>
  resp_body_json()
