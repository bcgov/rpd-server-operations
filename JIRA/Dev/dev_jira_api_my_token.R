source(here::here("utilities/utilities.R"))

# Load necessary packages
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

username <- "david.rattray@gov.bc.ca"

api_key <- keyring::key_get(
  service = "JIRA_API",
  username = username
)

# Encode token
token <- base64encode(charToRaw(paste0(username, ":", api_key)))
token_string <- paste("Basic", token)

base_url <- "https://inf-dev.atlassian.net/rest/api/3/"
etl_window <- get_etl_window()

# Get Fields ####
# https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-fields/#api-rest-api-3-field-get
query_url <- paste0(base_url, "field")

req <- request(query_url) |>
  req_headers_redacted(Authorization = token_string) |>
  apply_proxy_if_needed() |>
  req_perform()

resp <- req |> resp_body_json()

fields <- resp |>
  tibble::enframe() |>
  select(value) |>
  tidyr::unnest_wider(value) |>
  tidyr::unnest_wider(clauseNames, names_sep = "_") |>
  tidyr::unnest_wider(schema, names_sep = "_")

# Get custom field option ####
# https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-custom-field-options/#api-rest-api-3-customfieldoption-id-get
query_url <- paste0(base_url, "customFieldOption")

req <- request(query_url) |>
  req_headers_redacted(Authorization = token_string) |>
  req_url_path_append(
    "customfield_10010"
  ) |>
  apply_proxy_if_needed() |>
  req_perform()

# Get custom field contexts ####
# https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-custom-field-contexts/#api-group-issue-custom-field-contexts
query_url <- paste0(base_url, "field")

req <- request(query_url) |>
  req_headers_redacted(Authorization = token_string) |>
  req_url_path_append(
    "customfield_10010",
    "context"
  ) |>
  apply_proxy_if_needed() |>
  req_perform()

# Get custom field options (context) ####
# https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-custom-field-options/#api-rest-api-3-field-fieldid-context-contextid-option-get
