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

custom_fields <- fields |>
  filter(schema_type == "option")

# Get custom field contexts ####
# https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-custom-field-contexts/#api-group-issue-custom-field-contexts
query_url <- paste0(base_url, "field")

req <- request(query_url) |>
  req_headers_redacted(Authorization = token_string) |>
  req_url_path_append(
    "context"
  ) |>
  apply_proxy_if_needed() |>
  req_perform()
# HTTP 403 Forbidden

field_contexts <- req |>
  resp_body_json() |>
  purrr::pluck("values") |>
  enframe() |>
  tidyr::unnest_wider(value, names_sep = "_")

# Get custom field options (context) ####
# https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-custom-field-options/#api-rest-api-3-field-fieldid-context-contextid-option-get
query_url <- paste0(base_url, "field")

req <- request(query_url) |>
  req_headers_redacted(Authorization = token_string) |>
  req_url_path_append(
    "customfield_10404",
    "context",
    "10490",
    "option"
  ) |>
  apply_proxy_if_needed() |>
  req_perform()

context_options <- req |>
  resp_body_json() |>
  purrr::pluck("values") |>
  enframe() |>
  tidyr::unnest_wider(value)

# Update custom field options (context) ####
# https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-custom-field-contexts/#api-rest-api-3-field-fieldid-context-contextid-put

# Map of option id -> new label
updates <- tibble::tribble(
  ~id     , ~value      ,
  "10254" , "Bungalow"  ,
  "10255" , "Campsite"  ,
  "10256" , "Apartment"
)

body <- list(
  options = purrr::pmap(updates, \(id, value) list(id = id, value = value))
)

query_url <- paste0(base_url, "field")

req <- request(query_url) |>
  req_headers_redacted(Authorization = token_string) |>
  req_method("PUT") |>
  req_url_path_append(
    "customfield_10404",
    "context",
    "10490",
    "option"
  ) |>
  req_body_json(body, auto_unbox = TRUE) |>
  req_error(is_error = \(r) FALSE) |> # so you can read Jira's message on failure
  apply_proxy_if_needed() |>
  req_perform()

resp_status(req)
resp_body_string(req)

# Check Permissions ####
perm_req <- request(paste0(base_url, "mypermissions")) |>
  req_headers_redacted(Authorization = token_string) |>
  req_url_query(permissions = "ADMINISTER") |>
  apply_proxy_if_needed() |>
  req_perform()

perm_req |>
  resp_body_json() |>
  purrr::pluck("permissions", "ADMINISTER", "havePermission")

resp <- request(base_url) |>
  req_url_path_append("field", "customfield_10083", "context") |>
  req_headers_redacted(Authorization = token_string) |>
  req_error(is_error = \(r) FALSE) |>
  apply_proxy_if_needed() |>
  req_perform()

resp_status(resp)
resp_body_string(resp)


# Get my permission groups ####
resp <- request(base_url) |>
  req_url_path_append("myself") |>
  req_url_query(expand = "groups") |>
  req_headers_redacted(Authorization = token_string) |>
  apply_proxy_if_needed() |>
  req_perform()

me <- resp |> resp_body_json()

# Basic identity
me[c("accountId", "displayName", "emailAddress", "active")]

# Groups (names only)
groups <- me$groups$items |> map_chr("name")
groups
