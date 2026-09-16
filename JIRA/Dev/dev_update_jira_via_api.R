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

no_scope_key <- keyring::key_get(
  service = "JIRA_API_FULL",
  username = username
)

my_api_key_scopes <- keyring::key_get(
  service = "JIRA_API_TEST_SCOPES",
  username = username
)

email <- "rpd.spbooking@gov.bc.ca"
api_key <- keyring::key_get(
  service = "JIRA_API",
  username = email
)

api_key_test <- keyring::key_get(
  service = "JIRA_API_TEST",
  username = email
)

# Encode token
token <- base64encode(charToRaw(paste0(email, ":", api_key)))
token_string <- paste("Basic", token)

# Encode test token
token_test <- base64encode(charToRaw(paste0(email, ":", api_key_test)))
token_test_string <- paste("Basic", token_test)

# Encode my test token scoped
token_test_scoped <- base64encode(charToRaw(paste0(email, ":", my_api_key_scopes)))
token_test_scoped_string <- paste("Basic", token_test_scoped)

# Encode my test token scoped
no_scope_token <- base64encode(charToRaw(paste0(email, ":", no_scope_key)))
no_scope_token_string <- paste("Basic", no_scope_token)

base_url <- "https://inf-dev.atlassian.net/rest/api/3/"
dashboard_id <- "PAR"

# Regular search/jql ####
query_url <- paste0(base_url, "search/jql")
expand_opts = c("names", "fields")
max_results = 100
start_time <- etl_window$jira_start_time

req <- request(query_url) |>
  req_headers_redacted(Authorization = token_string) |> # redacted by httr2 in printed output
  req_url_query(
    jql = I(
      # I wrapper skips auto-formatting of the extra "=" sign
      utils::URLencode(
        paste0(
          "project=",
          dashboard_id,
          " AND Updated >= \"",
          start_time,
          "\""
        ),
        repeated = TRUE
      )
    ),
    expand = expand_opts,
    maxResults = max_results,
    fields = "*all",
    nextPageToken = nextPageToken,
    .multi = "comma"
  ) |>
  apply_proxy_if_needed() |>
  req_perform(req)

resp <- req |> resp_body_json()

# Current System uses ####
# https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-search/#api-rest-api-3-search-jql-get
# Has permissions
# OAuth 2.0 scopes required:
# ClassicRECOMMENDED:read:jira-work
# Granular:read:issue-details:jira, read:audit-log:jira, read:avatar:jira, read:field-configuration:jira, read:issue-meta:jira

# Connect app scope required: READ

# Get Issue fields ####
# https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-fields/#api-group-issue-fields
query_url <- paste0(base_url, "field")
req <- request(query_url) |>
  req_headers_redacted(Authorization = token_string) |> # redacted by httr2 in printed output
  apply_proxy_if_needed() |>
  req_perform()

resp <- req |> resp_body_json()

fields <- resp |>
  tibble::enframe() |>
  select(value) |>
  tidyr::unnest_wider(value) |>
  tidyr::unnest_wider(clauseNames, names_sep = "_") |>
  tidyr::unnest_wider(schema, names_sep = "_")

# Looks like my options for Building Number are "customfield_10839" "customfield_10235" "customfield_11666" "customfield_10319"

# Get custom field option ####
# https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-custom-field-options/#api-group-issue-custom-field-options
query_url <- paste0(base_url, "customFieldOption")
req <- request(query_url) |>
  req_headers_redacted(Authorization = token_string) |> # redacted by httr2 in printed output
  req_url_path_append(
    "customfield_10839"
  ) |>
  apply_proxy_if_needed() |>
  req_perform()

resp <- req |> resp_body_json()

# Get context id for field ####
# https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-custom-field-contexts/#api-group-issue-custom-field-contexts

req <- request(base_url) |>
  req_headers_redacted(Authorization = no_scope_token_string) |> # redacted by httr2 in printed output
  req_url_path_append(
    "field",
    "customfield_10839",
    "context"
  ) |>
  apply_proxy_if_needed() |>
  req_perform()

resp <- req |> resp_body_json()

# Get field schemes ####
# https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-field-schemes/#api-rest-api-3-config-fieldschemes-get
query_url <- paste0(base_url, "config/fieldschemes")
req <- request(query_url) |>
  req_headers_redacted(Authorization = token_string) |> # redacted by httr2 in printed output
  apply_proxy_if_needed() |>
  req_perform()

# Get custom field option ####
# https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-custom-field-options/#api-rest-api-3-customfieldoption-id-get

# customfield_10070 is Ministry/BPS Organization
# customfield_10113 is Building Number
query_url <- paste0(base_url, "customFieldOption")

req <- request(query_url) |>
  req_headers_redacted(Authorization = token_string) |> # redacted by httr2 in printed output
  req_url_path_append(
    "customfield_10070"
  ) |>
  apply_proxy_if_needed() |>
  req_perform()
