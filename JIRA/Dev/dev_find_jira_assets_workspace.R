# https://inf-dev.atlassian.net/jira/assets/object-schema/138?mode=object&typeId=228&objectId=10106

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

# Self check

query_url <- "https://inf-dev.atlassian.net/rest/api/3/myself"

resp <- request(query_url) |>
  req_headers(
    Authorization = token_string,
    Accept = "application/json"
  ) |>
  apply_proxy_if_needed() |>
  req_perform()

query_url <- "https://api.atlassian.com/jsm/assets/workspace"

req <- request(query_url) |>
  req_headers(
    Authorization = token_string
  ) |>
  apply_proxy_if_needed() |>
  req_perform()

query_url <- "https://inf-dev.atlassian.net/rest/insight/1.0/objectschema/list"

req <- request(query_url) |>
  req_headers(
    Authorization = token_string
  ) |>
  apply_proxy_if_needed() |>
  req_perform()

query_url <- "https://inf-dev.atlassian.net/rest/insight/1.0/object/10106"

req <- request(query_url) |>
  req_headers(
    Authorization = token_string
  ) |>
  apply_proxy_if_needed() |>
  req_perform()

query_url <- paste0(
  "https://inf-dev.atlassian.net/rest/servicedeskapi/assets/object/",
  "10106"
)

req <- request(query_url) |>
  req_headers(
    Authorization = token_string
  ) |>
  apply_proxy_if_needed() |>
  req_perform()

query_url <- paste0(
  "https://inf-dev.atlassian.net/rest/insight/1.0/object/",
  "10106"
)

req <- request(query_url) |>
  req_headers(
    Authorization = token_string
  ) |>
  apply_proxy_if_needed() |>
  req_perform()

query_url <- "https://inf-dev.atlassian.net/rest/api/3/serverInfo"

req <- request(query_url) |>
  req_headers(
    Authorization = token_string
  ) |>
  apply_proxy_if_needed() |>
  req_perform()

resp <- req |> resp_body_json()

query_url <- "https://inf-dev.atlassian.net/gateway/api/jsm/assets/workspace"

resp <- request(query_url) |>
  req_headers(
    Authorization = token_string,
    Accept = "application/json"
  ) |>
  apply_proxy_if_needed() |>
  req_error(is_error = \(resp) FALSE) |>
  req_perform()

resp_status(resp)

resp_body_string(resp)


query_url <- "https://inf-dev.atlassian.net/rest/servicedeskapi/assets/workspace"

resp <- request(query_url) |>
  req_headers(
    Authorization = token_string,
    Accept = "application/json"
  ) |>
  apply_proxy_if_needed() |>
  req_error(is_error = \(resp) FALSE) |>
  req_perform()

resp_status(resp)
resp_body_string(resp)

"workspaceId":"8784acb0-8896-4914-8612-b6a23f4d03c6"

workspace_id <- "8784acb0-8896-4914-8612-b6a23f4d03c6"

query_url <- paste0(
  "https://api.atlassian.com/jsm/assets/workspace/",
  workspace_id,
  "/v1/object/10106"
)

resp <- request(query_url) |>
  req_headers(
    Authorization = token_string,
    Accept = "application/json"
  ) |>
  apply_proxy_if_needed() |>
  req_error(is_error = \(resp) FALSE) |>
  req_perform()

resp_status(resp)
resp_body_string(resp)

query_url <- paste0(
  "https://api.atlassian.com/jsm/assets/workspace/",
  workspace_id,
  "/v1/objectschema/list"
)

resp <- request(query_url) |>
  req_headers(
    Authorization = token_string,
    Accept = "application/json"
  ) |>
  apply_proxy_if_needed() |>
  req_error(is_error = \(resp) FALSE) |>
  req_perform()

output <- resp |> resp_body_json()

query_url <- "https://api.atlassian.com/jsm/assets/workspace/8784acb0-8896-4914-8612-b6a23f4d03c6/v1/object/10106"

resp <- request(query_url) |>
  req_headers(
    Authorization = token_string,
    Accept = "application/json"
  ) |>
  apply_proxy_if_needed() |>
  req_error(is_error = \(resp) FALSE) |>
  req_perform()

resp_status(resp)
resp_body_string(resp)

output <- resp |>
  resp_body_json() |>
  purrr::pluck("values") |>
  tibble::enframe() |>
  unnest_wider(value, names_sep = "_")
