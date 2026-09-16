# Load helper functions
source(here::here("utilities/utilities.R"))

# Set options
options(scipen = 999)

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
library(stringr, quietly = TRUE, warn.conflicts = FALSE)
library(openxlsx2, quietly = TRUE, warn.conflicts = FALSE)
library(odbc, quietly = TRUE, warn.conflicts = FALSE)
library(DBI, quietly = TRUE, warn.conflicts = FALSE)

# CHEFS API Documentation ####
# https://submit.digital.gov.bc.ca/app/api/v1/docs#tag/Document-Templates/operation/readDocumentTemplates

# Setup API parameters ####
base_url = "https://submit.digital.gov.bc.ca/app/api/v1/"
formId = "a4f6a069-d176-43da-8a0c-605708820a0f"
username <- "a4f6a069-d176-43da-8a0c-605708820a0f"

api_key <- keyring::key_get(
  service = "CHEFS_API",
  username = username
)

# Get Form Metadata ####
req <- request(base_url) |>
  req_url_path_append(c("forms", formId)) |>
  req_auth_basic(username, api_key) |>
  req_headers(Accept = "application/json") |>
  req_perform()

resp <- req |> resp_body_json()

# Assume this is correct id for most recent form, have to validate
versionid <- resp$versions[[1]]$id

# Get Form Document Template ####
# req <- request(base_url) |>
#   req_url_path_append(c("forms", formId, "documentTemplates")) |>
#   req_auth_basic(username, api_key) |>
#   req_headers(Accept = "application/json") |>
#   req_perform()
#
# resp <- req |> resp_body_json()

# Get Form version ####
req <- request(base_url) |>
  req_url_path_append(c("forms", formId, "versions", versionid)) |>
  req_auth_basic(username, api_key) |>
  req_headers(Accept = "application/json") |>
  req_perform()

resp <- req |> resp_body_json()

formfieldid <- resp$schema$components[[2]]$components[[1]]$components[[1]]$id

# Get field value submissions ####
# https://submit.digital.gov.bc.ca/app/api/v1/forms/{formId}/versions/{formVersionId}/submissions/discover
req <- request(base_url) |>
  req_url_path_append(c("forms", formId, "documentTemplates")) |>
  req_auth_basic(username, api_key) |>
  req_headers(Accept = "application/json") |>
  req_perform()

# Main query
req <- request(base_url) |>
  req_url_path_append(c("forms", formId, "export")) |>
  req_url_query(
    format = "json",
    type = "submissions"
  ) |>
  req_auth_basic(username, api_key) |>
  req_headers(Accept = "application/json") |>
  req_perform()

resp <- req |> resp_body_json(check_type = FALSE)
