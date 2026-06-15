# Load necessary packages
library(dplyr, quietly = TRUE, warn.conflicts = FALSE)
library(httr2, quietly = TRUE, warn.conflicts = FALSE)
library(jsonlite, quietly = TRUE, warn.conflicts = FALSE)
library(lubridate, quietly = TRUE, warn.conflicts = FALSE)
library(purrr, quietly = TRUE, warn.conflicts = FALSE)
library(tibble, quietly = TRUE, warn.conflicts = FALSE)
library(tidyr, quietly = TRUE, warn.conflicts = FALSE)

base_url <- "https://submit.digital.gov.bc.ca/app/api/v1/"
formId <- "a3d13d43-3b29-4d3b-b950-c7a580790643"
username <- "a3d13d43-3b29-4d3b-b950-c7a580790643"

api_key <- keyring::key_get(
  service = "CHEFS_API",
  username = username,
  keyring = NULL
)

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

clean_data <- resp |>
  tibble::enframe() |>
  tidyr::unnest_wider("value")
