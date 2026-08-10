# Load helper functions
source(here::here("utilities/R/utilities.R"))

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

DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "Jira"

# Set necessary variables ####
##############################
DASHBOARD_ID <- "CSR"
##############################

TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = DASHBOARD_ID)
TEMP_TABLE <- paste0("#", DASHBOARD_ID, "Temp")
API_NAME <- SCHEMA_NAME
SCRIPT_NAME <- paste0(API_NAME, "_", DASHBOARD_ID)

ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}

# Connect to SQL database
con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = SQL_SERVER,
  database = DB_NAME,
  Trusted_Connection = "Yes"
)

email <- "rpd.spbooking@gov.bc.ca"
api_key <- keyring::key_get(
  service = "JIRA_API",
  username = email,
  keyring = NULL
)

# Encode token
token <- base64encode(charToRaw(paste0(email, ":", api_key)))
token_string <- paste("Basic", token)

base_url <- "https://citz-inf.atlassian.net/rest/api/3/"

req <- request(base_url) |>
  req_headers(
    Authorization = token_string
  ) |>
  req_url_path_append("dashboard") |>
  apply_proxy_if_needed() |>
  req_perform()

query_url <- paste0(base_url, "search/jql")

# Setup API parameters ####
expand_opts = c("names", "fields")
max_results = 100
nextPageToken = NULL
progress = 0
round = 1

# Issues Loop ####
while (progress < 2) {
  req <- request(query_url) |>
    req_headers_redacted(Authorization = token_string) |>
    # configure project, max_results, and start_at
    req_url_query(
      jql = I(
        # I wrapper skips auto-formatting of the extra "=" sign
        utils::URLencode(
          paste0(
            "project=",
            DASHBOARD_ID
          ),
          repeated = TRUE
        )
      ),
      expand = expand_opts,
      maxResults = max_results,
      fields = "*all",
      # startAt = start_at, #deprecated for nextPageToken
      nextPageToken = nextPageToken,
      .multi = "comma" # control how vectors are appended, for expand_opts
    ) |>
    # Server logging and proxy steps
    apply_proxy_if_needed()

  # Perform request with error handling and structured logging
  resp <- req_perform(req) |> resp_body_json()

  # Used to update total_results in while loop
  nextPageToken <- resp["nextPageToken"][[1]]

  if (is.null(nextPageToken)) {
    progress <- 2
  }

  names <- resp |>
    purrr::pluck("names") |>
    tibble::enframe() |>
    safe_hoist(value, Value = 1L) |>
    group_by(Value) |>
    mutate(row_name = row_number(), row_count = n()) |>
    mutate(
      Value = case_when(
        row_count > 1 ~ paste0(Value, "-", row_name),
        .default = Value
      )
    ) |>
    select(-c(row_name, row_count)) |>
    tibble::deframe()

  issues <- resp |>
    purrr::pluck("issues") |>
    tibble::enframe() |>
    tidyr::unnest_wider(value) |>
    tidyr::unnest_wider(fields) |>
    plyr::rename(names) |>
    # select_if(~ !all(is.na(.))) |>
    rename_with(~ gsub(" ", "", .)) |>
    select(
      IssueKey = key,
      Status,
      RequestType,
      Created,
      Updated,
      Resolved,
      ProjectEffectiveDate,
      Assignee,
      CSM,
      CSRIssueSubtype,
      Organization = `Ministry/BPSOrganization`,
      PIN = `PIN(ARENumber)`,
      Priority,
      ResponsibleGroup,
      Workstream
    ) |>
    safe_hoist(Status, Status = "name", .remove = FALSE) |>
    safe_hoist(
      RequestType,
      RequestType = list("requestType", "name"),
      .remove = FALSE
    ) |>
    safe_hoist(Assignee, Assignee = "displayName", .remove = FALSE) |>
    safe_hoist(CSM, CSM = "displayName", .remove = FALSE) |>
    safe_hoist(
      CSRIssueSubtype,
      CSRIssueSubtype = "value",
      .remove = FALSE
    ) |>
    safe_hoist(Organization, Organization = "value", .remove = FALSE) |>
    safe_hoist(Priority, Priority = "name", .remove = FALSE) |>
    safe_hoist(
      ResponsibleGroup,
      ResponsibleGroup = "value",
      .remove = FALSE
    ) |>
    safe_hoist(Workstream, Workstream = "value", .remove = FALSE) |>
    mutate(
      across(
        c(Created, Updated, Resolved, ProjectEffectiveDate),
        ~ as.Date(.x, format = "%Y-%m-%d")
      )
    )

  if (round == 1) {
    Issues <- issues
  } else {
    Issues <- full_join(Issues, issues)
  }

  round <- 2
}

# CSR Post Processing ####
if (DASHBOARD_ID == "CSR") {
  Issues <- Issues |>
    mutate(
      Assignee = tidyr::replace_na(Assignee, "Unassigned"),
      CSM = tidyr::replace_na(CSM, "Unassigned")
    ) |>
    mutate(RefreshDate = Sys.time(), .before = everything())
}
