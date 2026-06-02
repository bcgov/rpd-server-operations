# For server logging
# Begin timer
task_start <- Sys.time()
source("E:/Projects/citz-rpd-utilities/event_logger.R")
api_id <- "Jira" # for event_logger
project_id <- "GPOPR" # for event_logger

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

source("E:/Projects/citz-rpd-utilities/safe_hoist.R")

# desc <- paste0("Script started.")
# event_logger(api_id, project_id, "success", desc)

email <- "rpd.spbooking@gov.bc.ca"

api_key <- keyring::key_get(
  service = "JIRA_API",
  username = email,
  keyring = NULL
)

# Local Run
# source("C:/Projects/citz-rpd-utilities/safe_hoist.R")
# email <- "david.rattray@gov.bc.ca"
# api_key <- keyring::key_get(
#   service = "JIRA_API",
#   username = email,
#   keyring = NULL
# )

# Connect to SQL database
con <- odbc::dbConnect(
  drv = odbc(),
  driver = "SQL Server",
  server = "windfarm.idir.bcgov\\CA_TST",
  database = "BuildingIntelligence",
  Trusted_Connection = "Yes"
)

# Encode token
token <- base64encode(charToRaw(paste0(email, ":", api_key)))
token_string <- paste("Basic", token)

# Setup API parameters ####
query_url = "https://citz-rpd.atlassian.net/rest/api/3/search/jql"
# https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-search/#api-rest-api-3-search-jql-get
# https://developer.atlassian.com/changelog/#CHANGE-2046
dashboard_id = "GPOPR"
expand_opts = c("names", "fields")
max_results = 100
nextPageToken = NULL
progress = 0
round = 1

# Issues Loop ####
while (progress < 2) {
  req <- request(query_url) |>
    req_headers(Authorization = token_string) |>
    # configure project, max_results, and start_at
    req_url_query(
      jql = I(paste0("project=", dashboard_id)), # I wrapper skips auto-formatting of the extra "=" sign
      expand = expand_opts,
      maxResults = max_results,
      fields = "*all",
      # startAt = start_at, #deprecated for nextPageToken
      nextPageToken = nextPageToken,
      .multi = "comma" # control how vectors are appended, for expand_opts
    ) |>
    # Server logging and proxy steps
    req_proxy("142.34.229.249", 8080) |> # Use Server Proxy to connect
    req_error(
      is_error = function(resp) {
        lr <- resp_header(resp, "x-seraph-loginreason")
        bad_auth <- !is.null(lr) &&
          grepl("AUTHENTICATED_FAILED|AUTHENTICATION_DENIED", lr)
        empty_ok <- FALSE # we only care about bad_auth here
        bad_auth || empty_ok
      },

      body = function(resp) {
        paste0(
          "Auth Failure for ",
          api_id,
          " reason: ",
          resp_header(resp, "x-seraph-loginreason") %||% "UNKNOWN",
          " traceid: ",
          resp_header(resp, "atl-traceid") %||% "NA",
          " url: ",
          resp_url(resp)
        )
      }
    )

  # Perform request with error handling and structured logging
  resp <- tryCatch(
    req_perform(req) |> resp_body_json(),
    error = function(e) {
      # Compose a one-line description with context
      desc <- if (!is.null(e$body) && is.character(e$body)) {
        e$body
      } else {
        e$message
      }

      #     # Log the error to daily CSV
      event_logger(
        api = api_id,
        subset = project_id, # PAR / SBP / RBAS
        event_type = "error",
        description = desc
      )
      stop(e) # rethrow so task scheduler flags a failure (current monitoring is by Nagios)
    }
  )
  # For local run
  #   req_perform()
  #
  # resp <- req |> resp_body_json()

  # Used to update total_results in while loop
  nextPageToken <- resp["nextPageToken"][[1]]

  # total results isn't always accurate, check that response has issues
  if (length(resp$issues) == 0) {
    break
  }

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
      Created,
      Updated,
      Resolved,
      Assignee,
      GPOPackageApprover,
      GPOSubtype,
      Organization = `Ministry/BPSOrganization`,
      City = CityDropdown,
      RequestType,
      Requestparticipants,
      Priority,
      Duedate,
      Status,
      Summary
    ) |>
    safe_hoist(Organization, Organization = "value", .remove = FALSE) |>
    safe_hoist(
      GPOPackageApprover,
      GPOPackageApprover = "displayName",
      .remove = FALSE
    ) |>
    safe_hoist(Status, Status = "name", .remove = FALSE) |>
    safe_hoist(Assignee, Assignee = "displayName", .remove = FALSE) |>
    safe_hoist(Priority, Priority = "name", .remove = FALSE) |>
    safe_hoist(City, City = "value", .remove = FALSE) |>
    safe_hoist(
      RequestType,
      RequestType = list("requestType", "name"),
      .remove = FALSE
    ) |>
    tidyr::unnest_wider(Requestparticipants, names_sep = "-") |>
    tidyr::unnest_wider(starts_with("Requestparticipants"), names_sep = "-") |>
    rowwise() |>
    mutate(
      RequestParticipants = stringr::str_c(
        c_across(
          matches(
            "Requestparticipants-[0-9]+-displayName"
          )
        ),
        collapse = ";"
      )
    ) |>
    ungroup() |>
    select(
      IssueKey,
      Created,
      Updated,
      Resolved,
      Duedate,
      Priority,
      Status,
      Assignee,
      GPOPackageApprover,
      GPOSubtype,
      Organization,
      City,
      RequestType,
      RequestParticipants,
      Summary
    ) |>
    mutate(
      across(
        c(Created, Updated, Resolved, Duedate),
        ~ as.Date(.x, format = "%Y-%m-%d")
      )
    ) |>
    # in dev all NA values so listed as logical, doubt that will hold long term
    mutate(GPOSubtype = as.character(GPOSubtype))

  if (round == 1) {
    Issues <- issues
  } else {
    Issues <- full_join(Issues, issues)
  }

  round <- 2
}

# task_time <- as.numeric(difftime(Sys.time(), task_start, units = "secs"))
# desc <- paste0("Run Completed Up To SQL Insertion, ", nrow(Issues), " tickets. Runtime: ", task_time)
# event_logger(api_id, project_id, "success", desc)

# desc <- paste0("Writing to SQL, ", nrow(Issues), " tickets. ")
# event_logger(api_id, project_id, "success", desc)

# Control database transaction to ensure all steps done together or not at all
dbBegin(con)

# Begin error handling and rollback of transaction on failure
tryCatch(
  {
    # Create temp table to hold new data
    dbExecute(
      con,
      "
CREATE TABLE #IssuesTemp (
    IssueKey             NVARCHAR(MAX)    NOT NULL,
    Created              DATETIME2(3)     NULL,
    Updated              DATETIME2(3)     NULL,
    Resolved             DATETIME2(3)     NULL,
    Duedate              DATE             NULL,
    Priority             NVARCHAR(MAX)    NULL,
    Status               NVARCHAR(MAX)    NULL,
    Assignee             NVARCHAR(MAX)    NULL,
    GPOPackageApprover   NVARCHAR(MAX)    NULL,
    GPOSubtype           NVARCHAR(MAX)    NULL,
    Organization         NVARCHAR(MAX)    NULL,
    City                 NVARCHAR(MAX)    NULL,
    RequestType          NVARCHAR(MAX)    NULL,
    RequestParticipants  NVARCHAR(MAX)    NULL,
    Summary              NVARCHAR(MAX)    NULL
);
"
    )

    # Write into temp table the current Issues
    dbWriteTable(
      con,
      name = "#IssuesTemp",
      value = Issues,
      append = TRUE,
      overwrite = FALSE
    )

    # Update the GPOPR table with new data for existing rows
    n_updated <- dbExecute(
      con,
      "UPDATE tgt
SET
tgt.IssueKey            = src.IssueKey,
tgt.Created             = src.Created,
tgt.Updated             = src.Updated,
tgt.Resolved            = src.Resolved,
tgt.Duedate             = src.Duedate,
tgt.Priority            = src.Priority,
tgt.Status              = src.Status,
tgt.Assignee            = src.Assignee,
tgt.GPOPackageApprover  = src.GPOPackageApprover,
tgt.GPOSubtype          = src.GPOSubtype,
tgt.Organization        = src.Organization,
tgt.City                = src.City,
tgt.RequestType         = src.RequestType,
tgt.RequestParticipants = src.RequestParticipants,
tgt.Summary             = src.Summary
FROM Jira.GPOPR AS tgt
JOIN #IssuesTemp AS src
ON tgt.IssueKey = src.IssueKey;"
    )

    # Insert new rows into the GPOPR table
    n_inserted <- dbExecute(
      con,
      "
INSERT INTO Jira.GPOPR (
    IssueKey, Created, Updated, Resolved, Duedate, Priority, Status, Assignee,
    GPOPackageApprover, GPOSubtype, Organization, City, RequestType, RequestParticipants, Summary
)
SELECT
    src.IssueKey, src.Created, src.Updated, src.Resolved, src.Duedate, src.Priority, src.Status, src.Assignee,
    src.GPOPackageApprover, src.GPOSubtype, src.Organization, src.City, src.RequestType, src.RequestParticipants, src.Summary
FROM #IssuesTemp AS src
LEFT JOIN Jira.GPOPR AS tgt
    ON tgt.IssueKey = src.IssueKey
WHERE tgt.IssueKey IS NULL;
"
    )

    # Complete the transaction
    dbCommit(con)

    # rollback transaction on fail, completion of error handling
  },
  error = function(e) {
    dbRollback(con)
    stop(e)
  }
)


# Server Save
task_time <- as.numeric(difftime(Sys.time(), task_start, units = "secs"))
desc <- paste0(
  "Run Completed, ",
  nrow(Issues),
  " tickets. ",
  n_updated,
  " updated, and ",
  n_inserted,
  " inserted. Runtime: ",
  task_time
)
event_logger(api_id, project_id, "success", desc)

write.csv(Issues, "E:/Projects/PBI-Gateway/GPOPR_Issues.csv", row.names = FALSE)

# Local Save
# write.csv(Issues, here::here("GPOPR_output.csv"))
