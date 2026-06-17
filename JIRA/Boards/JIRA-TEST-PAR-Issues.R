# For server logging
# Begin timer
task_start <- Sys.time()

# Set necessary variables
ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "Jira"
DASHBOARD_ID <- "PAR"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = DASHBOARD_ID)
TEMP_TABLE <- paste0("#", DASHBOARD_ID, "Temp")
API_NAME <- "Jira"
SCRIPT_NAME <- "Jira_PAR"

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

# Setup API parameters ####
query_url = "https://citz-rpd.atlassian.net/rest/api/3/search/jql"
# https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-search/#api-rest-api-3-search-jql-get
# https://developer.atlassian.com/changelog/#CHANGE-2046
expand_opts = c("names", "fields")
max_results = 100
nextPageToken = NULL
progress = 0
round = 1

# Issues Loop ####
while (progress < 2) {
  req <- request(query_url) |>
    req_headers_redacted(Authorization = token_string) |> # redacted by httr2 in printed output
    # configure project, max_results, and start_at
    req_url_query(
      jql = I(paste0("project=", DASHBOARD_ID)), # I wrapper skips auto-formatting of the extra "=" sign
      expand = expand_opts,
      maxResults = max_results,
      fields = "*all",
      # startAt = start_at, #deprecated for nextPageToken
      nextPageToken = nextPageToken,
      .multi = "comma" # control how vectors are appended, for expand_opts
    ) |>
    apply_proxy_if_needed() |>
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
      # Log error to daily run file
      log_daily_etl_run(
        api_name = API_NAME,
        script_name = SCRIPT_NAME,
        table_name = DASHBOARD_ID,
        status = "FAILURE",
        message = substr(desc, 1, 500)
      )
      stop(e) # rethrow so task scheduler flags a failure (current monitoring is by Nagios)
    }
  )

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
      ProjectEffectiveDate,
      Created,
      Resolved,
      Updated,
      Organization = `Ministry/BPSOrganization`,
      RequestType,
      Status,
      StatusCategory,
      StatusCategoryChanged,
      Assignee,
      Reporter,
      Resolution,
      Summary
    ) |>
    safe_hoist(Organization, Organization = "value", .remove = FALSE) |>
    safe_hoist(StatusCategory, StatusCategory = "name", .remove = FALSE) |>
    safe_hoist(Status, Status = "name", .remove = FALSE) |>
    safe_hoist(Resolution, Resolution = "name", .remove = FALSE) |>
    safe_hoist(Assignee, Assignee = "displayName", .remove = FALSE) |>
    safe_hoist(Reporter, Reporter = "displayName", .remove = FALSE) |>
    safe_hoist(
      RequestType,
      RequestType = list("requestType", "name"),
      .remove = FALSE
    ) |>
    mutate(
      ProjectEffectiveDate = as.Date(ProjectEffectiveDate, format = "%Y-%m-%d")
    ) |>
    mutate(
      Created = as.POSIXct(
        Created,
        tz = "UTC",
        format = "%Y-%m-%dT%H:%M:%OS%z"
      )
    ) |>
    mutate(
      Resolved = as.POSIXct(
        Resolved,
        tz = "UTC",
        format = "%Y-%m-%dT%H:%M:%OS%z"
      )
    ) |>
    mutate(
      Updated = as.POSIXct(
        Updated,
        tz = "UTC",
        format = "%Y-%m-%dT%H:%M:%OS%z"
      )
    ) |>
    mutate(
      StatusCategoryChanged = as.POSIXct(
        StatusCategoryChanged,
        tz = "UTC",
        format = "%Y-%m-%dT%H:%M:%OS%z"
      )
    ) |>
    mutate(
      TimeToCompletion = case_when(
        is.na(Resolved) ~ NA,
        !is.na(Resolved) ~
          ((as.duration(interval(Created, Resolved))@.Data) / 60) / 60
      )
    )

  if (round == 1) {
    Issues <- issues
  } else {
    Issues <- full_join(Issues, issues)
  }

  round <- 2
}

Issues <- Issues |> mutate(RefreshDate = Sys.time(), .before = everything())

# Start database transaction ####
# dbRemoveTable(con, TARGET_TABLE)
if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- paste0(
    "CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    DASHBOARD_ID,
    " (
        RefreshDate            DATETIME2(3)     NOT NULL,
        IssueKey               NVARCHAR(20)     NOT NULL,
        ProjectEffectiveDate   DATETIME2(3)     NULL,
        Created                DATETIME2(3)     NULL,
        Resolved               DATETIME2(3)     NULL,
        Updated                DATETIME2(3)     NULL,
        Organization           NVARCHAR(100)    NULL,
        RequestType            NVARCHAR(100)    NULL,
        Status                 NVARCHAR(100)    NULL,
        StatusCategory         NVARCHAR(100)    NULL,
        StatusCategoryChanged  DATETIME2(3)     NULL,
        Assignee               NVARCHAR(100)    NULL,
        Reporter               NVARCHAR(100)    NULL,
        Resolution             NVARCHAR(100)    NULL,
        Summary                NVARCHAR(1000)   NULL,
        TimeToCompletion       DECIMAL(18,7)    NULL
    );
  "
  )

  dbExecute(con, sql)
}

etl_error <- NULL

# Control database transaction to ensure all steps done together or not at all
dbBegin(con)

# Begin error handling and rollback of transaction on failure
tryCatch(
  {
    if (dbExistsTable(con, TEMP_TABLE)) {
      dbRemoveTable(con, TEMP_TABLE)
    }

    # Create temp table to hold new data
    dbExecute(
      con,
      paste0(
        "CREATE TABLE ",
        TEMP_TABLE,
        " (
        RefreshDate            DATETIME2(3)     NOT NULL,
        IssueKey               NVARCHAR(20)     NOT NULL,
        ProjectEffectiveDate   DATETIME2(3)     NULL,
        Created                DATETIME2(3)     NULL,
        Resolved               DATETIME2(3)     NULL,
        Updated                DATETIME2(3)     NULL,
        Organization           NVARCHAR(100)    NULL,
        RequestType            NVARCHAR(100)    NULL,
        Status                 NVARCHAR(100)    NULL,
        StatusCategory         NVARCHAR(100)    NULL,
        StatusCategoryChanged  DATETIME2(3)     NULL,
        Assignee               NVARCHAR(100)    NULL,
        Reporter               NVARCHAR(100)    NULL,
        Resolution             NVARCHAR(100)    NULL,
        Summary                NVARCHAR(1000)   NULL,
        TimeToCompletion       DECIMAL(18,7)    NULL
    );
  "
      )
    )

    # Write into temp table the current Issues
    dbWriteTable(
      con,
      name = TEMP_TABLE,
      value = Issues,
      append = TRUE,
      overwrite = FALSE
    )

    # -- Guard: catch duplicate keys in source data before touching target --
    dup_count <- dbGetQuery(
      con,
      paste0(
        "SELECT COUNT(*) AS n
         FROM (
           SELECT IssueKey
           FROM ",
        TEMP_TABLE,
        "
           GROUP BY IssueKey
           HAVING COUNT(*) > 1
         ) dupes;"
      )
    )$n

    if (dup_count > 0) {
      stop(paste0(
        "Duplicate IssueKey values detected in source data (",
        dup_count,
        " keys affected). Rolling back."
      ))
    }

    # Update the GPOPR table with new data for existing rows
    n_updated <- dbExecute(
      con,
      paste0(
        "
      UPDATE tgt
      SET
        tgt.RefreshDate           = src.RefreshDate,
        tgt.ProjectEffectiveDate  = src.ProjectEffectiveDate,
        tgt.Created               = src.Created,
        tgt.Resolved              = src.Resolved,
        tgt.Updated               = src.Updated,
        tgt.Organization          = src.Organization,
        tgt.RequestType           = src.RequestType,
        tgt.Status                = src.Status,
        tgt.StatusCategory        = src.StatusCategory,
        tgt.StatusCategoryChanged = src.StatusCategoryChanged,
        tgt.Assignee              = src.Assignee,
        tgt.Reporter              = src.Reporter,
        tgt.Resolution            = src.Resolution,
        tgt.Summary               = src.Summary,
        tgt.TimeToCompletion      = src.TimeToCompletion
      FROM ",
        SCHEMA_NAME,
        ".",
        DASHBOARD_ID,
        " tgt
        INNER JOIN ",
        TEMP_TABLE,
        " src
        ON tgt.IssueKey = src.IssueKey;"
      )
    )

    # Insert new rows into the GPOPR table
    n_inserted <- dbExecute(
      con,
      paste0(
        "INSERT INTO ",
        SCHEMA_NAME,
        ".",
        DASHBOARD_ID,
        " (
        RefreshDate,
        IssueKey,
        ProjectEffectiveDate,
        Created,
        Resolved,
        Updated,
        Organization,
        RequestType,
        Status,
        StatusCategory,
        StatusCategoryChanged,
        Assignee,
        Reporter,
        Resolution,
        Summary,
        TimeToCompletion
      )
      SELECT
        src.RefreshDate,
        src.IssueKey,
        src.ProjectEffectiveDate,
        src.Created,
        src.Resolved,
        src.Updated,
        src.Organization,
        src.RequestType,
        src.Status,
        src.StatusCategory,
        src.StatusCategoryChanged,
        src.Assignee,
        src.Reporter,
        src.Resolution,
        src.Summary,
        src.TimeToCompletion
    FROM ",
        TEMP_TABLE,
        " src
        LEFT JOIN ",
        SCHEMA_NAME,
        ".",
        DASHBOARD_ID,
        " tgt
        ON tgt.IssueKey = src.IssueKey
        WHERE tgt.IssueKey IS NULL;"
      )
    )

    # Complete the transaction
    dbCommit(con)

    # Hoist counts to outer scope for logging
    n_updated <<- n_updated
    n_inserted <<- n_inserted

    cat("ETL complete — updated:", n_updated, "| inserted:", n_inserted, "\n")
    # rollback transaction on fail, completion of error handling
  },
  error = function(e) {
    dbRollback(con)
    etl_error <<- e
  }
)

task_end <- Sys.time()
task_duration <- interval(task_start, task_end) / dseconds()

if (is.null(etl_error)) {
  log_daily_etl_run(
    api_name = API_NAME,
    script_name = SCRIPT_NAME,
    table_name = DASHBOARD_ID,
    duration = task_duration,
    status = "SUCCESS",
    n_inserted = n_inserted,
    n_updated = n_updated,
    n_deleted = NA,
    message = "ETL completed successfully"
  )
} else {
  log_daily_etl_run(
    api_name = API_NAME,
    script_name = SCRIPT_NAME,
    table_name = DASHBOARD_ID,
    status = "FAILURE",
    message = substr(etl_error$message, 1, 500)
  )
  stop(etl_error)
}

write.csv(Issues, "E:/Projects/PBI-Gateway/PAR_Issues.csv", row.names = FALSE)
