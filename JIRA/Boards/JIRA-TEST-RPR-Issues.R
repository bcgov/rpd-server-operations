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
DASHBOARD_ID <- "RPR"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = DASHBOARD_ID)
TEMP_TABLE <- paste0("#", DASHBOARD_ID, "Temp")
API_NAME <- "Jira"
SCRIPT_NAME <- "Jira_RPR"

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
    req_headers(Authorization = token_string) |>
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
    # Server logging and proxy steps
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
  nextPageToken <- resp
  "nextPageToken"
  1

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
      IssueType,
      Status,
      Created,
      Updated,
      Resolved,
      Duedate,
      DueDateflexibility,
      Timetofirstresponse,
      Timetoresolution,
      Assignee,
      # Audience1 = `Audience-1`, # Possible source of issues here, right now all NA
      Audience = `Audience-2`,
      Frequency = `Frequency-RPR`,
      Priority,
      ReportName = Reportname,
      Reporter,
      RequestParticipants = Requestparticipants, # evaluate that the code dropped in still works
      RequestType,
      Summary,
      # Team = `Team-2`,# Possible source of issues here, right now all NA
      Team = `Team-RPR`,
      Branch = `Branch-RPR`
    ) |>
    safe_hoist(IssueType, IssueType = "name", .remove = FALSE) |>
    safe_hoist(Status, Status = "name", .remove = FALSE) |>
    safe_hoist(Assignee, Assignee = "displayName", .remove = FALSE) |>
    safe_hoist(Priority, Priority = "name", .remove = FALSE) |>
    safe_hoist(
      DueDateflexibility,
      DueDateflexibility = "value",
      .remove = FALSE
    ) |>
    safe_hoist(
      Timetofirstresponse,
      Timetofirstresponse = list(
        "completedCycles",
        1L,
        "elapsedTime",
        "millis"
      ),
      .remove = FALSE
    ) |>
    safe_hoist(
      Timetoresolution,
      Timetoresolution = list("ongoingCycle", "elapsedTime", "millis"),
      .remove = FALSE
    ) |>
    safe_hoist(Audience, Audience = list("value"), .remove = FALSE) |>
    safe_hoist(Frequency, Frequency = list("value"), .remove = FALSE) |>
    safe_hoist(Reporter, Reporter = "displayName", .remove = FALSE) |>
    safe_hoist(
      RequestType,
      RequestType = list("requestType", "name"),
      .remove = FALSE
    ) |>
    safe_hoist(
      Branch,
      Branch = list("value"),
      .remove = FALSE
    ) |>
    # tidyr::unnest_wider(Requestparticipants, names_sep = "-") |>
    # tidyr::unnest_wider(starts_with("RequestParticipants"), names_sep = "-") |>
    # rowwise() |>
    # mutate(
    #   RequestParticipants = stringr::str_c(
    #     c_across(
    #       matches(
    #         "RequestParticipants-0-9+-displayName"
    #       )
    #     ),
    #     collapse = ";"
    #   )
    # ) |>
    ungroup() |>
    mutate(
      across(
        c(Created, Updated, Resolved),
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

Issues <- Issues |>
  mutate(
    across(
      where(
        is.character
      ),
      ~ na_if(.x, "")
    )
  ) |>
  mutate(RefreshDate = Sys.time(), .before = everything())

# Start database transaction ####
# dbRemoveTable(con, TARGET_TABLE)
if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- paste0(
    "CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    DASHBOARD_ID,
    " (
      RefreshDate          DATETIME2(3)    NOT NULL,
      IssueKey             NVARCHAR(10)    NOT NULL,
      IssueType            NVARCHAR(10)    NULL,
      Status               NVARCHAR(20)    NULL,
      Created              DATE            NULL,
      Updated              DATE            NULL,
      Resolved             DATE            NULL,
      Duedate              NVARCHAR(10)    NULL,
      DueDateflexibility   NVARCHAR(10)    NULL,
      Timetofirstresponse  BIGINT          NULL,
      Timetoresolution     FLOAT           NULL,
      Assignee             NVARCHAR(20)    NULL,
      Audience             NVARCHAR(15)    NULL,
      Frequency            NVARCHAR(20)    NULL,
      Priority             NVARCHAR(10)    NULL,
      ReportName           NVARCHAR(250)   NULL,
      Reporter             NVARCHAR(25)    NULL,
      RequestType          NVARCHAR(35)    NULL,
      Summary              NVARCHAR(500)   NULL,
      Team                 NVARCHAR(10)    NULL,
      Branch               NVARCHAR(50)    NULL,
      RequestParticipants  NVARCHAR(500)   NULL
    );"
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
          RefreshDate          DATETIME2(3)    NOT NULL,
          IssueKey             NVARCHAR(10)    NOT NULL,
          IssueType            NVARCHAR(10)    NULL,
          Status               NVARCHAR(20)    NULL,
          Created              DATE            NULL,
          Updated              DATE            NULL,
          Resolved             DATE            NULL,
          Duedate              NVARCHAR(10)    NULL,
          DueDateflexibility   NVARCHAR(10)    NULL,
          Timetofirstresponse  BIGINT          NULL,
          Timetoresolution     FLOAT           NULL,
          Assignee             NVARCHAR(20)    NULL,
          Audience             NVARCHAR(15)    NULL,
          Frequency            NVARCHAR(20)    NULL,
          Priority             NVARCHAR(10)    NULL,
          ReportName           NVARCHAR(250)   NULL,
          Reporter             NVARCHAR(25)    NULL,
          RequestType          NVARCHAR(35)    NULL,
          Summary              NVARCHAR(500)   NULL,
          Team                 NVARCHAR(10)    NULL,
          Branch               NVARCHAR(50)    NULL,
          RequestParticipants  NVARCHAR(500)   NULL
        );"
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
       tgt.RefreshDate         = src.RefreshDate,
       tgt.IssueType           = src.IssueType,
       tgt.Status              = src.Status,
       tgt.Created             = src.Created,
       tgt.Updated             = src.Updated,
       tgt.Resolved            = src.Resolved,
       tgt.Duedate             = src.Duedate,
       tgt.DueDateflexibility  = src.DueDateflexibility,
       tgt.Timetofirstresponse = src.Timetofirstresponse,
       tgt.Timetoresolution    = src.Timetoresolution,
       tgt.Assignee            = src.Assignee,
       tgt.Audience            = src.Audience,
       tgt.Frequency           = src.Frequency,
       tgt.Priority            = src.Priority,
       tgt.ReportName          = src.ReportName,
       tgt.Reporter            = src.Reporter,
       tgt.RequestType         = src.RequestType,
       tgt.Summary             = src.Summary,
       tgt.Team                = src.Team,
       tgt.Branch              = src.Branch,
       tgt.RequestParticipants = src.RequestParticipants
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
        "
         (
           RefreshDate,
           IssueKey,
           IssueType,
           Status,
           Created,
           Updated,
           Resolved,
           Duedate,
           DueDateflexibility,
           Timetofirstresponse,
           Timetoresolution,
           Assignee,
           Audience,
           Frequency,
           Priority,
           ReportName,
           Reporter,
           RequestType,
           Summary,
           Team,
           Branch,
           RequestParticipants
         )
         SELECT
           src.RefreshDate,
           src.IssueKey,
           src.IssueType,
           src.Status,
           src.Created,
           src.Updated,
           src.Resolved,
           src.Duedate,
           src.DueDateflexibility,
           src.Timetofirstresponse,
           src.Timetoresolution,
           src.Assignee,
           src.Audience,
           src.Frequency,
           src.Priority,
           src.ReportName,
           src.Reporter,
           src.RequestType,
           src.Summary,
           src.Team,
           src.Branch,
           src.RequestParticipants
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
