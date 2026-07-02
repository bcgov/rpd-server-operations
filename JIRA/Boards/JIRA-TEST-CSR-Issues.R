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
DASHBOARD_ID <- "CSR"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = DASHBOARD_ID)
TEMP_TABLE <- paste0("#", DASHBOARD_ID, "Temp")
API_NAME <- "Jira"
SCRIPT_NAME <- "Jira_CSR"

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
      # jql = I(paste0("project=", DASHBOARD_ID)), # I wrapper skips auto-formatting of the extra "=" sign
      jql = I(
        # I wrapper skips auto-formatting of the extra "=" sign
        utils::URLencode(
          paste0(
            "project=",
            DASHBOARD_ID,
            " AND updated >= \"",
            etl_window$jira_start_time,
            "\""
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

  if (length(resp$issues == 0)) {
    # API succeeded, nothing to load
    no_data_msg <- paste0(
      "No data returned from API for window ",
      etl_window$start_time,
      " to ",
      etl_window$end_time
    )
    cat(no_data_msg, "— nothing to load. Exiting gracefully.\n")
    log_daily_etl_run(
      api_name = API_NAME,
      script_name = SCRIPT_NAME,
      table_name = TABLE_NAME,
      duration = as.numeric(difftime(Sys.time(), task_start, units = "secs")),
      status = "NO_DATA",
      message = no_data_msg
    )
    cond <- structure(
      class = c("no_data_condition", "condition"),
      list(message = no_data_msg)
    )
    stop(cond)
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
    safe_hoist(Assignee, Assignee = "displayName", .remove = FALSE) |>
    safe_hoist(CSM, CSM = "displayName", .remove = FALSE) |>
    safe_hoist(CSRIssueSubtype, CSRIssueSubtype = "value", .remove = FALSE) |>
    safe_hoist(Organization, Organization = "value", .remove = FALSE) |>
    safe_hoist(Priority, Priority = "name", .remove = FALSE) |>
    safe_hoist(ResponsibleGroup, ResponsibleGroup = "value", .remove = FALSE) |>
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

Issues <- Issues |>
  mutate(
    Assignee = tidyr::replace_na(Assignee, "Unassigned"),
    CSM = tidyr::replace_na(CSM, "Unassigned")
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
      RefreshDate          DATETIME2(3)  NOT NULL,
      IssueKey             NVARCHAR(100) NOT NULL,
      Status               NVARCHAR(100) NULL,
      Created              DATE          NULL,
      Updated              DATE          NULL,
      Resolved             DATE          NULL,
      ProjectEffectiveDate DATE          NULL,
      Assignee             NVARCHAR(100) NULL,
      CSM                  NVARCHAR(100) NULL,
      CSRIssueSubtype      NVARCHAR(100) NULL,
      Organization         NVARCHAR(100) NULL,
      PIN                  NVARCHAR(100) NULL,
      Priority             NVARCHAR(100) NULL,
      ResponsibleGroup     NVARCHAR(100) NULL,
      Workstream           NVARCHAR(100) NULL
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
          RefreshDate          DATETIME2(3)  NOT NULL,
          IssueKey             NVARCHAR(100) NOT NULL,
          Status               NVARCHAR(100) NULL,
          Created              DATE          NULL,
          Updated              DATE          NULL,
          Resolved             DATE          NULL,
          ProjectEffectiveDate DATE          NULL,
          Assignee             NVARCHAR(100) NULL,
          CSM                  NVARCHAR(100) NULL,
          CSRIssueSubtype      NVARCHAR(100) NULL,
          Organization         NVARCHAR(100) NULL,
          PIN                  NVARCHAR(100) NULL,
          Priority             NVARCHAR(100) NULL,
          ResponsibleGroup     NVARCHAR(100) NULL,
          Workstream           NVARCHAR(100) NULL
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
        tgt.[RefreshDate]          = src.[RefreshDate],
        tgt.[Status]               = src.[Status],
        tgt.[Created]              = src.[Created],
        tgt.[Updated]              = src.[Updated],
        tgt.[Resolved]             = src.[Resolved],
        tgt.[ProjectEffectiveDate] = src.[ProjectEffectiveDate],
        tgt.[Assignee]             = src.[Assignee],
        tgt.[CSM]                  = src.[CSM],
        tgt.[CSRIssueSubtype]      = src.[CSRIssueSubtype],
        tgt.[Organization]         = src.[Organization],
        tgt.[PIN]                  = src.[PIN],
        tgt.[Priority]             = src.[Priority],
        tgt.[ResponsibleGroup]     = src.[ResponsibleGroup],
        tgt.[Workstream]           = src.[Workstream]
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
          [RefreshDate],
          [IssueKey],
          [Status],
          [Created],
          [Updated],
          [Resolved],
          [ProjectEffectiveDate],
          [Assignee],
          [CSM],
          [CSRIssueSubtype],
          [Organization],
          [PIN],
          [Priority],
          [ResponsibleGroup],
          [Workstream]
        )
        SELECT
          src.[RefreshDate],
          src.[IssueKey],
          src.[Status],
          src.[Created],
          src.[Updated],
          src.[Resolved],
          src.[ProjectEffectiveDate],
          src.[Assignee],
          src.[CSM],
          src.[CSRIssueSubtype],
          src.[Organization],
          src.[PIN],
          src.[Priority],
          src.[ResponsibleGroup],
          src.[Workstream]
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
