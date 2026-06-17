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
DASHBOARD_ID <- "SBP"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = DASHBOARD_ID)
TEMP_TABLE <- paste0("#", DASHBOARD_ID, "Temp")
API_NAME <- "Jira"
SCRIPT_NAME <- "Jira_SBP"

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
expand_opts = c("changelog", "names", "fields")
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
      # Compose a one-line description with context from req_error
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
    # Parent column is sometimes missing as sparsely populated
    mutate(
      Parent = if ("Parent" %in% names(pick(everything()))) Parent else NA
    ) |>
    # Select fields of interest
    select(
      IssueKey = key,
      IssueType = `Issue Type`,
      Address,
      Assignee,
      Created,
      RequestedDueDate = `Requested Due Date`,
      SpaceBookingAdmin = `Name of Space Booking Admin`,
      NumberOfSpaces = `Number of Spaces to Onboard`,
      FloorPlan = `Do you have a floor plan?`,
      FurniturePlan = `Do you have a furniture plan?`,
      LastUpdatedStatus = `Last Updated Status`,
      Department = `Department-1`,
      DueDate = `Due date`,
      Organization = `Ministry/BPS Organization`,
      Priority,
      Reporter,
      RequestParticipants = `Request participants`,
      RequestType = `Request Type`,
      Resolved,
      Status,
      Summary,
      Updated,
      Parent,
      changelog
    ) |>
    safe_hoist(IssueType, IssueType = "name", .remove = FALSE) |>
    safe_hoist(
      Address,
      Address = list("content", 1L, "content", 1L, "text"),
      .remove = FALSE
    ) |>
    safe_hoist(Assignee, Assignee = "displayName", .remove = FALSE) |>
    safe_hoist(RequestedDueDate, RequestedDueDate = "value", .remove = FALSE) |>
    safe_hoist(FloorPlan, FloorPlan = "value", .remove = FALSE) |>
    safe_hoist(FurniturePlan, FurniturePlan = "value", .remove = FALSE) |>
    safe_hoist(Organization, Organization = "value", .remove = FALSE) |>
    safe_hoist(Priority, Priority = "name", .remove = FALSE) |>
    safe_hoist(Reporter, Reporter = "displayName", .remove = FALSE) |>
    safe_hoist_all(
      RequestParticipants,
      path = list("displayName"),
      .remove = FALSE
    ) |>
    mutate(
      RequestParticipants = RequestParticipants_displayName,
      .keep = "unused"
    ) |>
    safe_hoist(
      RequestType,
      RequestType = list("requestType", "name"),
      .remove = FALSE
    ) |>
    safe_hoist(Status, Status = "name", .remove = FALSE) |>
    safe_hoist(Parent, Parent = "key", .remove = FALSE)

  if (round == 1) {
    Issues <- issues
  } else {
    Issues <- full_join(Issues, issues)
  }

  round <- 2
}

# Calculate time spent in status for each ticket.
# timestamp is used for tickets that have only been open, calc time from creation to sys.time for total elapsed time.
timestamp <- format(Sys.time(), format = "%Y-%m-%dT%H:%M:%OS3%z")

StatusChange <- Issues |>
  select(IssueKey, Status, TicketCreated = Created, changelog) |>
  safe_hoist(changelog, histories = list("histories"), .remove = FALSE) |>
  select(-c(changelog)) |>
  unnest_longer(col = histories, values_to = "values") |>
  unnest_longer(col = values, values_to = "values", indices_to = "column") |>
  filter(column %in% c("created", "items")) |>
  pivot_wider(names_from = column, values_from = values, values_fn = list) |>
  unnest_longer(col = created:items) |>
  select(-c(created_id, items_id)) |>
  unnest_longer(items) |>
  safe_hoist(
    items,
    item_field = list("field"),
    .remove = FALSE
  ) |>
  safe_hoist(
    items,
    item_fromString = list("fromString"),
    .remove = FALSE
  ) |>
  safe_hoist(
    items,
    item_toString = list("toString"),
    .remove = FALSE
  ) |>
  group_by(IssueKey) |>
  mutate(rowNum = row_number()) |>
  mutate(
    created = case_when(
      !any(item_field == "status") & rowNum == 1 ~ timestamp,
      .default = created
    ),
    item_toString = case_when(
      !any(item_field == "status") & rowNum == 1 ~ "No-Change",
      .default = item_toString
    ),
    item_fromString = case_when(
      !any(item_field == "status") & rowNum == 1 ~ "Open",
      .default = item_fromString
    ),
    item_field = case_when(
      !any(item_field == "status") & rowNum == 1 ~ "status",
      .default = item_field
    )
  ) |>
  ungroup() |>
  filter(item_field == "status") |>
  select(-c(items, rowNum)) |>
  mutate(
    TicketCreated = ymd_hms(TicketCreated),
    created = ymd_hms(created)
  ) |>
  arrange(IssueKey, created) |>
  group_by(IssueKey) |>
  mutate(
    TimeElapsed = as.numeric(difftime(
      created,
      lag(created, n = 1, default = first(TicketCreated)),
      units = "days"
    ))
  ) |>
  ungroup() |>
  select(-c(item_toString, item_field, created, Status, TicketCreated)) |>
  mutate(
    item_fromString = gsub(" ", "", tools::toTitleCase(item_fromString))
  ) |> # deal with variable capitalization
  group_by(IssueKey, item_fromString) |>
  summarise(TimeElapsed = sum(TimeElapsed, na.rm = TRUE)) |>
  pivot_wider(names_from = item_fromString, values_from = TimeElapsed) |>
  ungroup()

# Deal with issues where extra newline characters screwed up the read in of data to power bi
Issues <- Issues |>
  mutate(across(where(is.character), ~ gsub(",", "", .x))) |>
  mutate(across(where(is.character), ~ trimws(.x))) |>
  left_join(StatusChange, by = join_by(IssueKey)) |>
  select(-changelog) |>
  mutate(across(
    c(Created, Updated, Resolved),
    ~ as.POSIXct(.x, format = "%Y-%m-%dT%H:%M:%OS%z", tz = "UTC")
  )) |>
  mutate(DueDate = as.Date(DueDate, format = "%Y-%m-%d")) |>
  mutate(NumberOfSpaces = as.integer(NumberOfSpaces)) |>
  mutate(across(
    c(Closed, InProgress, Open, Reopened, WaitingforCustomer, OnHold),
    ~ ifelse(is.finite(.x), .x, NA_real_)
  )) |>
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
      IssueKey             NVARCHAR(20)    NOT NULL,
      IssueType            NVARCHAR(50)    NULL,
      Address              NVARCHAR(800)   NULL,
      Assignee             NVARCHAR(100)   NULL,
      Created              DATETIME2(3)    NULL,
      RequestedDueDate     NVARCHAR(100)   NULL,
      SpaceBookingAdmin    NVARCHAR(100)   NULL,
      NumberOfSpaces       INT             NULL,
      FloorPlan            NVARCHAR(10)    NULL,
      FurniturePlan        NVARCHAR(10)    NULL,
      LastUpdatedStatus    NVARCHAR(800)   NULL,
      Department           NVARCHAR(300)   NULL,
      DueDate              DATE            NULL,
      Organization         NVARCHAR(100)   NULL,
      Priority             NVARCHAR(100)   NULL,
      Reporter             NVARCHAR(100)   NULL,
      RequestParticipants  NVARCHAR(900)   NULL,
      RequestType          NVARCHAR(250)   NULL,
      Resolved             DATETIME2(3)    NULL,
      Status               NVARCHAR(100)   NULL,
      Summary              NVARCHAR(900)   NULL,
      Updated              DATETIME2(3)    NULL,
      Parent               NVARCHAR(100)   NULL,
      Closed               DECIMAL(18,6)   NULL,
      InProgress           DECIMAL(18,6)   NULL,
      [Open]               DECIMAL(18,6)   NULL,
      Reopened             DECIMAL(18,6)   NULL,
      WaitingforCustomer   DECIMAL(18,6)   NULL,
      OnHold               DECIMAL(18,6)   NULL
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
          IssueKey             NVARCHAR(20)    NOT NULL,
          IssueType            NVARCHAR(50)    NULL,
          Address              NVARCHAR(800)   NULL,
          Assignee             NVARCHAR(100)   NULL,
          Created              DATETIME2(3)    NULL,
          RequestedDueDate     NVARCHAR(100)   NULL,
          SpaceBookingAdmin    NVARCHAR(100)   NULL,
          NumberOfSpaces       INT             NULL,
          FloorPlan            NVARCHAR(10)    NULL,
          FurniturePlan        NVARCHAR(10)    NULL,
          LastUpdatedStatus    NVARCHAR(800)   NULL,
          Department           NVARCHAR(300)   NULL,
          DueDate              DATE            NULL,
          Organization         NVARCHAR(100)   NULL,
          Priority             NVARCHAR(100)   NULL,
          Reporter             NVARCHAR(100)   NULL,
          RequestParticipants  NVARCHAR(900)   NULL,
          RequestType          NVARCHAR(250)   NULL,
          Resolved             DATETIME2(3)    NULL,
          Status               NVARCHAR(100)   NULL,
          Summary              NVARCHAR(900)   NULL,
          Updated              DATETIME2(3)    NULL,
          Parent               NVARCHAR(100)   NULL,
          Closed               DECIMAL(18,6)   NULL,
          InProgress           DECIMAL(18,6)   NULL,
          [Open]               DECIMAL(18,6)   NULL,
          Reopened             DECIMAL(18,6)   NULL,
          WaitingforCustomer   DECIMAL(18,6)   NULL,
          OnHold               DECIMAL(18,6)   NULL
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
      tgt.RefreshDate          = src.RefreshDate,
      tgt.IssueType            = src.IssueType,
      tgt.Address              = src.Address,
      tgt.Assignee             = src.Assignee,
      tgt.Created              = src.Created,
      tgt.RequestedDueDate     = src.RequestedDueDate,
      tgt.SpaceBookingAdmin    = src.SpaceBookingAdmin,
      tgt.NumberOfSpaces       = src.NumberOfSpaces,
      tgt.FloorPlan            = src.FloorPlan,
      tgt.FurniturePlan        = src.FurniturePlan,
      tgt.LastUpdatedStatus    = src.LastUpdatedStatus,
      tgt.Department           = src.Department,
      tgt.DueDate              = src.DueDate,
      tgt.Organization         = src.Organization,
      tgt.Priority             = src.Priority,
      tgt.Reporter             = src.Reporter,
      tgt.RequestParticipants  = src.RequestParticipants,
      tgt.RequestType          = src.RequestType,
      tgt.Resolved             = src.Resolved,
      tgt.Status               = src.Status,
      tgt.Summary              = src.Summary,
      tgt.Updated              = src.Updated,
      tgt.Parent               = src.Parent,
      tgt.Closed               = src.Closed,
      tgt.InProgress           = src.InProgress,
      tgt.[Open]               = src.[Open],
      tgt.Reopened             = src.Reopened,
      tgt.WaitingforCustomer   = src.WaitingforCustomer,
      tgt.OnHold               = src.OnHold
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

    # Insert new rows into the SBP table
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
          IssueType,
          Address,
          Assignee,
          Created,
          RequestedDueDate,
          SpaceBookingAdmin,
          NumberOfSpaces,
          FloorPlan,
          FurniturePlan,
          LastUpdatedStatus,
          Department,
          DueDate,
          Organization,
          Priority,
          Reporter,
          RequestParticipants,
          RequestType,
          Resolved,
          Status,
          Summary,
          Updated,
          Parent,
          Closed,
          InProgress,
          [Open],
          Reopened,
          WaitingforCustomer,
          OnHold
        )
        SELECT
          src.RefreshDate,
          src.IssueKey,
          src.IssueType,
          src.Address,
          src.Assignee,
          src.Created,
          src.RequestedDueDate,
          src.SpaceBookingAdmin,
          src.NumberOfSpaces,
          src.FloorPlan,
          src.FurniturePlan,
          src.LastUpdatedStatus,
          src.Department,
          src.DueDate,
          src.Organization,
          src.Priority,
          src.Reporter,
          src.RequestParticipants,
          src.RequestType,
          src.Resolved,
          src.Status,
          src.Summary,
          src.Updated,
          src.Parent,
          src.Closed,
          src.InProgress,
          src.[Open],
          src.Reopened,
          src.WaitingforCustomer,
          src.OnHold
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

write.csv(Issues, "E:/Projects/PBI-Gateway/SBP_Issues.csv", row.names = FALSE)
