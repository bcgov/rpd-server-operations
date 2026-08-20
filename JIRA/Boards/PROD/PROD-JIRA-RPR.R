# For server logging
# Begin timer
task_start <- Sys.time()

# Set necessary variables
dashboard_id <- "RPR"
target_table <- DBI::Id(schema = schema_name, table = dashboard_id)
temp_table <- paste0("#", dashboard_id, "Temp")
api_name <- "Jira"
script_name <- "PROD_Jira_RPR"

# Setup API parameters ####
expand_opts = c("names", "fields")
max_results = 100
start_time <- etl_window$jira_start_time

# Issues Loop ####
data <- call_jira_api(
  api_name,
  script_name,
  dashboard_id,
  query_url,
  expand_opts,
  max_results,
  token_string,
  start_time
)

if (length(data$issues) == 0) {
  # API succeeded, nothing to load
  no_data_msg <- paste0(
    "No data returned from API for window ",
    start_time,
    " to ",
    format(Sys.time(), tz = "UTC"),
    " UTC"
  )

  cat(no_data_msg, "— nothing to load. Exiting gracefully.\n")

  log_daily_etl_run(
    api_name = api_name,
    script_name = script_name,
    table_name = dashboard_id,
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

tryCatch(
  {
    names <- data |>
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

    Issues <- data |>
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
        # Enddate,
        Resolved,
        # Resolution,
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
      safe_hoist_all(
        RequestParticipants,
        RequestParticipants = list("displayName")
      ) |>
      ungroup() |>
      mutate(
        across(
          c(Created, Updated, Resolved),
          ~ as.Date(.x, format = "%Y-%m-%d")
        )
      ) |>
      mutate(
        DaysToResolution = Timetoresolution / (1000 * 60 * 60 * 24),
        MinutesToFirstResponse = Timetofirstresponse / (1000 * 60),
        .keep = "unused",
        .after = DueDateflexibility
      )
  },
  error = function(e) {
    log_daily_etl_run(
      api_name = api_name,
      script_name = script_name,
      table_name = dashboard_id,
      status = "FAILURE",
      message = paste0(
        "Data wrangling failure: ",
        substr(conditionMessage(e), 1, 500)
      )
    )
    stop(e) # rethrow so Task Scheduler/Nagios still flags it
  }
)

tryCatch(
  {
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
  },
  error = function(e) {
    log_daily_etl_run(
      api_name = api_name,
      script_name = script_name,
      table_name = dashboard_id,
      status = "FAILURE",
      message = paste0(
        "Issues assignment failure: ",
        substr(conditionMessage(e), 1, 500)
      )
    )
    stop(e) # rethrow so Task Scheduler/Nagios still flags it
  }
)

# Start database transaction ####
# dbRemoveTable(con, target_table)
if (!dbExistsTable(con, target_table)) {
  sql <- paste0(
    "CREATE TABLE ",
    schema_name,
    ".",
    dashboard_id,
    " (
      RefreshDate             DATETIME2(3)    NOT NULL,
      IssueKey                NVARCHAR(10)    NOT NULL,
      IssueType               NVARCHAR(50)    NULL,
      Status                  NVARCHAR(20)    NULL,
      Created                 DATE            NULL,
      Updated                 DATE            NULL,
      Resolved                DATE            NULL,
      Duedate                 NVARCHAR(10)    NULL,
      DueDateflexibility      NVARCHAR(10)    NULL,
      MinutesToFirstResponse  Decimal(18,9)   NULL,
      DaysToResolution        Decimal(18,9)   NULL,
      Assignee                NVARCHAR(20)    NULL,
      Audience                NVARCHAR(15)    NULL,
      Frequency               NVARCHAR(20)    NULL,
      Priority                NVARCHAR(10)    NULL,
      ReportName              NVARCHAR(250)   NULL,
      Reporter                NVARCHAR(25)    NULL,
      RequestType             NVARCHAR(35)    NULL,
      Summary                 NVARCHAR(500)   NULL,
      Team                    NVARCHAR(10)    NULL,
      Branch                  NVARCHAR(50)    NULL,
      RequestParticipants     NVARCHAR(500)   NULL
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
    if (dbExistsTable(con, temp_table)) {
      dbRemoveTable(con, temp_table)
    }

    # Create temp table to hold new data
    dbExecute(
      con,
      paste0(
        "CREATE TABLE ",
        temp_table,
        " (
          RefreshDate             DATETIME2(3)    NOT NULL,
          IssueKey                NVARCHAR(10)    NOT NULL,
          IssueType               NVARCHAR(50)    NULL,
          Status                  NVARCHAR(20)    NULL,
          Created                 DATE            NULL,
          Updated                 DATE            NULL,
          Resolved                DATE            NULL,
          Duedate                 NVARCHAR(10)    NULL,
          DueDateflexibility      NVARCHAR(10)    NULL,
          MinutesToFirstResponse  Decimal(18,9)   NULL,
          DaysToResolution        Decimal(18,9)   NULL,
          Assignee                NVARCHAR(20)    NULL,
          Audience                NVARCHAR(15)    NULL,
          Frequency               NVARCHAR(20)    NULL,
          Priority                NVARCHAR(10)    NULL,
          ReportName              NVARCHAR(250)   NULL,
          Reporter                NVARCHAR(25)    NULL,
          RequestType             NVARCHAR(35)    NULL,
          Summary                 NVARCHAR(500)   NULL,
          Team                    NVARCHAR(10)    NULL,
          Branch                  NVARCHAR(50)    NULL,
          RequestParticipants     NVARCHAR(500)   NULL
        );"
      )
    )

    # Write into temp table the current Issues
    dbWriteTable(
      con,
      name = temp_table,
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
        temp_table,
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

    # Update the RPR table with new data for existing rows
    n_updated <- dbExecute(
      con,
      paste0(
        "
      UPDATE tgt
      SET
       tgt.RefreshDate            = src.RefreshDate,
       tgt.IssueType              = src.IssueType,
       tgt.Status                 = src.Status,
       tgt.Created                = src.Created,
       tgt.Updated                = src.Updated,
       tgt.Resolved               = src.Resolved,
       tgt.Duedate                = src.Duedate,
       tgt.DueDateflexibility     = src.DueDateflexibility,
       tgt.MinutesToFirstResponse = src.MinutesToFirstResponse,
       tgt.DaysToResolution       = src.DaysToResolution,
       tgt.Assignee               = src.Assignee,
       tgt.Audience               = src.Audience,
       tgt.Frequency              = src.Frequency,
       tgt.Priority               = src.Priority,
       tgt.ReportName             = src.ReportName,
       tgt.Reporter               = src.Reporter,
       tgt.RequestType            = src.RequestType,
       tgt.Summary                = src.Summary,
       tgt.Team                   = src.Team,
       tgt.Branch                 = src.Branch,
       tgt.RequestParticipants    = src.RequestParticipants
      FROM ",
        schema_name,
        ".",
        dashboard_id,
        " tgt
       INNER JOIN ",
        temp_table,
        " src
       ON tgt.IssueKey = src.IssueKey;"
      )
    )

    # Insert new rows into the RPR table
    n_inserted <- dbExecute(
      con,
      paste0(
        "INSERT INTO ",
        schema_name,
        ".",
        dashboard_id,
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
           MinutesToFirstResponse,
           DaysToResolution,
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
           src.MinutesToFirstResponse,
           src.DaysToResolution,
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
        temp_table,
        " src
         LEFT JOIN ",
        schema_name,
        ".",
        dashboard_id,
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

    cat(script_name, " ETL complete — updated:", n_updated, "| inserted:", n_inserted, "\n")
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
    api_name = api_name,
    script_name = script_name,
    table_name = dashboard_id,
    duration = task_duration,
    status = "SUCCESS",
    n_inserted = n_inserted,
    n_updated = n_updated,
    n_deleted = NA,
    message = "ETL completed successfully"
  )
} else {
  log_daily_etl_run(
    api_name = api_name,
    script_name = script_name,
    table_name = dashboard_id,
    status = "FAILURE",
    message = substr(etl_error$message, 1, 500)
  )
  stop(etl_error)
}
