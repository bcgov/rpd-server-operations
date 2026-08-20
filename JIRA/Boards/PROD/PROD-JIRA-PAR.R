# For server logging
# Begin timer
task_start <- Sys.time()

# Set necessary variables
dashboard_id <- "PAR"
target_table <- DBI::Id(schema = schema_name, table = dashboard_id)
temp_table <- paste0("#", dashboard_id, "Temp")
api_name <- "Jira"
script_name <- "PROD_Jira_PAR"

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
    etl_window$jira_start_time,
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
        ProjectEffectiveDate = as.Date(
          ProjectEffectiveDate,
          format = "%Y-%m-%d"
        )
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
    Issues <- Issues |> mutate(RefreshDate = Sys.time(), .before = everything())
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

    # Update the PAR table with new data for existing rows
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

    # Insert new rows into the PAR table
    n_inserted <- dbExecute(
      con,
      paste0(
        "INSERT INTO ",
        schema_name,
        ".",
        dashboard_id,
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
