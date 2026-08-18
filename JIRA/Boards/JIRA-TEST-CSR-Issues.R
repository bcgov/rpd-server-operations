# For server logging
# Begin timer
task_start <- Sys.time()

# Set necessary variables
dashboard_id <- "CSR"
target_table <- DBI::Id(schema = schema_name, table = dashboard_id)
temp_table <- paste0("#", dashboard_id, "Temp")
api_name <- "Jira"
script_name <- "Jira_CSR"

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

      issues <- data |>
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
          Summary,
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

  if (round == 1) {
    Issues <- issues
  } else {
    Issues <- full_join(Issues, issues)
  }

  round <- 2
}

tryCatch(
  {
    Issues <- Issues |>
      mutate(
        Assignee = tidyr::replace_na(Assignee, "Unassigned"),
        CSM = tidyr::replace_na(CSM, "Unassigned")
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
      RefreshDate          DATETIME2(3)  NOT NULL,
      IssueKey             NVARCHAR(100) NOT NULL,
      Status               NVARCHAR(100) NULL,
      RequestType          NVARCHAR(100) NULL,
      Summary              NVARCHAR(500) NULL,
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
          RefreshDate          DATETIME2(3)  NOT NULL,
          IssueKey             NVARCHAR(100) NOT NULL,
          Status               NVARCHAR(100) NULL,
          RequestType          NVARCHAR(100) NULL,
          Summary              NVARCHAR(500) NULL,
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

    # Update the GPOPR table with new data for existing rows
    n_updated <- dbExecute(
      con,
      paste0(
        "
      UPDATE tgt
      SET
        tgt.[RefreshDate]          = src.[RefreshDate],
        tgt.[Status]               = src.[Status],
        tgt.[RequestType]          = src.[RequestType],
        tgt.[Summary]              = src.[Summary],
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

    # Insert new rows into the GPOPR table
    n_inserted <- dbExecute(
      con,
      paste0(
        "INSERT INTO ",
        schema_name,
        ".",
        dashboard_id,
        " (
          [RefreshDate],
          [IssueKey],
          [Status],
          [RequestType],
          [Summary],
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
          src.[RequestType],
          src.[Summary],
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
