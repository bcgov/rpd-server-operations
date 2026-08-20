# For server logging
# Begin timer
task_start <- Sys.time()

# Set necessary variables
dashboard_id <- "RBAS"
target_table <- DBI::Id(schema = schema_name, table = dashboard_id)
temp_table <- paste0("#", dashboard_id, "Temp")
api_name <- "Jira"
script_name <- "PROD_Jira_RBAS"

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
    # pull the names attribute and prep to rename the issue columns.
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

    # pull rows of issues, rename, unnest columns and format.
    Issues <- data |>
      purrr::pluck("issues") |>
      tibble::enframe() |>
      tidyr::unnest_wider(value) |>
      tidyr::unnest_wider(fields) |>
      plyr::rename(names) |>
      rename_with(~ gsub(" ", "", .)) |>
      select(
        IssueKey = key,
        Created,
        EndDate,
        RequestedDueDate,
        Duedate,
        Updated,
        Resolved,
        Resolution,
        Organization = `Ministry/BPSOrganization`,
        RPDBranch,
        MYSCReq = `MYSCReq#`,
        RequestType,
        Status,
        StatusCategory,
        StatusCategoryChanged,
        Assignee,
        EmployeeID,
        Reporter,
        Summary
      ) |>
      safe_hoist(Resolution, Resolution = "name", .remove = FALSE) |>
      safe_hoist(RPDBranch, RPDBranch = "value", .remove = FALSE) |>
      safe_hoist(
        RequestType,
        RequestType = list("requestType", "name"),
        .remove = FALSE
      ) |>
      safe_hoist(Status, Status = "name", .remove = FALSE) |>
      safe_hoist(StatusCategory, StatusCategory = "name", .remove = FALSE) |>
      safe_hoist(Assignee, Assignee = "displayName", .remove = FALSE) |>
      safe_hoist(Reporter, Reporter = "displayName", .remove = FALSE) |>
      safe_hoist(Organization, Organization = "value", .remove = FALSE) |>
      safe_hoist(
        RequestedDueDate,
        RequestedDueDate = "value",
        .remove = FALSE
      ) |>
      mutate(
        across(
          c(
            Created,
            Resolved,
            StatusCategoryChanged,
            Updated
          ),
          ~ as.POSIXct(.x, format = "%Y-%m-%dT%H:%M:%OS%z", tz = "UTC")
        )
      ) |>
      mutate(
        across(
          c(
            Duedate,
            EndDate
          ),
          as.Date
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
    Issues <- Issues |>
      # Add a filter step to remove all the test tickets prior to launch on Aug 18th 2025
      filter(
        !IssueKey %in%
          c("RBAS-1", "RBAS-2", "RBAS-3", "RBAS-4", "RBAS-5", "RBAS-6")
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
      RefreshDate            DATETIME2(3)    NOT NULL,
      IssueKey               NVARCHAR(250)   NOT NULL,
      Created                DATETIME2(3)    NOT NULL,
      EndDate                DATE            NULL,
      RequestedDueDate       NVARCHAR(100)   NULL,
      Duedate                DATE            NULL,
      Updated                DATETIME2(3)    NULL,
      Resolved               DATETIME2(3)    NULL,
      Resolution             NVARCHAR(100)   NULL,
      Organization           NVARCHAR(25)    NULL,
      RPDBranch              NVARCHAR(100)   NULL,
      MYSCReq                NVARCHAR(500)   NULL,
      RequestType            NVARCHAR(100)   NULL,
      Status                 NVARCHAR(100)   NULL,
      StatusCategory         NVARCHAR(100)   NULL,
      StatusCategoryChanged  DATETIME2(3)    NULL,
      Assignee               NVARCHAR(100)   NULL,
      EmployeeID             NVARCHAR(100)   NULL,
      Reporter               NVARCHAR(100)   NULL,
      Summary                NVARCHAR(1000)  NULL
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
          RefreshDate            DATETIME2(3)    NOT NULL,
          IssueKey               NVARCHAR(250)   NOT NULL,
          Created                DATETIME2(3)    NOT NULL,
          EndDate                DATE            NULL,
          RequestedDueDate       NVARCHAR(100)   NULL,
          Duedate                DATE            NULL,
          Updated                DATETIME2(3)    NULL,
          Resolved               DATETIME2(3)    NULL,
          Resolution             NVARCHAR(100)   NULL,
          Organization           NVARCHAR(25)    NULL,
          RPDBranch              NVARCHAR(100)   NULL,
          MYSCReq                NVARCHAR(500)   NULL,
          RequestType            NVARCHAR(100)   NULL,
          Status                 NVARCHAR(100)   NULL,
          StatusCategory         NVARCHAR(100)   NULL,
          StatusCategoryChanged  DATETIME2(3)    NULL,
          Assignee               NVARCHAR(100)   NULL,
          EmployeeID             NVARCHAR(100)   NULL,
          Reporter               NVARCHAR(100)   NULL,
          Summary                NVARCHAR(1000)  NULL
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

    # Update the RBAS table with new data for existing rows
    n_updated <- dbExecute(
      con,
      paste0(
        "UPDATE tgt
         SET
         tgt.RefreshDate              = src.RefreshDate,
         tgt.Created                  = src.Created,
         tgt.EndDate                  = src.EndDate,
         tgt.RequestedDueDate         = src.RequestedDueDate,
         tgt.Duedate                  = src.Duedate,
         tgt.Updated                  = src.Updated,
         tgt.Resolved                 = src.Resolved,
         tgt.Resolution               = src.Resolution,
         tgt.Organization             = src.Organization,
         tgt.RPDBranch                = src.RPDBranch,
         tgt.MYSCReq                  = src.MYSCReq,
         tgt.RequestType              = src.RequestType,
         tgt.Status                   = src.Status,
         tgt.StatusCategory           = src.StatusCategory,
         tgt.StatusCategoryChanged    = src.StatusCategoryChanged,
         tgt.Assignee                 = src.Assignee,
         tgt.EmployeeID               = src.EmployeeID,
         tgt.Reporter                 = src.Reporter,
         tgt.Summary                  = src.Summary
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

    # Insert new rows into the RBAS table
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
          Created,
          EndDate,
          RequestedDueDate,
          Duedate,
          Updated,
          Resolved,
          Resolution,
          Organization,
          RPDBranch,
          MYSCReq,
          RequestType,
          Status,
          StatusCategory,
          StatusCategoryChanged,
          Assignee,
          EmployeeID,
          Reporter,
          Summary
          )
        SELECT
          src.RefreshDate,
          src.IssueKey,
          src.Created,
          src.EndDate,
          src.RequestedDueDate,
          src.Duedate,
          src.Updated,
          src.Resolved,
          src.Resolution,
          src.Organization,
          src.RPDBranch,
          src.MYSCReq,
          src.RequestType,
          src.Status,
          src.StatusCategory,
          src.StatusCategoryChanged,
          src.Assignee,
          src.EmployeeID,
          src.Reporter,
          src.Summary
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
