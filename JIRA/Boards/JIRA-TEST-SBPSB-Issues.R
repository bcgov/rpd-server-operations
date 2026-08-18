# For server logging
# Begin timer
task_start <- Sys.time()

# Set necessary variables
dashboard_id <- "SBPSB"
extension <- "_LinkedIssues"
target_table <- DBI::Id(schema = schema_name, table = dashboard_id)
target_table2 <- DBI::Id(
  schema = schema_name,
  table = paste0(dashboard_id, extension)
)
temp_table <- paste0("#", dashboard_id, "Temp")
temp_table2 <- paste0("#", dashboard_id, extension, "Temp")
api_name <- "Jira"
script_name <- "Jira_SBPSB"
script_name2 <- paste0("Jira_SBPSB", extension)

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

    issues <- data |>
      purrr::pluck("issues") |>
      tibble::enframe() |>
      tidyr::unnest_wider(value) |>
      tidyr::unnest_wider(fields) |>
      plyr::rename(names) |>
      rename_with(~ gsub(" ", "", .)) |>
      # Parent column is sometimes missing as sparsely populated
      mutate(
        Parent = if ("Parent" %in% names(pick(everything()))) Parent else NA
      ) |>
      # Select fields of interest
      select(
        IssueKey = key,
        IssueType,
        Assignee,
        Created,
        Labels,
        OriginalEstimate = Originalestimate,
        ApprovedByExecutive = ApprovedbyExecutives,
        MoSoCOW,
        ImpactToUser = ImpacttoUser,
        LinkedIssues,
        Priority,
        Reporter,
        RequestType,
        Status,
        Parent,
        Project,
        Summary
      ) |>
      safe_hoist(IssueType, IssueType = "name", .remove = FALSE) |>
      safe_hoist(Assignee, Assignee = "displayName", .remove = FALSE) |>
      # Labels will need some concatenation
      tidyr::unnest_wider(Labels, names_sep = "_") |>
      rowwise() |>
      mutate(
        Labels = stringr::str_flatten_comma(
          c(across(starts_with("Labels_"))),
          na.rm = TRUE
        ),
        .after = Created,
        .keep = "unused"
      ) |>
      ungroup() |>
      safe_hoist(
        ApprovedByExecutive,
        ApprovedByExecutive = "value",
        .remove = FALSE
      ) |>
      safe_hoist(MoSoCOW, MoSoCOW = "value", .remove = FALSE) |>
      safe_hoist(ImpactToUser, ImpactToUser = "value", .remove = FALSE) |>
      safe_hoist(Priority, Priority = "name", .remove = FALSE) |>
      safe_hoist(Reporter, Reporter = "displayName", .remove = FALSE) |>
      safe_hoist(
        RequestType,
        RequestType = list("requestType", "name"),
        .remove = FALSE
      ) |>
      safe_hoist(Status, Status = "name", .remove = FALSE) |>
      safe_hoist(Project, Project = "key", .remove = FALSE) |>
      safe_hoist(Parent, Parent = "key", .remove = FALSE)
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


# Linked Issues
tryCatch(
  {
    LinkedIssues <- Issues |>
      select(IssueKey, LinkedIssues) |>
      tidyr::unnest_wider(LinkedIssues, names_sep = "_") |>
      tidyr::unnest_wider(starts_with("LinkedIssues"), names_sep = "_") |>
      tidyr::unnest_wider(where(is.list), names_sep = "_") |>
      # select(IssueKey, matches("(\\d+)_id"), ends_with("type_name"), , ends_with("type_outward"), ends_with("outwardIssue_key")) |>
      select(
        IssueKey,
        matches("(\\d+)_id"),
        ends_with("type_name"),
        ends_with("type_inward"),
        ends_with("type_outward"),
        ends_with("Issue_key")
      ) |>
      pivot_longer(
        cols = matches("LinkedIssues_(\\d+)_id"),
        names_to = "link_name",
        values_to = "link_value"
      ) |>
      filter(!is.na(link_value)) |>
      relocate(link_value, .after = IssueKey) |>
      pivot_longer(
        cols = matches("(\\d+)"),
        names_to = "col_name",
        values_to = "col_value"
      ) |>
      mutate(
        link_name_num = stringr::str_extract(link_name, "(\\d+)"),
        col_name_num = stringr::str_extract(col_name, "(\\d+)")
      ) |>
      filter(link_name_num == col_name_num) |>
      select(-c(link_name, link_name_num, col_name_num)) |>
      mutate(
        col_name = stringr::str_replace(col_name, "LinkedIssues_(\\d+)_", "")
      ) |>
      pivot_wider(
        id_cols = c(IssueKey, link_value),
        names_from = col_name,
        values_from = col_value
      ) |>
      # This step guarantees the dataframe shape after pivot
      # variable API payload may result in missing columns
      ensure_columns(c("type_name", "type_inward", "type_outward", "inwardIssue_key", "outwardIssue_key")) |>
      # Next three steps have a .default = "Error", will need some kind of logging or check for this
      mutate(
        TypeFlag = case_when(
          is.na(inwardIssue_key) ~ "Outward",
          is.na(outwardIssue_key) ~ "Inward",
          .default = "Error"
        )
      ) |>
      mutate(
        RelationDesc = case_when(
          TypeFlag == "Outward" ~ type_outward,
          TypeFlag == "Inward" ~ type_inward,
          .default = "Error"
        )
      ) |>
      mutate(
        RelationIssueKey = case_when(
          TypeFlag == "Outward" ~ outwardIssue_key,
          TypeFlag == "Inward" ~ inwardIssue_key,
          .default = "Error"
        )
      ) |>
      rename(
        RelationId = link_value,
        RelationCategory = type_name,
      ) |>
      select(
        -c(
          type_outward,
          type_inward,
          TypeFlag,
          outwardIssue_key,
          inwardIssue_key
        )
      ) |>
      mutate(RefreshDate = Sys.time(), .before = everything())
  },
  error = function(e) {
    log_daily_etl_run(
      api_name = api_name,
      script_name = script_name2,
      table_name = dashboard_id,
      status = "FAILURE",
      message = paste0(
        "LinkedIssues failure: ",
        substr(conditionMessage(e), 1, 500)
      )
    )
    stop(e) # rethrow so Task Scheduler/Nagios still flags it
  }
)

error_rows <- LinkedIssues |> filter(RelationDesc == "Error" | RelationIssueKey == "Error")

if (nrow(error_rows) > 0) {
  log_daily_etl_run(
    status = "WARNING",  # or whatever your existing status vocabulary supports
    message = sprintf(
      "Relation Error in Desc or IssueKey for %d row(s). IssueKeys: %s",
      nrow(error_rows),
      paste(head(error_rows$IssueKey, 10), collapse = ", ")
    )
  )
}

# Deal with issues where extra newline characters screwed up the read in of data to power bi
tryCatch(
  {
    Issues <- Issues |>
      select(-LinkedIssues) |>
      mutate(across(where(is.character), ~ gsub(",", "", .x))) |>
      mutate(across(where(is.character), ~ trimws(.x))) |>
      mutate(
        across(
          c(
            Created
          ),
          ~ as.POSIXct(.x, format = "%Y-%m-%dT%H:%M:%OS%z", tz = "UTC")
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
      RefreshDate          DATETIME2(3)     NOT NULL,
      IssueKey             NVARCHAR(20)     NOT NULL,
      IssueType            NVARCHAR(20)     NOT NULL,
      Assignee             NVARCHAR(100)    NULL,
      Created              DATETIME2(0)     NULL,
      Labels               NVARCHAR(100)    NULL,
      OriginalEstimate     NVARCHAR(20)     NULL,
      ApprovedByExecutive  NVARCHAR(20)     NULL,
      MoSoCOW              NVARCHAR(20)     NULL,
      ImpactToUser         NVARCHAR(100)    NULL,
      Priority             NVARCHAR(20)     NULL,
      Reporter             NVARCHAR(100)    NULL,
      RequestType          NVARCHAR(100)    NULL,
      Status               NVARCHAR(50)     NULL,
      Parent               NVARCHAR(20)     NULL,
      Project              NVARCHAR(10)     NULL,
      Summary              NVARCHAR(500)    NULL
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
        RefreshDate          DATETIME2(3)     NOT NULL,
        IssueKey             NVARCHAR(20)     NOT NULL,
        IssueType            NVARCHAR(20)     NOT NULL,
        Assignee             NVARCHAR(100)    NULL,
        Created              DATETIME2(0)     NULL,
        Labels               NVARCHAR(100)    NULL,
        OriginalEstimate     NVARCHAR(20)     NULL,
        ApprovedByExecutive  NVARCHAR(20)     NULL,
        MoSoCOW              NVARCHAR(20)     NULL,
        ImpactToUser         NVARCHAR(100)    NULL,
        Priority             NVARCHAR(20)     NULL,
        Reporter             NVARCHAR(100)    NULL,
        RequestType          NVARCHAR(100)    NULL,
        Status               NVARCHAR(50)     NULL,
        Parent               NVARCHAR(20)     NULL,
        Project              NVARCHAR(10)     NULL,
        Summary              NVARCHAR(500)    NULL
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

    # Update the table with new data for existing rows
    n_updated <- dbExecute(
      con,
      paste0(
        "UPDATE tgt
        SET
        tgt.RefreshDate         = src.RefreshDate,
        tgt.IssueType           = src.IssueType,
        tgt.Assignee            = src.Assignee,
        tgt.Created             = src.Created,
        tgt.Labels              = src.Labels,
        tgt.OriginalEstimate    = src.OriginalEstimate,
        tgt.ApprovedByExecutive = src.ApprovedByExecutive,
        tgt.MoSoCOW             = src.MoSoCOW,
        tgt.ImpactToUser        = src.ImpactToUser,
        tgt.Priority            = src.Priority,
        tgt.Reporter            = src.Reporter,
        tgt.RequestType         = src.RequestType,
        tgt.Status              = src.Status,
        tgt.Parent              = src.Parent,
        tgt.Project             = src.Project,
        tgt.Summary             = src.Summary
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

    # Insert new rows into the table
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
          IssueType,
          Assignee,
          Created,
          Labels,
          OriginalEstimate,
          ApprovedByExecutive,
          MoSoCOW,
          ImpactToUser,
          Priority,
          Reporter,
          RequestType,
          Status,
          Parent,
          Project,
          Summary
        )
        SELECT
          src.RefreshDate,
          src.IssueKey,
          src.IssueType,
          src.Assignee,
          src.Created,
          src.Labels,
          src.OriginalEstimate,
          src.ApprovedByExecutive,
          src.MoSoCOW,
          src.ImpactToUser,
          src.Priority,
          src.Reporter,
          src.RequestType,
          src.Status,
          src.Parent,
          src.Project,
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

# Database SBPSB Linked Issues ####

# dbRemoveTable(con, target_table2)
if (!dbExistsTable(con, target_table2)) {
  sql <- paste0(
    "CREATE TABLE ",
    schema_name,
    ".",
    dashboard_id,
    extension,
    " (
      RefreshDate        DATETIME2(3)  NOT NULL,
      IssueKey           NVARCHAR(20)  NOT NULL,
      RelationId         NVARCHAR(10)  NULL,
      RelationCategory   NVARCHAR(50)  NULL,
      RelationDesc       NVARCHAR(50)  NULL,
      RelationIssueKey   NVARCHAR(20)  NULL
    );
  "
  )

  dbExecute(con, sql)
}

etl_error <- NULL

# Control database transaction to ensure all steps done together or not at all
dbBegin(con)

tryCatch(
  {
    if (dbExistsTable(con, temp_table2)) {
      dbRemoveTable(con, temp_table2)
    }

    # Create temp table to hold new data
    dbExecute(
      con,
      paste0(
        "CREATE TABLE ",
        temp_table2,
        " (
          RefreshDate        DATETIME2(3)  NOT NULL,
          IssueKey           NVARCHAR(20)  NOT NULL,
          RelationId         NVARCHAR(10)  NULL,
          RelationCategory   NVARCHAR(50)  NULL,
          RelationDesc       NVARCHAR(50)  NULL,
          RelationIssueKey   NVARCHAR(20)  NULL
        );
        "
      )
    )

    # Write into temp table the current Issues
    dbWriteTable(
      con,
      name = temp_table2,
      value = LinkedIssues,
      append = TRUE,
      overwrite = FALSE
    )

    # -- Guard: catch duplicate keys in source data before touching target --
    dup_count <- dbGetQuery(
      con,
      paste0(
        "SELECT COUNT(*) AS n
         FROM (
           SELECT IssueKey, RelationId
           FROM ",
        temp_table2,
        "
           GROUP BY IssueKey, RelationId
           HAVING COUNT(*) > 1
         ) dupes;"
      )
    )$n

    if (dup_count > 0) {
      stop(paste0(
        "Duplicate IssueKey, RelationId values detected in source data (",
        dup_count,
        " keys affected). Rolling back."
      ))
    }

    # Update the table with new data for existing rows
    n_updated <- dbExecute(
      con,
      paste0(
        "UPDATE tgt
          SET
          tgt.RefreshDate      = src.RefreshDate,
          tgt.RelationId       = src.RelationId,
          tgt.RelationCategory = src.RelationCategory,
          tgt.RelationDesc     = src.RelationDesc,
          tgt.RelationIssueKey = src.RelationIssueKey
        FROM ",
        schema_name,
        ".",
        dashboard_id,
        extension,
        " tgt
        INNER JOIN ",
        temp_table2,
        " src
        ON tgt.IssueKey = src.IssueKey
        AND tgt.RelationId = src.RelationId;"
      )
    )

    # Insert new rows into the table
    n_inserted <- dbExecute(
      con,
      paste0(
        "INSERT INTO ",
        schema_name,
        ".",
        dashboard_id,
        extension,
        " (
            RefreshDate,
            IssueKey,
            RelationId,
            RelationCategory,
            RelationDesc,
            RelationIssueKey
          )
          SELECT
            src.RefreshDate,
            src.IssueKey,
            src.RelationId,
            src.RelationCategory,
            src.RelationDesc,
            src.RelationIssueKey
          FROM ",
        temp_table2,
        " src
        LEFT JOIN ",
        schema_name,
        ".",
        dashboard_id,
        extension,
        " tgt
        ON tgt.IssueKey = src.IssueKey
        AND tgt.RelationId = src.RelationId
        WHERE tgt.IssueKey IS NULL
        AND tgt.RelationId IS NULL;"
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
    script_name = script_name2,
    table_name = paste0(dashboard_id, extension),
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
    script_name = script_name2,
    table_name = paste0(dashboard_id, extension),
    status = "FAILURE",
    message = substr(etl_error$message, 1, 500)
  )
  stop(etl_error)
}
