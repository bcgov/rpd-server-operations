# For server logging
# Begin timer
task_start <- Sys.time()

# Set necessary variables
DASHBOARD_ID <- "SBPSB"
EXTENSION <- "_LinkedIssues"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = DASHBOARD_ID)
TARGET_TABLE2 <- DBI::Id(
  schema = SCHEMA_NAME,
  table = paste0(DASHBOARD_ID, EXTENSION)
)
TEMP_TABLE <- paste0("#", DASHBOARD_ID, "Temp")
TEMP_TABLE2 <- paste0("#", DASHBOARD_ID, EXTENSION, "Temp")
API_NAME <- "Jira"
SCRIPT_NAME <- "Jira_SBPSB"
SCRIPT_NAME2 <- paste0("Jira_SBPSB", EXTENSION)

# Setup API parameters ####
expand_opts = c("names", "fields")
max_results = 100
nextPageToken = NULL
progress = 0
round = 1

# Issues Loop ####
while (progress < 2) {
  req <- request(query_url) |>
    req_headers_redacted(Authorization = token_string) |>
    # configure project, max_results, and start_at
    req_url_query(
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
          SCRIPT_NAME,
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

  if (is.null(nextPageToken)) {
    progress <- 2
  }

  if (length(resp$issues) == 0) {
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
      api_name = API_NAME,
      script_name = SCRIPT_NAME,
      table_name = DASHBOARD_ID,
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
        api_name = API_NAME,
        script_name = SCRIPT_NAME,
        table_name = DASHBOARD_ID,
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
      api_name = API_NAME,
      script_name = SCRIPT_NAME2,
      table_name = DASHBOARD_ID,
      status = "FAILURE",
      message = paste0(
        "LinkedIssues failure: ",
        substr(conditionMessage(e), 1, 500)
      )
    )
    stop(e) # rethrow so Task Scheduler/Nagios still flags it
  }
)

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
      api_name = API_NAME,
      script_name = SCRIPT_NAME,
      table_name = DASHBOARD_ID,
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
# dbRemoveTable(con, TARGET_TABLE)
if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- paste0(
    "CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    DASHBOARD_ID,
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

    # Insert new rows into the table
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

# Database SBPSB Linked Issues ####

# dbRemoveTable(con, TARGET_TABLE2)
if (!dbExistsTable(con, TARGET_TABLE2)) {
  sql <- paste0(
    "CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    DASHBOARD_ID,
    EXTENSION,
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
    if (dbExistsTable(con, TEMP_TABLE2)) {
      dbRemoveTable(con, TEMP_TABLE2)
    }

    # Create temp table to hold new data
    dbExecute(
      con,
      paste0(
        "CREATE TABLE ",
        TEMP_TABLE2,
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
      name = TEMP_TABLE2,
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
        TEMP_TABLE2,
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
        SCHEMA_NAME,
        ".",
        DASHBOARD_ID,
        EXTENSION,
        " tgt
        INNER JOIN ",
        TEMP_TABLE2,
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
        SCHEMA_NAME,
        ".",
        DASHBOARD_ID,
        EXTENSION,
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
        TEMP_TABLE2,
        " src
        LEFT JOIN ",
        SCHEMA_NAME,
        ".",
        DASHBOARD_ID,
        EXTENSION,
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
    api_name = API_NAME,
    script_name = SCRIPT_NAME2,
    table_name = paste0(DASHBOARD_ID, EXTENSION),
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
    script_name = SCRIPT_NAME2,
    table_name = paste0(DASHBOARD_ID, EXTENSION),
    status = "FAILURE",
    message = substr(etl_error$message, 1, 500)
  )
  stop(etl_error)
}
