# For server logging
# Begin timer
task_start <- Sys.time()

# Set necessary variables
DASHBOARD_ID <- "PSO"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = DASHBOARD_ID)
TEMP_TABLE <- paste0("#", DASHBOARD_ID, "Temp")
API_NAME <- "Jira"
SCRIPT_NAME <- "Jira_PSO"

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
        # select_if(~ !all(is.na(.))) |>
        rename_with(~ gsub(" ", "", .)) |>
        select(
          ArchibusPinNumber = ArchibusPINNumber,
          AccuracyCompletenessConcerns = `Arethereknowndataaccuracy,completeness,orsystemsconcernscausingafinancialdiscrepancy?`,
          Assignee,
          BranchBusinessArea = `BusinessArea/BranchSubmittingRequest`,
          BranchBusinessAreaImpacted = `Businessarea/branchesimpactedbythisrequest`,
          Created,
          GPOPackageApprover,
          HelpTopic,
          ComplexBeliefs = `Howcomplexdoyoubelievethisrequestis?`,
          DeadlineFinancialDriver = `Isthereadeadlineorfinancialcycle/processdrivingthisrequest?`,
          RequestRelatedProject = `Isthisrequestrelatedtoaspecificproject/agreement/PIN#`,
          IssueKey = key,
          IssueType,
          KahuaNumber,
          PerceivedImpact,
          Priority,
          ProjectDeliveryMethod,
          ProjectPartition,
          PurchaseOrder = `PurchaseOrder(PO)Number`,
          Reporter,
          Resolution,
          Resolved,
          Requestparticipants,
          RequestSubmittedBy,
          RequestType,
          Status,
          Summary,
          Timetofirstresponse,
          Timetoresolution,
          Updated,
          WhatRequest = `Whatareyourequesting?`,
          DecisionSubmissionSupport = `Whatdecision,submission,orfinancialactionsdoesthissupport?`,
          ChangeRequired = `Whattypeofchangeisrequired?`,
          FinancialSupportRequested = `WhattypeofFinancialSupportareyourequesting?`,
          ProcessImprovement = `WhattypeofProcessImprovementorPerformanceEnhancementareyourequesting?`,
          WhoImpactedOpportunity = `Whoisimpactedbythisissueorimprovementopportunity?`,
          WhoImpactedRequest = `Whoisimpactedbythisissueorrequest?`
        ) |>
        safe_hoist(
          AccuracyCompletenessConcerns,
          AccuracyCompletenessConcerns = "value",
          .remove = FALSE
        ) |>
        safe_hoist(Assignee, Assignee = "displayName", .remove = FALSE) |>
        safe_hoist(
          BranchBusinessArea,
          BranchBusinessArea = list(1L, "value"),
          .remove = FALSE
        ) |>
        safe_hoist(
          BranchBusinessAreaImpacted,
          BranchBusinessAreaImpacted = list(1L, "value"),
          .remove = FALSE
        ) |>
        safe_hoist(
          GPOPackageApprover,
          GPOPackageApprover = "displayName",
          .remove = FALSE
        ) |>
        safe_hoist(
          HelpTopic,
          HelpTopicDetail = list("child", "value"),
          .remove = FALSE
        ) |>
        safe_hoist(HelpTopic, HelpTopic = "value", .remove = FALSE) |>
        safe_hoist(ComplexBeliefs, ComplexBeliefs = "value", .remove = FALSE) |>
        safe_hoist(
          DeadlineFinancialDriver,
          DeadlineFinancialDriver = "value",
          .remove = FALSE
        ) |>
        safe_hoist(
          RequestRelatedProject,
          RequestRelatedProject = "value",
          .remove = FALSE
        ) |>
        safe_hoist(IssueType, IssueType = "name", .remove = FALSE) |>
        safe_hoist(
          PerceivedImpact,
          PerceivedImpact = "value",
          .remove = FALSE
        ) |>
        safe_hoist(Priority, Priority = "name", .remove = FALSE) |>
        safe_hoist(
          ProjectDeliveryMethod,
          ProjectDeliveryMethod = "value",
          .remove = FALSE
        ) |>
        safe_hoist(
          ProjectPartition,
          ProjectPartition = "value",
          .remove = FALSE
        ) |>
        safe_hoist(Reporter, Reporter = "displayName", .remove = FALSE) |>
        tidyr::unnest_wider(Requestparticipants, names_sep = "-") |>
        tidyr::unnest_wider(
          starts_with("Requestparticipants"),
          names_sep = "-"
        ) |>
        rowwise() |>
        mutate(
          RequestParticipants = stringr::str_c(
            c_across(
              matches(
                "Requestparticipants-[0-9]+-displayName"
              )
            ),
            collapse = ";"
          ),
          .after = Resolved
        ) |>
        ungroup() |>
        safe_hoist(
          RequestSubmittedBy,
          RequestSubmittedBy = "displayName",
          .remove = FALSE
        ) |>
        safe_hoist(
          RequestType,
          RequestType = list("requestType", "name"),
          .remove = FALSE
        ) |>
        safe_hoist(Resolution, Resolution = "name", .remove = FALSE) |>
        safe_hoist(Status, Status = "name", .remove = FALSE) |>
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
        safe_hoist(WhatRequest, WhatRequest = "value", .remove = FALSE) |>
        safe_hoist(
          DecisionSubmissionSupport,
          DecisionSubmissionSupport = "value",
          .remove = FALSE
        ) |>
        safe_hoist(
          ChangeRequired,
          ChangeRequired = list(1L, "value"),
          .remove = FALSE
        ) |>
        safe_hoist(
          FinancialSupportRequested,
          FinancialSupportRequested = "value",
          .remove = FALSE
        ) |>
        safe_hoist(
          ProcessImprovement,
          ProcessImprovement = list(1L, "value"),
          .remove = FALSE
        ) |>
        safe_hoist(
          WhoImpactedOpportunity,
          WhoImpactedOpportunity = list(1L, "value"),
          .remove = FALSE
        ) |>
        safe_hoist(
          WhoImpactedRequest,
          WhoImpactedRequest = list(1L, "value"),
          .remove = FALSE
        ) |>
        mutate(
          across(
            c(Created, Updated, Resolved),
            ~ as.Date(.x, format = "%Y-%m-%d")
          )
        ) |>
        mutate(
          MinutesToFirstResponse = round(
            (Timetofirstresponse / 1000 / 60),
            digits = 1
          ),
          MinutesToResolution = round(
            (Timetoresolution / 1000 / 60),
            digits = 1
          )
        ) |>
        mutate(
          across(
            where(is.character),
            trimws
          )
        ) |>
        mutate(
          across(
            where(is.character),
            ~ replace_values(.x, "n/a" ~ NA_character_, "N/A" ~ NA_character_)
          )
        ) |>
        select(
          ArchibusPinNumber,
          AccuracyCompletenessConcerns,
          Assignee,
          BranchBusinessArea,
          BranchBusinessAreaImpacted,
          Created,
          GPOPackageApprover,
          HelpTopic,
          HelpTopicDetail,
          ComplexBeliefs,
          DeadlineFinancialDriver,
          RequestRelatedProject,
          IssueKey,
          IssueType,
          KahuaNumber,
          PerceivedImpact,
          Priority,
          ProjectDeliveryMethod,
          ProjectPartition,
          PurchaseOrder,
          Reporter,
          Resolution,
          Resolved,
          RequestParticipants,
          RequestSubmittedBy,
          RequestType,
          Status,
          Summary,
          MinutesToFirstResponse,
          MinutesToResolution,
          Updated,
          WhatRequest,
          DecisionSubmissionSupport,
          ChangeRequired,
          FinancialSupportRequested,
          ProcessImprovement,
          WhoImpactedOpportunity,
          WhoImpactedRequest
        )
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

tryCatch(
  {
    Issues <- Issues |> mutate(RefreshDate = Sys.time(), .before = everything())
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
      RefreshDate                    DATETIME2(3)    NOT NULL,
      ArchibusPinNumber              NVARCHAR(25)    NULL,
      AccuracyCompletenessConcerns   NVARCHAR(250)   NULL,
      Assignee                       NVARCHAR(100)   NULL,
      BranchBusinessArea             NVARCHAR(25)    NULL,
      BranchBusinessAreaImpacted     NVARCHAR(25)    NULL,
      Created                        DATETIME2(3)    NOT NULL,
      GPOPackageApprover             NVARCHAR(100)   NULL,
      HelpTopic                      NVARCHAR(250)   NULL,
      HelpTopicDetail                NVARCHAR(250)   NULL,
      ComplexBeliefs                 NVARCHAR(250)   NULL,
      DeadlineFinancialDriver        NVARCHAR(25)    NULL,
      RequestRelatedProject          NVARCHAR(25)    NULL,
      IssueKey                       NVARCHAR(25)    NOT NULL,
      IssueType                      NVARCHAR(250)   NULL,
      KahuaNumber                    NVARCHAR(50)    NULL,
      PerceivedImpact                NVARCHAR(250)   NULL,
      Priority                       NVARCHAR(25)    NULL,
      ProjectDeliveryMethod          NVARCHAR(250)   NULL,
      ProjectPartition               NVARCHAR(25)    NULL,
      PurchaseOrder                  NVARCHAR(250)   NULL,
      Reporter                       NVARCHAR(250)   NULL,
      Resolution                     NVARCHAR(25)    NULL,
      Resolved                       DATETIME2(3)    NULL,
      RequestParticipants            NVARCHAR(250)   NULL,
      RequestSubmittedBy             NVARCHAR(250)   NULL,
      RequestType                    NVARCHAR(250)   NULL,
      Status                         NVARCHAR(250)   NULL,
      Summary                        NVARCHAR(1000)  NULL,
      MinutesToFirstResponse         DECIMAL(18,1)   NULL,
      MinutesToResolution            DECIMAL(18,1)   NULL,
      Updated                        DATETIME2(3)    NULL,
      WhatRequest                    NVARCHAR(250)   NULL,
      DecisionSubmissionSupport      NVARCHAR(250)   NULL,
      ChangeRequired                 NVARCHAR(250)   NULL,
      FinancialSupportRequested      NVARCHAR(250)   NULL,
      ProcessImprovement             NVARCHAR(250)   NULL,
      WhoImpactedOpportunity         NVARCHAR(250)   NULL,
      WhoImpactedRequest             NVARCHAR(250)   NULL
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
          RefreshDate                    DATETIME2(3)    NOT NULL,
          ArchibusPinNumber              NVARCHAR(25)    NULL,
          AccuracyCompletenessConcerns   NVARCHAR(250)   NULL,
          Assignee                       NVARCHAR(100)   NULL,
          BranchBusinessArea             NVARCHAR(25)    NULL,
          BranchBusinessAreaImpacted     NVARCHAR(25)    NULL,
          Created                        DATETIME2(3)    NOT NULL,
          GPOPackageApprover             NVARCHAR(100)   NULL,
          HelpTopic                      NVARCHAR(250)   NULL,
          HelpTopicDetail                NVARCHAR(250)   NULL,
          ComplexBeliefs                 NVARCHAR(250)   NULL,
          DeadlineFinancialDriver        NVARCHAR(25)    NULL,
          RequestRelatedProject          NVARCHAR(25)    NULL,
          IssueKey                       NVARCHAR(25)    NOT NULL,
          IssueType                      NVARCHAR(250)   NULL,
          KahuaNumber                    NVARCHAR(50)    NULL,
          PerceivedImpact                NVARCHAR(250)   NULL,
          Priority                       NVARCHAR(25)    NULL,
          ProjectDeliveryMethod          NVARCHAR(250)   NULL,
          ProjectPartition               NVARCHAR(25)    NULL,
          PurchaseOrder                  NVARCHAR(250)   NULL,
          Reporter                       NVARCHAR(250)   NULL,
          Resolution                     NVARCHAR(25)    NULL,
          Resolved                       DATETIME2(3)    NULL,
          RequestParticipants            NVARCHAR(250)   NULL,
          RequestSubmittedBy             NVARCHAR(250)   NULL,
          RequestType                    NVARCHAR(250)   NULL,
          Status                         NVARCHAR(250)   NULL,
          Summary                        NVARCHAR(1000)  NULL,
          MinutesToFirstResponse         DECIMAL(18,1)   NULL,
          MinutesToResolution            DECIMAL(18,1)   NULL,
          Updated                        DATETIME2(3)    NULL,
          WhatRequest                    NVARCHAR(250)   NULL,
          DecisionSubmissionSupport      NVARCHAR(250)   NULL,
          ChangeRequired                 NVARCHAR(250)   NULL,
          FinancialSupportRequested      NVARCHAR(250)   NULL,
          ProcessImprovement             NVARCHAR(250)   NULL,
          WhoImpactedOpportunity         NVARCHAR(250)   NULL,
          WhoImpactedRequest             NVARCHAR(250)   NULL
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
         tgt.RefreshDate                  = src.RefreshDate,
         tgt.ArchibusPinNumber            = src.ArchibusPinNumber,
         tgt.AccuracyCompletenessConcerns = src.AccuracyCompletenessConcerns,
         tgt.Assignee                     = src.Assignee,
         tgt.BranchBusinessArea           = src.BranchBusinessArea,
         tgt.BranchBusinessAreaImpacted   = src.BranchBusinessAreaImpacted,
         tgt.Created                      = src.Created,
         tgt.GPOPackageApprover           = src.GPOPackageApprover,
         tgt.HelpTopic                    = src.HelpTopic,
         tgt.HelpTopicDetail              = src.HelpTopicDetail,
         tgt.ComplexBeliefs               = src.ComplexBeliefs,
         tgt.DeadlineFinancialDriver      = src.DeadlineFinancialDriver,
         tgt.RequestRelatedProject        = src.RequestRelatedProject,
         tgt.IssueType                    = src.IssueType,
         tgt.KahuaNumber                  = src.KahuaNumber,
         tgt.PerceivedImpact              = src.PerceivedImpact,
         tgt.Priority                     = src.Priority,
         tgt.ProjectDeliveryMethod        = src.ProjectDeliveryMethod,
         tgt.ProjectPartition             = src.ProjectPartition,
         tgt.PurchaseOrder                = src.PurchaseOrder,
         tgt.Reporter                     = src.Reporter,
         tgt.Resolution                   = src.Resolution,
         tgt.Resolved                     = src.Resolved,
         tgt.RequestParticipants          = src.RequestParticipants,
         tgt.RequestSubmittedBy           = src.RequestSubmittedBy,
         tgt.RequestType                  = src.RequestType,
         tgt.Status                       = src.Status,
         tgt.Summary                      = src.Summary,
         tgt.MinutesToFirstResponse       = src.MinutesToFirstResponse,
         tgt.MinutesToResolution          = src.MinutesToResolution,
         tgt.Updated                      = src.Updated,
         tgt.WhatRequest                  = src.WhatRequest,
         tgt.DecisionSubmissionSupport    = src.DecisionSubmissionSupport,
         tgt.ChangeRequired               = src.ChangeRequired,
         tgt.FinancialSupportRequested    = src.FinancialSupportRequested,
         tgt.ProcessImprovement           = src.ProcessImprovement,
         tgt.WhoImpactedOpportunity       = src.WhoImpactedOpportunity,
         tgt.WhoImpactedRequest           = src.WhoImpactedRequest
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
          ArchibusPinNumber,
          AccuracyCompletenessConcerns,
          Assignee,
          BranchBusinessArea,
          BranchBusinessAreaImpacted,
          Created,
          GPOPackageApprover,
          HelpTopic,
          HelpTopicDetail,
          ComplexBeliefs,
          DeadlineFinancialDriver,
          RequestRelatedProject,
          IssueKey,
          IssueType,
          KahuaNumber,
          PerceivedImpact,
          Priority,
          ProjectDeliveryMethod,
          ProjectPartition,
          PurchaseOrder,
          Reporter,
          Resolution,
          Resolved,
          RequestParticipants,
          RequestSubmittedBy,
          RequestType,
          Status,
          Summary,
          MinutesToFirstResponse,
          MinutesToResolution,
          Updated,
          WhatRequest,
          DecisionSubmissionSupport,
          ChangeRequired,
          FinancialSupportRequested,
          ProcessImprovement,
          WhoImpactedOpportunity,
          WhoImpactedRequest
          )
        SELECT
          src.RefreshDate,
          src.ArchibusPinNumber,
          src.AccuracyCompletenessConcerns,
          src.Assignee,
          src.BranchBusinessArea,
          src.BranchBusinessAreaImpacted,
          src.Created,
          src.GPOPackageApprover,
          src.HelpTopic,
          src.HelpTopicDetail,
          src.ComplexBeliefs,
          src.DeadlineFinancialDriver,
          src.RequestRelatedProject,
          src.IssueKey,
          src.IssueType,
          src.KahuaNumber,
          src.PerceivedImpact,
          src.Priority,
          src.ProjectDeliveryMethod,
          src.ProjectPartition,
          src.PurchaseOrder,
          src.Reporter,
          src.Resolution,
          src.Resolved,
          src.RequestParticipants,
          src.RequestSubmittedBy,
          src.RequestType,
          src.Status,
          src.Summary,
          src.MinutesToFirstResponse,
          src.MinutesToResolution,
          src.Updated,
          src.WhatRequest,
          src.DecisionSubmissionSupport,
          src.ChangeRequired,
          src.FinancialSupportRequested,
          src.ProcessImprovement,
          src.WhoImpactedOpportunity,
          src.WhoImpactedRequest
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
