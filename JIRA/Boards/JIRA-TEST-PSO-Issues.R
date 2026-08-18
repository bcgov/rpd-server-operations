# For server logging
# Begin timer
task_start <- Sys.time()

# Set necessary variables
dashboard_id <- "PSO"
target_table <- DBI::Id(schema = schema_name, table = dashboard_id)
temp_table <- paste0("#", dashboard_id, "Temp")
api_name <- "Jira"
script_name <- "Jira_PSO"

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
