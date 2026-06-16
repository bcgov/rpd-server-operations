# For server logging
# Begin timer
task_start <- Sys.time()

# Setup necessary variables
ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "RealProperty"
TABLE_NAME <- "ActivityCodeReport"
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
SCRIPT_NAME <- "ActivityCodeReport"
API_NAME <- "None"

# Connect to SQL database
con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = SQL_SERVER,
  database = DB_NAME,
  Trusted_Connection = "Yes"
)

# Prep Datasets ####

# ProjectRoleData
query <- dbSendQuery(
  con,
  "SELECT contact_skey, first_name, last_name, email_id
  FROM CbreStaging.pjm_dim_contact"
)
PjmDimContactData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(
  con,
  "SELECT project_role_skey, project_role
   FROM CbreStaging.pjm_dim_project_role"
)
PjmDimProjectRoleData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(
  con,
  "SELECT project_skey, project_role_skey, contact_skey, source_unique_id
  FROM CbreStaging.pjm_fact_project_role"
)
PjmFactProjectRoleData <- dbFetch(query, n = -1)
dbClearResult(query)

ProjectRoleData <- PjmFactProjectRoleData |>
  filter(grepl("[0-9]_project_manager", source_unique_id)) |>
  left_join(PjmDimProjectRoleData, by = join_by(project_role_skey)) |>
  left_join(PjmDimContactData, by = join_by(contact_skey)) |>
  mutate(ProjectManager = paste(first_name, last_name)) |>
  select(project_skey, ProjectManager, ProjectManagerEmail = email_id)

# ProjectData

query <- dbSendQuery(
  con,
  "SELECT project_skey, project_status, project_phase FROM CbreStaging.pjm_fact_project"
)
PjmFactProjectData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(
  con,
  "SELECT project_skey, project_number, project_name FROM CbreStaging.pjm_dim_project"
)
PjmDimProjectData <- dbFetch(query, n = -1)
dbClearResult(query)

ProjectData <- PjmFactProjectData |>
  left_join(PjmDimProjectData, by = join_by(project_skey)) |>
  select(
    project_skey,
    project_status,
    project_phase,
    project_number,
    project_name
  )

# Project Activity Data
query <- dbSendQuery(
  con,
  "SELECT project_activity_skey, code, code_type, code_parent, activity_desc
  FROM CbreStaging.pjm_dim_project_activity"
)
PjmDimProjectActivityData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(
  con,
  "SELECT
    project_skey,
    project_activity_skey,
    paid,
    retained,
    invoiced,
    awarded_amount,
    budget_estimated_total_value,
    budget_approved_changes_total_value,
    budget_approved_adjustment_total_value,
    budget_approved_total_value,
    cost_original_total_value,
    cost_approved_changes_total_value,
    cost_pending_commitments_total_value,
    cost_pending_changes_total_value,
    cost_projected_changes_total_value,
    payables_remaining_total_value
  FROM CbreStaging.pjm_fact_project_activity"
)
PjmFactProjectActivityData <- dbFetch(query, n = -1)
dbClearResult(query)

ProjectActivityData <- PjmFactProjectActivityData |>
  left_join(PjmDimProjectActivityData, by = join_by(project_activity_skey))

# Create ACR Report ####
AcrReport <- ProjectActivityData |>
  left_join(ProjectData, by = join_by(project_skey)) |>
  left_join(ProjectRoleData, by = join_by(project_skey)) |>
  mutate(
    CurrentBudget = round(
      budget_approved_total_value +
        budget_approved_adjustment_total_value +
        budget_approved_changes_total_value,
      digits = 2
    ),
    AnticipatedFinalCost = awarded_amount +
      cost_projected_changes_total_value,
    Variance = round(AnticipatedFinalCost - CurrentBudget, digits = 2),
    VariancePercent = round((Variance / CurrentBudget) * 100, digits = 2),
    code_sibling = stringr::str_sub(code, start = 5, end = 6),
    code_cousin = stringr::str_sub(code, start = 7, end = 9)
  ) |>
  mutate(
    VariancePercent = case_when(
      is.infinite(VariancePercent) ~ NA,
      .default = VariancePercent
    )
  ) |>
  select(
    ProjectSkey = project_skey,
    ProjectNumber = project_number,
    ProjectName = project_name,
    ProjectStatus = project_status,
    ProjectManager,
    ProjectManagerEmail,
    ParentCode = code_parent,
    code_sibling,
    code_cousin,
    ActivityCode = code,
    CodeType = code_type,
    Description = activity_desc,
    PreliminaryBudget = budget_estimated_total_value,
    ApprovedBudget = budget_approved_total_value,
    Adjustments = budget_approved_adjustment_total_value,
    ApprovedBudgetChanges = budget_approved_changes_total_value,
    CurrentBudget,
    OriginalCommitted = cost_original_total_value,
    ApprovedChanges = cost_approved_changes_total_value,
    CurrentCommitted = awarded_amount,
    PendingCommitments = cost_pending_commitments_total_value,
    PendingChanges = cost_pending_changes_total_value,
    ProjectedExposure = cost_projected_changes_total_value,
    AnticipatedFinalCost,
    Variance,
    VariancePercent,
    Invoiced = invoiced,
    Retained = retained,
    Paid = paid,
    Remaining = payables_remaining_total_value
  ) |>
  filter(CodeType == "detail") |>
  group_by(ProjectSkey, ParentCode, code_sibling) |>
  mutate(count = n()) |>
  filter(!(count >= 2 & code_cousin == "")) |>
  ungroup() |>
  filter(
    !if_all(
      c(
        PreliminaryBudget,
        CurrentBudget,
        CurrentCommitted,
        Invoiced,
        Retained,
        Paid,
        Remaining
      ),
      ~ .x == 0
    )
  ) |>
  arrange(
    ProjectNumber,
    ActivityCode
  ) |>
  select(
    -c(
      code_sibling,
      code_cousin,
      count
    )
  ) |>
  mutate(RefreshDate = Sys.time(), .before = everything())


# Create target table if it doesn't exist ####
if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- paste0(
    "CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    TABLE_NAME,
    " (
      RefreshDate             DATETIME2(3)    NOT NULL,
      ProjectSkey             NVARCHAR(50)    NOT NULL,
      ProjectNumber           NVARCHAR(50)    NULL,
      ProjectName             NVARCHAR(255)   NULL,
      ProjectStatus           NVARCHAR(100)   NULL,
      ProjectManager          NVARCHAR(255)   NULL,
      ProjectManagerEmail     NVARCHAR(255)   NULL,
      ParentCode              NVARCHAR(50)    NULL,
      ActivityCode            NVARCHAR(50)    NULL,
      CodeType                NVARCHAR(50)    NULL,
      Description             NVARCHAR(255)   NULL,
      PreliminaryBudget       DECIMAL(18,2)   NULL,
      ApprovedBudget          DECIMAL(18,2)   NULL,
      Adjustments             DECIMAL(18,2)   NULL,
      ApprovedBudgetChanges   DECIMAL(18,2)   NULL,
      CurrentBudget           DECIMAL(18,2)   NULL,
      OriginalCommitted       DECIMAL(18,2)   NULL,
      ApprovedChanges         DECIMAL(18,2)   NULL,
      CurrentCommitted        DECIMAL(18,2)   NULL,
      PendingCommitments      DECIMAL(18,2)   NULL,
      PendingChanges          DECIMAL(18,2)   NULL,
      ProjectedExposure       DECIMAL(18,2)   NULL,
      AnticipatedFinalCost    DECIMAL(18,2)   NULL,
      Variance                DECIMAL(18,2)   NULL,
      VariancePercent         DECIMAL(18,2)   NULL,
      Invoiced                DECIMAL(18,2)   NULL,
      Retained                DECIMAL(18,2)   NULL,
      Paid                    DECIMAL(18,2)   NULL,
      Remaining               DECIMAL(18,2)   NULL
    );"
  )
  dbExecute(con, sql)
}

# Database Transaction ####
etl_error <- NULL

dbBegin(con)

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
        SCHEMA_NAME,
        ".",
        TEMP_TABLE,
        " (
          RefreshDate             DATETIME2(3)    NOT NULL,
          ProjectSkey             NVARCHAR(50)    NOT NULL,
          ProjectNumber           NVARCHAR(50)    NULL,
          ProjectName             NVARCHAR(255)   NULL,
          ProjectStatus           NVARCHAR(100)   NULL,
          ProjectManager          NVARCHAR(255)   NULL,
          ProjectManagerEmail     NVARCHAR(255)   NULL,
          ParentCode              NVARCHAR(50)    NULL,
          ActivityCode            NVARCHAR(50)    NULL,
          CodeType                NVARCHAR(50)    NULL,
          Description             NVARCHAR(255)   NULL,
          PreliminaryBudget       DECIMAL(18,2)   NULL,
          ApprovedBudget          DECIMAL(18,2)   NULL,
          Adjustments             DECIMAL(18,2)   NULL,
          ApprovedBudgetChanges   DECIMAL(18,2)   NULL,
          CurrentBudget           DECIMAL(18,2)   NULL,
          OriginalCommitted       DECIMAL(18,2)   NULL,
          ApprovedChanges         DECIMAL(18,2)   NULL,
          CurrentCommitted        DECIMAL(18,2)   NULL,
          PendingCommitments      DECIMAL(18,2)   NULL,
          PendingChanges          DECIMAL(18,2)   NULL,
          ProjectedExposure       DECIMAL(18,2)   NULL,
          AnticipatedFinalCost    DECIMAL(18,2)   NULL,
          Variance                DECIMAL(18,2)   NULL,
          VariancePercent         DECIMAL(18,2)   NULL,
          Invoiced                DECIMAL(18,2)   NULL,
          Retained                DECIMAL(18,2)   NULL,
          Paid                    DECIMAL(18,2)   NULL,
          Remaining               DECIMAL(18,2)   NULL
        );"
      )
    )

    dbWriteTable(
      con,
      name = TEMP_TABLE,
      value = AcrReport,
      append = TRUE,
      overwrite = FALSE
    )

    dbExecute(
      con,
      paste0("DELETE FROM ", SCHEMA_NAME, ".", TABLE_NAME, ";")
    )

    n_inserted <- dbExecute(
      con,
      paste0(
        "INSERT INTO ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " (
          RefreshDate,
          ProjectSkey,
          ProjectNumber,
          ProjectName,
          ProjectStatus,
          ProjectManager,
          ProjectManagerEmail,
          ParentCode,
          ActivityCode,
          CodeType,
          Description,
          PreliminaryBudget,
          ApprovedBudget,
          Adjustments,
          ApprovedBudgetChanges,
          CurrentBudget,
          OriginalCommitted,
          ApprovedChanges,
          CurrentCommitted,
          PendingCommitments,
          PendingChanges,
          ProjectedExposure,
          AnticipatedFinalCost,
          Variance,
          VariancePercent,
          Invoiced,
          Retained,
          Paid,
          Remaining
        )
        SELECT * FROM ",
        TEMP_TABLE,
        ";"
      )
    )

    dbCommit(con)
    n_inserted <<- n_inserted
  },
  error = function(e) {
    dbRollback(con)
    stop(e)
  }
)

task_end <- Sys.time()
task_duration <- interval(task_start, task_end) / dseconds()

if (is.null(etl_error)) {
  log_daily_etl_run(
    api_name = API_NAME,
    script_name = SCRIPT_NAME,
    table_name = TABLE_NAME,
    duration = task_duration,
    status = "SUCCESS",
    n_inserted = n_inserted,
    n_updated = NA,
    n_deleted = NA,
    message = "ETL completed successfully"
  )
} else {
  log_daily_etl_run(
    api_name = API_NAME,
    script_name = SCRIPT_NAME,
    table_name = TABLE_NAME,
    status = "FAILURE",
    message = substr(etl_error$message, 1, 500)
  )
  stop(etl_error)
}
