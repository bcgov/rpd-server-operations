ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "CbreStaging"
TABLE_NAME <- "dim_project"
CBRE_TABLE_NAME <- "dim_project_vw"

# Load libraries
library(base64enc, quietly = TRUE, warn.conflicts = FALSE)
library(dplyr, quietly = TRUE, warn.conflicts = FALSE)
library(here, quietly = TRUE, warn.conflicts = FALSE)
library(httr2, quietly = TRUE, warn.conflicts = FALSE)
library(jsonlite, quietly = TRUE, warn.conflicts = FALSE)
library(lubridate, quietly = TRUE, warn.conflicts = FALSE)
library(purrr, quietly = TRUE, warn.conflicts = FALSE)
library(tibble, quietly = TRUE, warn.conflicts = FALSE)
library(tidyr, quietly = TRUE, warn.conflicts = FALSE)

library(odbc, quietly = TRUE, warn.conflicts = FALSE)
library(DBI, quietly = TRUE, warn.conflicts = FALSE)

# Load helper functions
source(here::here("./utilities/R/cbre_api_function.R"))
source(here::here("./utilities/R/event_logger.R"))
source(here::here("./utilities/R/sql_helper_functions.R"))

# Connect to SQL database
con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = SQL_SERVER,
  database = DB_NAME,
  Trusted_Connection = "Yes"
)

target_table <- Id(schema = SCHEMA_NAME, table = TABLE_NAME)
temp_table <- paste0("#", TABLE_NAME, "Temp")

raw_data <- extract_cbre_data(CBRE_TABLE_NAME)

cleaned_data <- raw_data |>
  select_if(~ !all(is.na(.))) |>
  select_if(~ !all(. == 0)) |>
  select_if(~ !all(. == '-1')) |>
  select_if(~ !all(. == "N/A")) |>
  select_if(~ !all(. == "-")) |>
  mutate(
    across(
      c(
        project_created_date,
        csf_createddatetime,
        csf_modifieddatetime,
        source_created_ts,
        edp_update_ts
      ),
      ~ as.POSIXct(.x, format = "%Y-%m-%dT%H:%M:%OSZ")
    )
  ) |>
  mutate(RefreshDate = as.POSIXct(Sys.Date())) |>
  select(
    RefreshDate,
    project_skey,
    project_id,
    project_number,
    project_name,
    client_project_number,
    client_project_status,
    project_created_date,
    source_created_ts,
    source_unique_id,
    edp_update_ts,
    source_partition_id,
    sub_industry_type,
    risk_status,
    schedule_health,
    budget_health,
    overall_project_health,
    client_industry,
    cbre_organization,
    cbre_operating_model,
    is_active,
    risks_assumptions_constraints,
    project_comment,
    project_type,
    scope_desc,
    delivery_account,
    project_quality,
    proj_mngmnt_deliverables = project_management_deliverables,
    project_decision_framework,
    communications,
    core_project_team,
    project_schedule,
    project_budget,
    project_scope,
    project_objective,
    csf_pjmfeepercentage,
    csf_partitionid,
    charter_approved_offline_f,
    csf_totalforecastfinalcost,
    csf_agreementnumber,
    byp_online_client_approval = bypass_online_client_approval_f,
    csf_migratedprojectid,
    csf_clientrecoverable,
    csf_branchchildorg,
    csf_proposeduseagreement,
    csf_modifieddatetime,
    csf_id,
    csf_prevreportedfyytdvowcomp = csf_previouslyreportedcurrentfyytdvowcomplete,
    csf_workcomplete,
    csf_fundingsource,
    csf_estimatetype,
    int_engage_letter_executed = internal_engagement_letter_executed_f,
    csf_last_updated = csf_edp_last_updated_timestamp,
    csf_ministryparentorg,
    csf_transaction_flag,
    csf_fundingtype,
    csf_chargeaction,
    csf_pmosource,
    csf_programtype,
    csf_domainpartitionid,
    csf_servicetype,
    csf_leaseid,
    csf_projectsubtype,
    csf_complexity,
    csf_identifiedproject,
    csf_aresnumber,
    csf_ytdworkcomplete,
    csf_wsiregion,
    csf_projecttype,
    standard_project_type,
    csf_createddatetime,
    qualify_for_fusion_f,
    fusion_dir_response_status = fusion_director_response_status,
    fusion_project_type,
    budget_comments,
    risk_comments,
    schedule_comments,
    csf_projectadministrationnotes,
    csf_rpdlaborrecoverypercent,
    csf_scopehealthcomments,
    record_type,
    csf_scopehealth,
    csf_clientbillingstatus
  )


if (!dbExistsTable(con, target_table)) {
  sql <- paste0(
    "CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    TABLE_NAME,
    " (
        RefreshDate                   DATETIME2(3)   NOT NULL,
        project_skey                  INT            NOT NULL,
        project_id                    INT            NOT NULL,
        project_number                NVARCHAR(20)   NULL,
        project_name                  NVARCHAR(200)  NULL,
        client_project_number         NVARCHAR(200)  NULL,
        client_project_status         NVARCHAR(200)  NULL,
        project_created_date          DATETIME2(3)   NULL,
        source_created_ts             DATETIME2(3)   NULL,
        source_unique_id              NVARCHAR(20)   NULL,
        edp_update_ts                 DATETIME2(3)   NULL,
        source_partition_id           NVARCHAR(20)   NULL,
        sub_industry_type             NVARCHAR(100)  NULL,
        risk_status                   NVARCHAR(20)   NULL,
        schedule_health               NVARCHAR(20)   NULL,
        budget_health                 NVARCHAR(20)   NULL,
        overall_project_health        NVARCHAR(20)   NULL,
        client_industry               NVARCHAR(50)   NULL,
        cbre_organization             NVARCHAR(20)   NULL,
        cbre_operating_model          NVARCHAR(20)   NULL,
        is_active                     NVARCHAR(20)   NULL,
        risks_assumptions_constraints NVARCHAR(750)  NULL,
        project_comment               NVARCHAR(MAX)  NULL,
        project_type                  NVARCHAR(30)   NULL,
        scope_desc                    NVARCHAR(MAX)  NULL,
        delivery_account              NVARCHAR(100)  NULL,
        project_quality               NVARCHAR(200)  NULL,
        proj_mngmnt_deliverables      NVARCHAR(300)  NULL,
        project_decision_framework    NVARCHAR(300)  NULL,
        communications                NVARCHAR(500)  NULL,
        core_project_team             NVARCHAR(MAX)  NULL,
        project_schedule              NVARCHAR(500)  NULL,
        project_budget                NVARCHAR(500)  NULL,
        project_scope                 NVARCHAR(MAX)  NULL,
        project_objective             NVARCHAR(MAX)  NULL,
        csf_pjmfeepercentage          NVARCHAR(20)   NULL,
        csf_partitionid               INT            NULL,
        charter_approved_offline_f    NVARCHAR(20)   NULL,
        csf_totalforecastfinalcost    NVARCHAR(20)   NULL,
        csf_agreementnumber           NVARCHAR(100)  NULL,
        byp_online_client_approval    NVARCHAR(20)   NULL,
        csf_migratedprojectid         NVARCHAR(20)   NULL,
        csf_clientrecoverable         NVARCHAR(20)   NULL,
        csf_branchchildorg            NVARCHAR(20)   NULL,
        csf_proposeduseagreement      NVARCHAR(20)   NULL,
        csf_modifieddatetime          DATETIME2(3)   NULL,
        csf_id                        NVARCHAR(20)   NULL,
        csf_prevreportedfyytdvowcomp  NVARCHAR(30)   NULL,
        csf_workcomplete              NVARCHAR(30)   NULL,
        csf_fundingsource             NVARCHAR(30)   NULL,
        csf_estimatetype              NVARCHAR(30)   NULL,
        int_engage_letter_executed    NVARCHAR(20)   NULL,
        csf_last_updated              DATETIME2(3)   NULL,
        csf_ministryparentorg         NVARCHAR(200)  NULL,
        csf_transaction_flag          NVARCHAR(20)   NULL,
        csf_fundingtype               NVARCHAR(50)   NULL,
        csf_chargeaction              NVARCHAR(20)   NULL,
        csf_pmosource                 NVARCHAR(50)   NULL,
        csf_programtype               NVARCHAR(50)   NULL,
        csf_domainpartitionid         INT            NULL,
        csf_servicetype               NVARCHAR(70)   NULL,
        csf_leaseid                   NVARCHAR(50)   NULL,
        csf_projectsubtype            NVARCHAR(20)   NULL,
        csf_complexity                NVARCHAR(30)   NULL,
        csf_identifiedproject         BIT            NULL,
        csf_aresnumber                NVARCHAR(30)   NULL,
        csf_ytdworkcomplete           NVARCHAR(20)   NULL,
        csf_wsiregion                 NVARCHAR(20)   NULL,
        csf_projecttype               NVARCHAR(20)   NULL,
        standard_project_type         NVARCHAR(20)   NULL,
        csf_createddatetime           DATETIME2(3)   NULL,
        qualify_for_fusion_f          NVARCHAR(20)   NULL,
        fusion_dir_response_status    NVARCHAR(20)   NULL,
        fusion_project_type           NVARCHAR(20)   NULL,
        budget_comments               NVARCHAR(20)   NULL,
        risk_comments                 NVARCHAR(20)   NULL,
        schedule_comments             NVARCHAR(20)   NULL,
        csf_projadminnotes            NVARCHAR(20)   NULL,
        csf_rpdlaborrecoverypercent   NVARCHAR(20)   NULL,
        csf_scopehealthcomments       NVARCHAR(20)   NULL,
        record_type                   NVARCHAR(20)   NULL,
        csf_scopehealth               NVARCHAR(20)   NULL,
        csf_clientbillingstatus       NVARCHAR(20)   NULL
      );"
  )

  dbExecute(con, sql)
}

# Database Transaction ####
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
          RefreshDate               DATETIME2(3)  NOT NULL,
          budget_skey               NVARCHAR(10)  NOT NULL,
          budget_id                 NVARCHAR(10)  NOT NULL,
          budget_number             NVARCHAR(5)   NOT NULL,
          budget_type               NVARCHAR(100) NULL,
          budget_subject            NVARCHAR(100) NULL,
          budget_approval_status    NVARCHAR(20)  NULL,
          budget_date               DATETIME2(3)  NULL,
          budget_submitted_date     DATETIME2(3)  NULL,
          authorized_date           DATETIME2(3)  NULL,
          source_unique_id          NVARCHAR(10)  NOT NULL,
          source_partition_id       NVARCHAR(10)  NOT NULL,
          source_modified_ts        DATETIME2(3)  NULL,
          edp_update_ts             DATETIME2(3)  NULL,
          edp_create_ts             DATETIME2(3)  NULL
  );
  "
      )
    )

    # Write the current tibble into the temp table
    dbWriteTable(
      con,
      name = temp_table,
      value = cleaned_data,
      append = TRUE,
      overwrite = FALSE
    )

    # Update existing rows in the target table
    n_updated <- dbExecute(
      con,
      paste0(
        "
    UPDATE tgt
    SET
        tgt.RefreshDate             = src.RefreshDate,
        tgt.budget_skey             = src.budget_skey,
        tgt.budget_id               = src.budget_id,
        tgt.budget_number           = src.budget_number,
        tgt.budget_type             = src.budget_type,
        tgt.budget_subject          = src.budget_subject,
        tgt.budget_approval_status  = src.budget_approval_status,
        tgt.budget_date             = src.budget_date,
        tgt.budget_submitted_date   = src.budget_submitted_date,
        tgt.authorized_date         = src.authorized_date,
        tgt.source_unique_id        = src.source_unique_id,
        tgt.source_partition_id     = src.source_partition_id,
        tgt.source_modified_ts      = src.source_modified_ts,
        tgt.edp_update_ts           = src.edp_update_ts,
        tgt.edp_create_ts           = src.edp_create_ts
    FROM ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " AS tgt
    JOIN ",
        temp_table,
        " AS src
      ON  tgt.budget_skey = src.budget_skey;
  "
      )
    )

    # Insert new rows not already in the target
    n_inserted <- dbExecute(
      con,
      paste0(
        "
    INSERT INTO ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " (
        RefreshDate,
        budget_skey,
        budget_id,
        budget_number,
        budget_type,
        budget_subject,
        budget_approval_status,
        budget_date,
        budget_submitted_date,
        authorized_date,
        source_unique_id,
        source_partition_id,
        source_modified_ts,
        edp_update_ts,
        edp_create_ts
    )
    SELECT
        src.RefreshDate,
        src.budget_skey,
        src.budget_id,
        src.budget_number,
        src.budget_type,
        src.budget_subject,
        src.budget_approval_status,
        src.budget_date,
        src.budget_submitted_date,
        src.authorized_date,
        src.source_unique_id,
        src.source_partition_id,
        src.source_modified_ts,
        src.edp_update_ts,
        src.edp_create_ts
    FROM ",
        temp_table,
        " AS src
    LEFT JOIN ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " AS tgt
      ON tgt.budget_skey = src.budget_skey
      WHERE tgt.budget_skey IS NULL;
  "
      )
    )

    # Complete the transaction
    dbCommit(con)

    n_inserted <<- n_inserted
    n_updated <<- n_updated
    # Rollback transaction on failure
  },
  error = function(e) {
    dbRollback(con)
    stop(e)
  }
)
