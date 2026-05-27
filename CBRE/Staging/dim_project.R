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
source(here::here("./utilities/R/api_helpers.R"))
source(here::here("./utilities/R/event_logger.R"))
source(here::here("./utilities/R/sql_helper_functions.R"))

# Set necessary variables
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
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
etl_window <- get_etl_window()
API_NAME <- "CBRE"
SCRIPT_NAME <- "dim_project"

# Connect to SQL database
con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = SQL_SERVER,
  database = DB_NAME,
  Trusted_Connection = "Yes"
)

# Query API
raw_data <- call_cbre_api(
  CBRE_TABLE_NAME,
  start_time = etl_window$start_time,
  end_time = etl_window$end_time
)

if (is.null(raw_data$data) || nrow(raw_data$data) == 0) {
  cat(
    "No data returned from API for window",
    etl_window$start_time,
    "to",
    etl_window$end_time,
    "— nothing to load. Exiting gracefully.\n"
  )
  stop("No new data from API")
}

clean_data <- raw_data |>
  purrr::pluck("data") |>
  # # comment out these after initial data analysis as risk of
  # # losing columns in small data loads
  # select_if(~ !all(is.na(.))) |>
  # select_if(~ !all(. == 0)) |>
  # select_if(~ !all(. == '-1')) |>
  # select_if(~ !all(. == "N/A")) |>
  # select_if(~ !all(. == "-")) |>
  mutate(
    across(
      c(
        project_created_date,
        csf_createddatetime,
        csf_modifieddatetime,
        csf_edp_last_updated_timestamp,
        source_created_ts,
        edp_update_ts
      ),
      ~ as.POSIXct(.x, format = "%Y-%m-%dT%H:%M:%OSZ")
    )
  ) |>
  mutate(
    across(
      c(
        csf_totalforecastfinalcost,
        csf_previouslyreportedcurrentfyytdvowcomplete,
        csf_workcomplete,
        csf_ytdworkcomplete,
        csf_rpdlaborrecoverypercent
      ),
      as.double
    )
  ) |>
  mutate(
    across(
      c(
        project_skey
      ),
      as.character
    )
  ) |>
  mutate(RefreshDate = as.POSIXct(Sys.time())) |>
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
    csf_projadminnotes = csf_projectadministrationnotes,
    csf_rpdlaborrecoverypercent,
    csf_scopehealthcomments,
    record_type,
    csf_scopehealth,
    csf_clientbillingstatus
  )

# dbRemoveTable(con, TARGET_TABLE)

if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- paste0(
    "CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    TABLE_NAME,
    " (
          RefreshDate                   DATETIME2(3)   NOT NULL,
          project_skey                  NVARCHAR(20)   NOT NULL,
          project_id                    INT            NOT NULL,
          project_number                NVARCHAR(20)   NULL,
          project_name                  NVARCHAR(200)  NULL,
          client_project_number         NVARCHAR(200)  NULL,
          client_project_status         NVARCHAR(200)  NULL,
          project_created_date          DATETIME2(3)   NULL,
          source_created_ts             DATETIME2(3)   NULL,
          source_unique_id              NVARCHAR(20)   NULL,
          edp_update_ts                 DATETIME2(3)   NULL,
          source_partition_id           INT            NULL,
          sub_industry_type             NVARCHAR(100)  NULL,
          risk_status                   NVARCHAR(20)   NULL,
          schedule_health               NVARCHAR(20)   NULL,
          budget_health                 NVARCHAR(20)   NULL,
          overall_project_health        NVARCHAR(20)   NULL,
          client_industry               NVARCHAR(50)   NULL,
          cbre_organization             NVARCHAR(20)   NULL,
          cbre_operating_model          NVARCHAR(20)   NULL,
          is_active                     NVARCHAR(20)   NULL,
          risks_assumptions_constraints NVARCHAR(MAX)  NULL,
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
          csf_totalforecastfinalcost    DECIMAL(18,2)  NULL,
          csf_agreementnumber           NVARCHAR(100)  NULL,
          byp_online_client_approval    NVARCHAR(20)   NULL,
          csf_migratedprojectid         NVARCHAR(200)  NULL,
          csf_clientrecoverable         BIT            NULL,
          csf_branchchildorg            NVARCHAR(100)  NULL,
          csf_proposeduseagreement      NVARCHAR(20)   NULL,
          csf_modifieddatetime          DATETIME2(3)   NULL,
          csf_id                        INT            NULL,
          csf_prevreportedfyytdvowcomp  DECIMAL(18,2)  NULL,
          csf_workcomplete              DECIMAL(18,2)  NULL,
          csf_fundingsource             NVARCHAR(30)   NULL,
          csf_estimatetype              NVARCHAR(30)   NULL,
          int_engage_letter_executed    NVARCHAR(20)   NULL,
          csf_last_updated              DATETIME2(3)   NULL,
          csf_ministryparentorg         NVARCHAR(200)  NULL,
          csf_transaction_flag          NVARCHAR(20)   NULL,
          csf_fundingtype               NVARCHAR(50)   NULL,
          csf_chargeaction              NVARCHAR(20)   NULL,
          csf_pmosource                 NVARCHAR(50)   NULL,
          csf_programtype               NVARCHAR(70)   NULL,
          csf_domainpartitionid         INT            NULL,
          csf_servicetype               NVARCHAR(70)   NULL,
          csf_leaseid                   NVARCHAR(50)   NULL,
          csf_projectsubtype            NVARCHAR(20)   NULL,
          csf_complexity                NVARCHAR(30)   NULL,
          csf_identifiedproject         BIT            NULL,
          csf_aresnumber                NVARCHAR(30)   NULL,
          csf_ytdworkcomplete           DECIMAL(18,2)  NULL,
          csf_wsiregion                 NVARCHAR(100)  NULL,
          csf_projecttype               NVARCHAR(25)   NULL,
          standard_project_type         NVARCHAR(50)   NULL,
          csf_createddatetime           DATETIME2(3)   NULL,
          qualify_for_fusion_f          NVARCHAR(20)   NULL,
          fusion_dir_response_status    NVARCHAR(50)   NULL,
          fusion_project_type           NVARCHAR(50)   NULL,
          budget_comments               NVARCHAR(MAX)  NULL,
          risk_comments                 NVARCHAR(MAX)  NULL,
          schedule_comments             NVARCHAR(MAX)  NULL,
          csf_projadminnotes            NVARCHAR(MAX)  NULL,
          csf_rpdlaborrecoverypercent   DECIMAL(18,2)  NULL,
          csf_scopehealthcomments       NVARCHAR(MAX)  NULL,
          record_type                   NVARCHAR(20)   NULL,
          csf_scopehealth               NVARCHAR(20)   NULL,
          csf_clientbillingstatus       NVARCHAR(50)   NULL
      );"
  )

  dbExecute(con, sql)
}

# Database Transaction ####

etl_start_time <- Sys.time()

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
          RefreshDate                   DATETIME2(3)   NOT NULL,
          project_skey                  NVARCHAR(20)   NOT NULL,
          project_id                    INT            NOT NULL,
          project_number                NVARCHAR(20)   NULL,
          project_name                  NVARCHAR(200)  NULL,
          client_project_number         NVARCHAR(200)  NULL,
          client_project_status         NVARCHAR(200)  NULL,
          project_created_date          DATETIME2(3)   NULL,
          source_created_ts             DATETIME2(3)   NULL,
          source_unique_id              NVARCHAR(20)   NULL,
          edp_update_ts                 DATETIME2(3)   NULL,
          source_partition_id           INT            NULL,
          sub_industry_type             NVARCHAR(100)  NULL,
          risk_status                   NVARCHAR(20)   NULL,
          schedule_health               NVARCHAR(20)   NULL,
          budget_health                 NVARCHAR(20)   NULL,
          overall_project_health        NVARCHAR(20)   NULL,
          client_industry               NVARCHAR(50)   NULL,
          cbre_organization             NVARCHAR(20)   NULL,
          cbre_operating_model          NVARCHAR(20)   NULL,
          is_active                     NVARCHAR(20)   NULL,
          risks_assumptions_constraints NVARCHAR(MAX)  NULL,
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
          csf_totalforecastfinalcost    DECIMAL(18,2)  NULL,
          csf_agreementnumber           NVARCHAR(100)  NULL,
          byp_online_client_approval    NVARCHAR(20)   NULL,
          csf_migratedprojectid         NVARCHAR(200)  NULL,
          csf_clientrecoverable         BIT            NULL,
          csf_branchchildorg            NVARCHAR(100)  NULL,
          csf_proposeduseagreement      NVARCHAR(20)   NULL,
          csf_modifieddatetime          DATETIME2(3)   NULL,
          csf_id                        INT            NULL,
          csf_prevreportedfyytdvowcomp  DECIMAL(18,2)  NULL,
          csf_workcomplete              DECIMAL(18,2)  NULL,
          csf_fundingsource             NVARCHAR(30)   NULL,
          csf_estimatetype              NVARCHAR(30)   NULL,
          int_engage_letter_executed    NVARCHAR(20)   NULL,
          csf_last_updated              DATETIME2(3)   NULL,
          csf_ministryparentorg         NVARCHAR(200)  NULL,
          csf_transaction_flag          NVARCHAR(20)   NULL,
          csf_fundingtype               NVARCHAR(50)   NULL,
          csf_chargeaction              NVARCHAR(20)   NULL,
          csf_pmosource                 NVARCHAR(50)   NULL,
          csf_programtype               NVARCHAR(70)   NULL,
          csf_domainpartitionid         INT            NULL,
          csf_servicetype               NVARCHAR(70)   NULL,
          csf_leaseid                   NVARCHAR(50)   NULL,
          csf_projectsubtype            NVARCHAR(20)   NULL,
          csf_complexity                NVARCHAR(30)   NULL,
          csf_identifiedproject         BIT            NULL,
          csf_aresnumber                NVARCHAR(30)   NULL,
          csf_ytdworkcomplete           DECIMAL(18,2)  NULL,
          csf_wsiregion                 NVARCHAR(100)  NULL,
          csf_projecttype               NVARCHAR(25)   NULL,
          standard_project_type         NVARCHAR(50)   NULL,
          csf_createddatetime           DATETIME2(3)   NULL,
          qualify_for_fusion_f          NVARCHAR(20)   NULL,
          fusion_dir_response_status    NVARCHAR(50)   NULL,
          fusion_project_type           NVARCHAR(50)   NULL,
          budget_comments               NVARCHAR(MAX)  NULL,
          risk_comments                 NVARCHAR(MAX)  NULL,
          schedule_comments             NVARCHAR(MAX)  NULL,
          csf_projadminnotes            NVARCHAR(MAX)  NULL,
          csf_rpdlaborrecoverypercent   DECIMAL(18,2)  NULL,
          csf_scopehealthcomments       NVARCHAR(MAX)  NULL,
          record_type                   NVARCHAR(20)   NULL,
          csf_scopehealth               NVARCHAR(20)   NULL,
          csf_clientbillingstatus       NVARCHAR(50)   NULL
          );
  "
      )
    )

    # Write the current tibble into the temp table
    dbWriteTable(
      con,
      name = TEMP_TABLE,
      value = clean_data,
      append = TRUE,
      overwrite = FALSE
    )

    # -- Guard: catch duplicate keys in source data before touching target --
    dup_count <- dbGetQuery(
      con,
      paste0(
        "SELECT COUNT(*) AS n
         FROM (
           SELECT project_skey
           FROM ",
        TEMP_TABLE,
        "
           GROUP BY project_skey
           HAVING COUNT(*) > 1
         ) dupes;"
      )
    )$n

    if (dup_count > 0) {
      stop(paste0(
        "Duplicate project_skey values detected in source data (",
        dup_count,
        " keys affected). Rolling back."
      ))
    }

    # Update existing rows in the target table that have changed
    n_updated <- dbExecute(
      con,
      paste0(
        "
    UPDATE tgt
      SET
        tgt.RefreshDate                   = src.RefreshDate,
        tgt.project_id                    = src.project_id,
        tgt.project_number                = src.project_number,
        tgt.project_name                  = src.project_name,
        tgt.client_project_number         = src.client_project_number,
        tgt.client_project_status         = src.client_project_status,
        tgt.project_created_date          = src.project_created_date,
        tgt.source_created_ts             = src.source_created_ts,
        tgt.source_unique_id              = src.source_unique_id,
        tgt.edp_update_ts                 = src.edp_update_ts,
        tgt.source_partition_id           = src.source_partition_id,
        tgt.sub_industry_type             = src.sub_industry_type,
        tgt.risk_status                   = src.risk_status,
        tgt.schedule_health               = src.schedule_health,
        tgt.budget_health                 = src.budget_health,
        tgt.overall_project_health        = src.overall_project_health,
        tgt.client_industry               = src.client_industry,
        tgt.cbre_organization             = src.cbre_organization,
        tgt.cbre_operating_model          = src.cbre_operating_model,
        tgt.is_active                     = src.is_active,
        tgt.risks_assumptions_constraints = src.risks_assumptions_constraints,
        tgt.project_comment               = src.project_comment,
        tgt.project_type                  = src.project_type,
        tgt.scope_desc                    = src.scope_desc,
        tgt.delivery_account              = src.delivery_account,
        tgt.project_quality               = src.project_quality,
        tgt.proj_mngmnt_deliverables      = src.proj_mngmnt_deliverables,
        tgt.project_decision_framework    = src.project_decision_framework,
        tgt.communications                = src.communications,
        tgt.core_project_team             = src.core_project_team,
        tgt.project_schedule              = src.project_schedule,
        tgt.project_budget                = src.project_budget,
        tgt.project_scope                 = src.project_scope,
        tgt.project_objective             = src.project_objective,
        tgt.csf_pjmfeepercentage          = src.csf_pjmfeepercentage,
        tgt.csf_partitionid               = src.csf_partitionid,
        tgt.charter_approved_offline_f    = src.charter_approved_offline_f,
        tgt.csf_totalforecastfinalcost    = src.csf_totalforecastfinalcost,
        tgt.csf_agreementnumber           = src.csf_agreementnumber,
        tgt.byp_online_client_approval    = src.byp_online_client_approval,
        tgt.csf_migratedprojectid         = src.csf_migratedprojectid,
        tgt.csf_clientrecoverable         = src.csf_clientrecoverable,
        tgt.csf_branchchildorg            = src.csf_branchchildorg,
        tgt.csf_proposeduseagreement      = src.csf_proposeduseagreement,
        tgt.csf_modifieddatetime          = src.csf_modifieddatetime,
        tgt.csf_id                        = src.csf_id,
        tgt.csf_prevreportedfyytdvowcomp  = src.csf_prevreportedfyytdvowcomp,
        tgt.csf_workcomplete              = src.csf_workcomplete,
        tgt.csf_fundingsource             = src.csf_fundingsource,
        tgt.csf_estimatetype              = src.csf_estimatetype,
        tgt.int_engage_letter_executed    = src.int_engage_letter_executed,
        tgt.csf_last_updated              = src.csf_last_updated,
        tgt.csf_ministryparentorg         = src.csf_ministryparentorg,
        tgt.csf_transaction_flag          = src.csf_transaction_flag,
        tgt.csf_fundingtype               = src.csf_fundingtype,
        tgt.csf_chargeaction              = src.csf_chargeaction,
        tgt.csf_pmosource                 = src.csf_pmosource,
        tgt.csf_programtype               = src.csf_programtype,
        tgt.csf_domainpartitionid         = src.csf_domainpartitionid,
        tgt.csf_servicetype               = src.csf_servicetype,
        tgt.csf_leaseid                   = src.csf_leaseid,
        tgt.csf_projectsubtype            = src.csf_projectsubtype,
        tgt.csf_complexity                = src.csf_complexity,
        tgt.csf_identifiedproject         = src.csf_identifiedproject,
        tgt.csf_aresnumber                = src.csf_aresnumber,
        tgt.csf_ytdworkcomplete           = src.csf_ytdworkcomplete,
        tgt.csf_wsiregion                 = src.csf_wsiregion,
        tgt.csf_projecttype               = src.csf_projecttype,
        tgt.standard_project_type         = src.standard_project_type,
        tgt.csf_createddatetime           = src.csf_createddatetime,
        tgt.qualify_for_fusion_f          = src.qualify_for_fusion_f,
        tgt.fusion_dir_response_status    = src.fusion_dir_response_status,
        tgt.fusion_project_type           = src.fusion_project_type,
        tgt.budget_comments               = src.budget_comments,
        tgt.risk_comments                 = src.risk_comments,
        tgt.schedule_comments             = src.schedule_comments,
        tgt.csf_projadminnotes            = src.csf_projadminnotes,
        tgt.csf_rpdlaborrecoverypercent   = src.csf_rpdlaborrecoverypercent,
        tgt.csf_scopehealthcomments       = src.csf_scopehealthcomments,
        tgt.record_type                   = src.record_type,
        tgt.csf_scopehealth               = src.csf_scopehealth,
        tgt.csf_clientbillingstatus       = src.csf_clientbillingstatus
      FROM ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
      JOIN ",
        TEMP_TABLE,
        " src
        ON tgt.project_skey = src.project_skey;
      "
      )
    )

    # Insert data into the SQL table
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
        proj_mngmnt_deliverables,
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
        byp_online_client_approval,
        csf_migratedprojectid,
        csf_clientrecoverable,
        csf_branchchildorg,
        csf_proposeduseagreement,
        csf_modifieddatetime,
        csf_id,
        csf_prevreportedfyytdvowcomp,
        csf_workcomplete,
        csf_fundingsource,
        csf_estimatetype,
        int_engage_letter_executed,
        csf_last_updated,
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
        fusion_dir_response_status,
        fusion_project_type,
        budget_comments,
        risk_comments,
        schedule_comments,
        csf_projadminnotes,
        csf_rpdlaborrecoverypercent,
        csf_scopehealthcomments,
        record_type,
        csf_scopehealth,
        csf_clientbillingstatus
    )
    SELECT
        src.RefreshDate,
        src.project_skey,
        src.project_id,
        src.project_number,
        src.project_name,
        src.client_project_number,
        src.client_project_status,
        src.project_created_date,
        src.source_created_ts,
        src.source_unique_id,
        src.edp_update_ts,
        src.source_partition_id,
        src.sub_industry_type,
        src.risk_status,
        src.schedule_health,
        src.budget_health,
        src.overall_project_health,
        src.client_industry,
        src.cbre_organization,
        src.cbre_operating_model,
        src.is_active,
        src.risks_assumptions_constraints,
        src.project_comment,
        src.project_type,
        src.scope_desc,
        src.delivery_account,
        src.project_quality,
        src.proj_mngmnt_deliverables,
        src.project_decision_framework,
        src.communications,
        src.core_project_team,
        src.project_schedule,
        src.project_budget,
        src.project_scope,
        src.project_objective,
        src.csf_pjmfeepercentage,
        src.csf_partitionid,
        src.charter_approved_offline_f,
        src.csf_totalforecastfinalcost,
        src.csf_agreementnumber,
        src.byp_online_client_approval,
        src.csf_migratedprojectid,
        src.csf_clientrecoverable,
        src.csf_branchchildorg,
        src.csf_proposeduseagreement,
        src.csf_modifieddatetime,
        src.csf_id,
        src.csf_prevreportedfyytdvowcomp,
        src.csf_workcomplete,
        src.csf_fundingsource,
        src.csf_estimatetype,
        src.int_engage_letter_executed,
        src.csf_last_updated,
        src.csf_ministryparentorg,
        src.csf_transaction_flag,
        src.csf_fundingtype,
        src.csf_chargeaction,
        src.csf_pmosource,
        src.csf_programtype,
        src.csf_domainpartitionid,
        src.csf_servicetype,
        src.csf_leaseid,
        src.csf_projectsubtype,
        src.csf_complexity,
        src.csf_identifiedproject,
        src.csf_aresnumber,
        src.csf_ytdworkcomplete,
        src.csf_wsiregion,
        src.csf_projecttype,
        src.standard_project_type,
        src.csf_createddatetime,
        src.qualify_for_fusion_f,
        src.fusion_dir_response_status,
        src.fusion_project_type,
        src.budget_comments,
        src.risk_comments,
        src.schedule_comments,
        src.csf_projadminnotes,
        src.csf_rpdlaborrecoverypercent,
        src.csf_scopehealthcomments,
        src.record_type,
        src.csf_scopehealth,
        src.csf_clientbillingstatus
    FROM ",
        TEMP_TABLE,
        " AS src
    LEFT JOIN ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " AS tgt
      ON tgt.project_skey = src.project_skey
      WHERE tgt.project_skey IS NULL;
  "
      )
    )

    # Complete the transaction
    dbCommit(con)

    # Hoist counts to outer scope for logging
    n_updated <<- n_updated
    n_inserted <<- n_inserted

    cat("ETL complete — updated:", n_updated, "| inserted:", n_inserted, "\n")
  },
  error = function(e) {
    dbRollback(con)
    etl_error <<- e
  }
)

if (is.null(etl_error)) {
  log_daily_etl_run(
    api_name = API_NAME,
    script_name = SCRIPT_NAME,
    table_name = TABLE_NAME,
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
    table_name = TABLE_NAME,
    status = "FAILURE",
    message = substr(etl_error$message, 1, 500)
  )
  stop(etl_error)
}
