# Load libraries
library(assertthat, quietly = TRUE, warn.conflicts = FALSE)
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

# Set necessary variables
ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "CbreStaging"
TABLE_NAME <- "pjm_dim_project"
CBRE_TABLE_NAME <- "pjm_dim_project_vw"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
API_NAME <- "CBRE"
SCRIPT_NAME <- "pjm_dim_project"

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
  mutate(RefreshDate = as.POSIXct(Sys.time())) |>
  mutate(
    across(
      c(
        project_skey
      ),
      as.character
    )
  ) |>
  mutate(
    across(
      c(
        csf_ytdworkcomplete,
        csf_workcomplete,
        csf_totalforecastfinalcost,
        csf_previouslyreportedcurrentfyytdvowcomplete,
        csf_rpdlaborrecoverypercent
      ),
      as.double
    )
  ) |>
  mutate(
    across(
      c(
        csf_modifieddatetime,
        csf_createddatetime,
        source_created_ts,
        project_created_date,
        edp_update_ts
      ),
      ~ as.POSIXct(.x, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC")
    )
  ) |>
  select(
    RefreshDate,
    project_skey,
    project_number,
    project_id,
    csf_ytdworkcomplete,
    csf_wsiregion,
    csf_workcomplete,
    csf_transaction_flag,
    csf_totalforecastfinalcost,
    csf_servicetype,
    csf_proposeduseagreement,
    csf_projecttype,
    csf_projectsubtype,
    csf_programtype,
    csf_prevcurrfyytdvowcomp = csf_previouslyreportedcurrentfyytdvowcomplete,
    csf_pmosource,
    csf_pjmfeepercentage,
    csf_partitionid,
    csf_modifieddatetime,
    csf_ministryparentorg,
    csf_migratedprojectid,
    csf_leaseid,
    csf_identifiedproject,
    csf_id,
    csf_fundingtype,
    csf_fundingsource,
    csf_estimatetype,
    csf_domainpartitionid,
    csf_createddatetime,
    csf_complexity,
    csf_clientrecoverable,
    csf_chargeaction,
    csf_branchchildorg,
    csf_aresnumber,
    csf_agreementnumber,
    charter_approved_offline_f,
    byp_online_client_apprv_f = bypass_online_client_approval_f,
    int_engagement_letter_ex_f = internal_engagement_letter_executed_f,
    risks_assumpt_constraints = risks_assumptions_constraints,
    is_active,
    client_project_status,
    source_client_name,
    source_created_ts,
    source_unique_id,
    source_partition_id,
    project_created_date,
    sub_industry_type,
    risk_status,
    schedule_health,
    budget_health,
    overall_project_health,
    client_industry,
    client_project_number,
    cbre_organization,
    cbre_operating_model,
    project_comment,
    project_type,
    project_currency,
    scope_desc,
    project_number,
    project_name,
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
    standard_project_type,
    qualify_for_fusion_f,
    fusion_dir_response_status = fusion_director_response_status,
    fusion_project_type,
    risk_comments,
    schedule_comments,
    budget_comments,
    csf_scopehealthcomments,
    csf_projadminnotes = csf_projectadministrationnotes,
    csf_rpdlaborrecoverypercent,
    csf_scopehealth,
    csf_clientbillingstatus,
    record_type,
    fusion_enabled_f,
    csf_fyclosed,
    csf_capplanningreference = csf_capitalplanningreference,
    csf_revenuerecovery,
    csf_accountingnotes,
    csf_psrapplicable,
    csf_erprojectcategory,
    csf_parentprojectnumber,
    edp_update_ts
  )

# dbRemoveTable(con, TARGET_TABLE)

if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- paste0(
    "CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    TABLE_NAME,
    " (
      RefreshDate                 DATETIME2(3)   NOT NULL,
      project_skey                NVARCHAR(20)   NOT NULL,
      project_number              NVARCHAR(20)   NULL,
      project_id                  INT            NULL,
      csf_ytdworkcomplete         DECIMAL(18,4)  NULL,
      csf_wsiregion               NVARCHAR(60)   NULL,
      csf_workcomplete            NVARCHAR(30)   NULL,
      csf_transaction_flag        NVARCHAR(5)    NULL,
      csf_totalforecastfinalcost  DECIMAL(18,4)  NULL,
      csf_source_system           NVARCHAR(15)   NULL,
      csf_servicetype             NVARCHAR(60)   NULL,
      csf_proposeduseagreement    NVARCHAR(15)   NULL,
      csf_projecttype             NVARCHAR(30)   NULL,
      csf_projectsubtype          NVARCHAR(20)   NULL,
      csf_programtype             NVARCHAR(60)   NULL,
      csf_prevcurrfyytdvowcomp    DECIMAL(18,4)  NULL,
      csf_pmosource               NVARCHAR(30)   NULL,
      csf_pjmfeepercentage        NVARCHAR(10)   NULL,
      csf_partitionid             INT            NULL,
      csf_modifieddatetime        DATETIME2(3)   NULL,
      csf_ministryparentorg       NVARCHAR(90)   NULL,
      csf_migratedprojectid       NVARCHAR(140)  NULL,
      csf_leaseid                 NVARCHAR(40)   NULL,
      csf_identifiedproject       BIT            NULL,
      csf_id                      INT            NULL,
      csf_fundingtype             NVARCHAR(25)   NULL,
      csf_fundingsource           NVARCHAR(15)   NULL,
      csf_estimatetype            NVARCHAR(20)   NULL,
      csf_domainpartitionid       INT            NULL,
      csf_createddatetime         DATETIME2(3)   NULL,
      csf_complexity              NVARCHAR(25)   NULL,
      csf_clientrecoverable       BIT            NULL,
      csf_chargeaction            NVARCHAR(10)   NULL,
      csf_branchchildorg          NVARCHAR(80)   NULL,
      csf_aresnumber              NVARCHAR(40)   NULL,
      csf_agreementnumber         NVARCHAR(75)   NULL,
      charter_approved_offline_f  NVARCHAR(1)    NULL,
      byp_online_client_apprv_f   NVARCHAR(1)    NULL,
      int_engagement_letter_ex_f  NVARCHAR(1)    NULL,
      risks_assumpt_constraints   NVARCHAR(MAX)  NULL,
      is_active                   NVARCHAR(1)    NULL,
      client_project_status       NVARCHAR(90)   NULL,
      source_client_name          NVARCHAR(25)   NULL,
      source_created_ts           DATETIME2(3)   NULL,
      source_unique_id            NVARCHAR(20)   NULL,
      source_partition_id         INT            NULL,
      project_created_date        DATETIME2(3)   NULL,
      sub_industry_type           NVARCHAR(60)   NULL,
      risk_status                 NVARCHAR(15)   NULL,
      schedule_health             NVARCHAR(15)   NULL,
      budget_health               NVARCHAR(15)   NULL,
      overall_project_health      NVARCHAR(15)   NULL,
      client_industry             NVARCHAR(40)   NULL,
      client_project_number       NVARCHAR(30)   NULL,
      cbre_organization           NVARCHAR(20)   NULL,
      cbre_operating_model        NVARCHAR(20)   NULL,
      project_comment             NVARCHAR(MAX)  NULL,
      project_type                NVARCHAR(30)   NULL,
      project_currency            NVARCHAR(10)   NULL,
      scope_desc                  NVARCHAR(MAX)  NULL,
      project_name                NVARCHAR(200)  NULL,
      delivery_account            NVARCHAR(80)   NULL,
      project_quality             NVARCHAR(200)  NULL,
      proj_mngmnt_deliverables    NVARCHAR(250)  NULL,
      project_decision_framework  NVARCHAR(220)  NULL,
      communications              NVARCHAR(450)  NULL,
      core_project_team           NVARCHAR(MAX)  NULL,
      project_schedule            NVARCHAR(600)  NULL,
      project_budget              NVARCHAR(600)  NULL,
      project_scope               NVARCHAR(MAX)  NULL,
      project_objective           NVARCHAR(MAX)  NULL,
      standard_project_type       NVARCHAR(50)   NULL,
      qualify_for_fusion_f        NVARCHAR(1)    NULL,
      fusion_dir_response_status  NVARCHAR(25)   NULL,
      fusion_project_type         NVARCHAR(50)   NULL,
      risk_comments               NVARCHAR(MAX)  NULL,
      schedule_comments           NVARCHAR(MAX)  NULL,
      budget_comments             NVARCHAR(MAX)  NULL,
      csf_scopehealthcomments     NVARCHAR(MAX)  NULL,
      csf_projadminnotes          NVARCHAR(MAX)  NULL,
      csf_rpdlaborrecoverypercent DECIMAL(18,4)  NULL,
      csf_scopehealth             NVARCHAR(15)   NULL,
      csf_clientbillingstatus     NVARCHAR(40)   NULL,
      record_type                 NVARCHAR(15)   NULL,
      fusion_enabled_f            NVARCHAR(2)    NULL,
      csf_fyclosed                NVARCHAR(15)   NULL,
      csf_capplanningreference    NVARCHAR(700)  NULL,
      csf_revenuerecovery         BIT            NULL,
      csf_accountingnotes         NVARCHAR(40)   NULL,
      csf_psrapplicable           NVARCHAR(10)   NULL,
      csf_erprojectcategory       NVARCHAR(50)   NULL,
      csf_parentprojectnumber     NVARCHAR(30)   NULL,
      edp_update_ts               DATETIME2(3)   NULL
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

    dbExecute(
      con,
      paste0(
        "CREATE TABLE ",
        TEMP_TABLE,
        " (
        RefreshDate                 DATETIME2(3)   NOT NULL,
        project_skey                NVARCHAR(20)   NOT NULL,
        project_number              NVARCHAR(20)   NULL,
        project_id                  INT            NULL,
        csf_ytdworkcomplete         DECIMAL(18,4)  NULL,
        csf_wsiregion               NVARCHAR(60)   NULL,
        csf_workcomplete            NVARCHAR(30)   NULL,
        csf_transaction_flag        NVARCHAR(5)    NULL,
        csf_totalforecastfinalcost  DECIMAL(18,4)  NULL,
        csf_source_system           NVARCHAR(15)   NULL,
        csf_servicetype             NVARCHAR(60)   NULL,
        csf_proposeduseagreement    NVARCHAR(15)   NULL,
        csf_projecttype             NVARCHAR(30)   NULL,
        csf_projectsubtype          NVARCHAR(20)   NULL,
        csf_programtype             NVARCHAR(60)   NULL,
        csf_prevcurrfyytdvowcomp    DECIMAL(18,4)  NULL,
        csf_pmosource               NVARCHAR(30)   NULL,
        csf_pjmfeepercentage        NVARCHAR(10)   NULL,
        csf_partitionid             INT            NULL,
        csf_modifieddatetime        DATETIME2(3)   NULL,
        csf_ministryparentorg       NVARCHAR(90)   NULL,
        csf_migratedprojectid       NVARCHAR(140)  NULL,
        csf_leaseid                 NVARCHAR(40)   NULL,
        csf_identifiedproject       BIT            NULL,
        csf_id                      INT            NULL,
        csf_fundingtype             NVARCHAR(25)   NULL,
        csf_fundingsource           NVARCHAR(15)   NULL,
        csf_estimatetype            NVARCHAR(20)   NULL,
        csf_domainpartitionid       INT            NULL,
        csf_createddatetime         DATETIME2(3)   NULL,
        csf_complexity              NVARCHAR(25)   NULL,
        csf_clientrecoverable       BIT            NULL,
        csf_chargeaction            NVARCHAR(10)   NULL,
        csf_branchchildorg          NVARCHAR(80)   NULL,
        csf_aresnumber              NVARCHAR(40)   NULL,
        csf_agreementnumber         NVARCHAR(75)   NULL,
        charter_approved_offline_f  NVARCHAR(1)    NULL,
        byp_online_client_apprv_f   NVARCHAR(1)    NULL,
        int_engagement_letter_ex_f  NVARCHAR(1)    NULL,
        risks_assumpt_constraints   NVARCHAR(MAX)  NULL,
        is_active                   NVARCHAR(1)    NULL,
        client_project_status       NVARCHAR(90)   NULL,
        source_client_name          NVARCHAR(25)   NULL,
        source_created_ts           DATETIME2(3)   NULL,
        source_unique_id            NVARCHAR(20)   NULL,
        source_partition_id         INT            NULL,
        project_created_date        DATETIME2(3)   NULL,
        sub_industry_type           NVARCHAR(60)   NULL,
        risk_status                 NVARCHAR(15)   NULL,
        schedule_health             NVARCHAR(15)   NULL,
        budget_health               NVARCHAR(15)   NULL,
        overall_project_health      NVARCHAR(15)   NULL,
        client_industry             NVARCHAR(40)   NULL,
        client_project_number       NVARCHAR(30)   NULL,
        cbre_organization           NVARCHAR(20)   NULL,
        cbre_operating_model        NVARCHAR(20)   NULL,
        project_comment             NVARCHAR(MAX)  NULL,
        project_type                NVARCHAR(30)   NULL,
        project_currency            NVARCHAR(10)   NULL,
        scope_desc                  NVARCHAR(MAX)  NULL,
        project_name                NVARCHAR(200)  NULL,
        delivery_account            NVARCHAR(80)   NULL,
        project_quality             NVARCHAR(200)  NULL,
        proj_mngmnt_deliverables    NVARCHAR(250)  NULL,
        project_decision_framework  NVARCHAR(220)  NULL,
        communications              NVARCHAR(450)  NULL,
        core_project_team           NVARCHAR(MAX)  NULL,
        project_schedule            NVARCHAR(600)  NULL,
        project_budget              NVARCHAR(600)  NULL,
        project_scope               NVARCHAR(MAX)  NULL,
        project_objective           NVARCHAR(MAX)  NULL,
        standard_project_type       NVARCHAR(50)   NULL,
        qualify_for_fusion_f        NVARCHAR(1)    NULL,
        fusion_dir_response_status  NVARCHAR(25)   NULL,
        fusion_project_type         NVARCHAR(50)   NULL,
        risk_comments               NVARCHAR(MAX)  NULL,
        schedule_comments           NVARCHAR(MAX)  NULL,
        budget_comments             NVARCHAR(MAX)  NULL,
        csf_scopehealthcomments     NVARCHAR(MAX)  NULL,
        csf_projadminnotes          NVARCHAR(MAX)  NULL,
        csf_rpdlaborrecoverypercent DECIMAL(18,4)  NULL,
        csf_scopehealth             NVARCHAR(15)   NULL,
        csf_clientbillingstatus     NVARCHAR(40)   NULL,
        record_type                 NVARCHAR(15)   NULL,
        fusion_enabled_f            NVARCHAR(2)    NULL,
        csf_fyclosed                NVARCHAR(15)   NULL,
        csf_capplanningreference    NVARCHAR(700)  NULL,
        csf_revenuerecovery         BIT            NULL,
        csf_accountingnotes         NVARCHAR(40)   NULL,
        csf_psrapplicable           NVARCHAR(10)   NULL,
        csf_erprojectcategory       NVARCHAR(50)   NULL,
        csf_parentprojectnumber     NVARCHAR(30)   NULL,
        edp_update_ts               DATETIME2(3)   NULL
      );"
      )
    )

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
        tgt.RefreshDate                 = src.RefreshDate,
        tgt.project_number              = src.project_number,
        tgt.project_id                  = src.project_id,
        tgt.csf_ytdworkcomplete         = src.csf_ytdworkcomplete,
        tgt.csf_wsiregion               = src.csf_wsiregion,
        tgt.csf_workcomplete            = src.csf_workcomplete,
        tgt.csf_transaction_flag        = src.csf_transaction_flag,
        tgt.csf_totalforecastfinalcost  = src.csf_totalforecastfinalcost,
        tgt.csf_source_system           = src.csf_source_system,
        tgt.csf_servicetype             = src.csf_servicetype,
        tgt.csf_proposeduseagreement    = src.csf_proposeduseagreement,
        tgt.csf_projecttype             = src.csf_projecttype,
        tgt.csf_projectsubtype          = src.csf_projectsubtype,
        tgt.csf_programtype             = src.csf_programtype,
        tgt.csf_prevcurrfyytdvowcomp    = src.csf_prevcurrfyytdvowcomp,
        tgt.csf_pmosource               = src.csf_pmosource,
        tgt.csf_pjmfeepercentage        = src.csf_pjmfeepercentage,
        tgt.csf_partitionid             = src.csf_partitionid,
        tgt.csf_modifieddatetime        = src.csf_modifieddatetime,
        tgt.csf_ministryparentorg       = src.csf_ministryparentorg,
        tgt.csf_migratedprojectid       = src.csf_migratedprojectid,
        tgt.csf_leaseid                 = src.csf_leaseid,
        tgt.csf_identifiedproject       = src.csf_identifiedproject,
        tgt.csf_id                      = src.csf_id,
        tgt.csf_fundingtype             = src.csf_fundingtype,
        tgt.csf_fundingsource           = src.csf_fundingsource,
        tgt.csf_estimatetype            = src.csf_estimatetype,
        tgt.csf_domainpartitionid       = src.csf_domainpartitionid,
        tgt.csf_createddatetime         = src.csf_createddatetime,
        tgt.csf_complexity              = src.csf_complexity,
        tgt.csf_clientrecoverable       = src.csf_clientrecoverable,
        tgt.csf_chargeaction            = src.csf_chargeaction,
        tgt.csf_branchchildorg          = src.csf_branchchildorg,
        tgt.csf_aresnumber              = src.csf_aresnumber,
        tgt.csf_agreementnumber         = src.csf_agreementnumber,
        tgt.charter_approved_offline_f  = src.charter_approved_offline_f,
        tgt.byp_online_client_apprv_f   = src.byp_online_client_apprv_f,
        tgt.int_engagement_letter_ex_f  = src.int_engagement_letter_ex_f,
        tgt.risks_assumpt_constraints   = src.risks_assumpt_constraints,
        tgt.is_active                   = src.is_active,
        tgt.client_project_status       = src.client_project_status,
        tgt.source_client_name          = src.source_client_name,
        tgt.source_created_ts           = src.source_created_ts,
        tgt.source_unique_id            = src.source_unique_id,
        tgt.source_partition_id         = src.source_partition_id,
        tgt.project_created_date        = src.project_created_date,
        tgt.sub_industry_type            = src.sub_industry_type,
        tgt.risk_status                 = src.risk_status,
        tgt.schedule_health             = src.schedule_health,
        tgt.budget_health               = src.budget_health,
        tgt.overall_project_health      = src.overall_project_health,
        tgt.client_industry             = src.client_industry,
        tgt.client_project_number       = src.client_project_number,
        tgt.cbre_organization           = src.cbre_organization,
        tgt.cbre_operating_model        = src.cbre_operating_model,
        tgt.project_comment             = src.project_comment,
        tgt.project_type                = src.project_type,
        tgt.project_currency            = src.project_currency,
        tgt.scope_desc                  = src.scope_desc,
        tgt.project_name                = src.project_name,
        tgt.delivery_account            = src.delivery_account,
        tgt.project_quality             = src.project_quality,
        tgt.proj_mngmnt_deliverables    = src.proj_mngmnt_deliverables,
        tgt.project_decision_framework  = src.project_decision_framework,
        tgt.communications              = src.communications,
        tgt.core_project_team           = src.core_project_team,
        tgt.project_schedule            = src.project_schedule,
        tgt.project_budget              = src.project_budget,
        tgt.project_scope               = src.project_scope,
        tgt.project_objective           = src.project_objective,
        tgt.standard_project_type       = src.standard_project_type,
        tgt.qualify_for_fusion_f        = src.qualify_for_fusion_f,
        tgt.fusion_dir_response_status  = src.fusion_dir_response_status,
        tgt.fusion_project_type         = src.fusion_project_type,
        tgt.risk_comments               = src.risk_comments,
        tgt.schedule_comments           = src.schedule_comments,
        tgt.budget_comments             = src.budget_comments,
        tgt.csf_scopehealthcomments     = src.csf_scopehealthcomments,
        tgt.csf_projadminnotes          = src.csf_projadminnotes,
        tgt.csf_rpdlaborrecoverypercent = src.csf_rpdlaborrecoverypercent,
        tgt.csf_scopehealth             = src.csf_scopehealth,
        tgt.csf_clientbillingstatus     = src.csf_clientbillingstatus,
        tgt.record_type                 = src.record_type,
        tgt.fusion_enabled_f            = src.fusion_enabled_f,
        tgt.csf_fyclosed                = src.csf_fyclosed,
        tgt.csf_capplanningreference    = src.csf_capplanningreference,
        tgt.csf_revenuerecovery         = src.csf_revenuerecovery,
        tgt.csf_accountingnotes         = src.csf_accountingnotes,
        tgt.csf_psrapplicable           = src.csf_psrapplicable,
        tgt.csf_erprojectcategory       = src.csf_erprojectcategory,
        tgt.csf_parentprojectnumber     = src.csf_parentprojectnumber,
        tgt.edp_update_ts               = src.edp_update_ts
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

    # Insert new rows that don't exist in the SQL table
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
      project_number,
      project_id,
      csf_ytdworkcomplete,
      csf_wsiregion,
      csf_workcomplete,
      csf_transaction_flag,
      csf_totalforecastfinalcost,
      csf_source_system,
      csf_servicetype,
      csf_proposeduseagreement,
      csf_projecttype,
      csf_projectsubtype,
      csf_programtype,
      csf_prevcurrfyytdvowcomp,
      csf_pmosource,
      csf_pjmfeepercentage,
      csf_partitionid,
      csf_modifieddatetime,
      csf_ministryparentorg,
      csf_migratedprojectid,
      csf_leaseid,
      csf_identifiedproject,
      csf_id,
      csf_fundingtype,
      csf_fundingsource,
      csf_estimatetype,
      csf_domainpartitionid,
      csf_createddatetime,
      csf_complexity,
      csf_clientrecoverable,
      csf_chargeaction,
      csf_branchchildorg,
      csf_aresnumber,
      csf_agreementnumber,
      charter_approved_offline_f,
      byp_online_client_apprv_f,
      int_engagement_letter_ex_f,
      risks_assumpt_constraints,
      is_active,
      client_project_status,
      source_client_name,
      source_created_ts,
      source_unique_id,
      source_partition_id,
      project_created_date,
      sub_industry_type,
      risk_status,
      schedule_health,
      budget_health,
      overall_project_health,
      client_industry,
      client_project_number,
      cbre_organization,
      cbre_operating_model,
      project_comment,
      project_type,
      project_currency,
      scope_desc,
      project_name,
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
      standard_project_type,
      qualify_for_fusion_f,
      fusion_dir_response_status,
      fusion_project_type,
      risk_comments,
      schedule_comments,
      budget_comments,
      csf_scopehealthcomments,
      csf_projadminnotes,
      csf_rpdlaborrecoverypercent,
      csf_scopehealth,
      csf_clientbillingstatus,
      record_type,
      fusion_enabled_f,
      csf_fyclosed,
      csf_capplanningreference,
      csf_revenuerecovery,
      csf_accountingnotes,
      csf_psrapplicable,
      csf_erprojectcategory,
      csf_parentprojectnumber,
      edp_update_ts
    )
    SELECT
      src.RefreshDate,
      src.project_skey,
      src.project_number,
      src.project_id,
      src.csf_ytdworkcomplete,
      src.csf_wsiregion,
      src.csf_workcomplete,
      src.csf_transaction_flag,
      src.csf_totalforecastfinalcost,
      src.csf_source_system,
      src.csf_servicetype,
      src.csf_proposeduseagreement,
      src.csf_projecttype,
      src.csf_projectsubtype,
      src.csf_programtype,
      src.csf_prevcurrfyytdvowcomp,
      src.csf_pmosource,
      src.csf_pjmfeepercentage,
      src.csf_partitionid,
      src.csf_modifieddatetime,
      src.csf_ministryparentorg,
      src.csf_migratedprojectid,
      src.csf_leaseid,
      src.csf_identifiedproject,
      src.csf_id,
      src.csf_fundingtype,
      src.csf_fundingsource,
      src.csf_estimatetype,
      src.csf_domainpartitionid,
      src.csf_createddatetime,
      src.csf_complexity,
      src.csf_clientrecoverable,
      src.csf_chargeaction,
      src.csf_branchchildorg,
      src.csf_aresnumber,
      src.csf_agreementnumber,
      src.charter_approved_offline_f,
      src.byp_online_client_apprv_f,
      src.int_engagement_letter_ex_f,
      src.risks_assumpt_constraints,
      src.is_active,
      src.client_project_status,
      src.source_client_name,
      src.source_created_ts,
      src.source_unique_id,
      src.source_partition_id,
      src.project_created_date,
      src.sub_industry_type,
      src.risk_status,
      src.schedule_health,
      src.budget_health,
      src.overall_project_health,
      src.client_industry,
      src.client_project_number,
      src.cbre_organization,
      src.cbre_operating_model,
      src.project_comment,
      src.project_type,
      src.project_currency,
      src.scope_desc,
      src.project_name,
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
      src.standard_project_type,
      src.qualify_for_fusion_f,
      src.fusion_dir_response_status,
      src.fusion_project_type,
      src.risk_comments,
      src.schedule_comments,
      src.budget_comments,
      src.csf_scopehealthcomments,
      src.csf_projadminnotes,
      src.csf_rpdlaborrecoverypercent,
      src.csf_scopehealth,
      src.csf_clientbillingstatus,
      src.record_type,
      src.fusion_enabled_f,
      src.csf_fyclosed,
      src.csf_capplanningreference,
      src.csf_revenuerecovery,
      src.csf_accountingnotes,
      src.csf_psrapplicable,
      src.csf_erprojectcategory,
      src.csf_parentprojectnumber,
      src.edp_update_ts
    FROM ",
        TEMP_TABLE,
        " src
    LEFT JOIN ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
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
