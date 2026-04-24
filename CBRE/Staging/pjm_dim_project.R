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
  mutate(RefreshDate = as.POSIXct(Sys.Date())) |>
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
    project_management_deliverables,
    project_decision_framework,
    communications,
    core_project_team,
    project_schedule,
    project_budget,
    project_scope,
    project_objective,
    standard_project_type,
    qualify_for_fusion_f,
    fusion_director_response_status,
    fusion_project_type,
    risk_comments,
    schedule_comments,
    budget_comments,
    csf_scopehealthcomments,
    csf_projectadministrationnotes,
    csf_rpdlaborrecoverypercent,
    csf_scopehealth,
    csf_clientbillingstatus,
    record_type,
    fusion_enabled_f,
    csf_fyclosed,
    csf_capitalplanningreference,
    csf_revenuerecovery,
    csf_accountingnotes,
    csf_psrapplicable,
    csf_erprojectcategory,
    csf_parentprojectnumber
  )

# dbRemoveTable(con, target_table)

if (!dbExistsTable(con, target_table)) {
  sql <- paste0(
    "CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    TABLE_NAME,
    " (
      RefreshDate                 DATETIME2(3)   NOT NULL,
      project_skey                INT NOT NULL,
      project_number              NVARCHAR(20),
      project_id                  INT,
      csf_ytdworkcomplete         NVARCHAR(30),
      csf_wsiregion               NVARCHAR(60),
      csf_workcomplete            NVARCHAR(30),
      csf_transaction_flag        NVARCHAR(5),
      csf_totalforecastfinalcost  NVARCHAR(30),
      csf_source_system           NVARCHAR(15),
      csf_servicetype             NVARCHAR(60),
      csf_proposeduseagreement    NVARCHAR(15),
      csf_projecttype             NVARCHAR(30),
      csf_projectsubtype          NVARCHAR(20),
      csf_programtype             NVARCHAR(60),
      csf_prevcurrfyytdvowcomp    NVARCHAR(30),
      csf_pmosource               NVARCHAR(30),
      csf_pjmfeepercentage        NVARCHAR(10),
      csf_partitionid             INT,
      csf_modifieddatetime        NVARCHAR(30),
      csf_ministryparentorg       NVARCHAR(90),
      csf_migratedprojectid       NVARCHAR(140),
      csf_leaseid                 NVARCHAR(40),
      csf_identifiedproject       BIT,
      csf_id                      INT,
      csf_fundingtype             NVARCHAR(25),
      csf_fundingsource           NVARCHAR(15),
      csf_estimatetype            NVARCHAR(20),
      csf_domainpartitionid       INT,
      csf_createddatetime         NVARCHAR(30),
      csf_complexity              NVARCHAR(25),
      csf_clientrecoverable       BIT,
      csf_chargeaction            NVARCHAR(10),
      csf_branchchildorg          NVARCHAR(80),
      csf_aresnumber              NVARCHAR(40),
      csf_agreementnumber         NVARCHAR(75),
      charter_approved_offline_f  NVARCHAR(1),
      byp_online_client_apprv_f   NVARCHAR(1),
      int_engagement_letter_ex_f  NVARCHAR(1),
      risks_assumpt_constraints   NVARCHAR(1000),

      client_additional_attrib NVARCHAR(MAX),   -- JSON blob (~6k observed)

      is_active NVARCHAR(1),
      client_project_status NVARCHAR(90),
      source_client_name NVARCHAR(25),
      source_created_ts NVARCHAR(30),
      source_unique_id NVARCHAR(20),
      source_partition_id INT,
      project_created_date NVARCHAR(30),
      sub_industry_type NVARCHAR(60),

      risk_status NVARCHAR(15),
      schedule_health NVARCHAR(15),
      budget_health NVARCHAR(15),
      overall_project_health NVARCHAR(15),

      client_industry NVARCHAR(40),
      client_project_number NVARCHAR(30),
      cbre_organization NVARCHAR(20),
      cbre_operating_model NVARCHAR(20),

      project_comment NVARCHAR(MAX),
      project_type NVARCHAR(30),
      project_currency NVARCHAR(10),

      scope_desc NVARCHAR(MAX),
      project_name NVARCHAR(200),
      delivery_account NVARCHAR(80),

      project_quality NVARCHAR(200),
      project_management_deliverables NVARCHAR(250),
      project_decision_framework NVARCHAR(220),
      communications NVARCHAR(450),

      core_project_team NVARCHAR(3000),
      project_schedule NVARCHAR(600),
      project_budget NVARCHAR(600),
      project_scope NVARCHAR(MAX),
      project_objective NVARCHAR(1500),

      standard_project_type NVARCHAR(50),

      qualify_for_fusion_f NVARCHAR(1),
      fusion_director_response_status NVARCHAR(25),
      fusion_project_type NVARCHAR(50),

      risk_comments NVARCHAR(1600),
      schedule_comments NVARCHAR(1600),
      budget_comments NVARCHAR(2200),
      csf_scopehealthcomments NVARCHAR(1500),
      csf_projectadministrationnotes NVARCHAR(2000),

      csf_rpdlaborrecoverypercent NVARCHAR(30),
      csf_scopehealth NVARCHAR(15),
      csf_clientbillingstatus NVARCHAR(40),

      record_type NVARCHAR(15),
      fusion_enabled_f NVARCHAR(2),
      csf_fyclosed NVARCHAR(15),

      csf_capitalplanningreference NVARCHAR(700),
      csf_revenuerecovery BIT,
      csf_accountingnotes NVARCHAR(40),
      csf_psrapplicable NVARCHAR(10),
      csf_erprojectcategory NVARCHAR(50),
      edp_create_ts NVARCHAR(40),
      csf_parentprojectnumber NVARCHAR(30),

  CONSTRAINT PK_Project_Stg PRIMARY KEY (project_skey)

    );"
  )

  dbExecute(con, sql)
}

# Database Transaction ####
# Control database transaction to ensure all steps done together or not at all
dbBegin(con)

tryCatch(
  {
    if (dbExistsTable(con, temp_table)) {
      dbRemoveTable(con, temp_table)
    }

    dbExecute(
      con,
      paste0(
        "CREATE TABLE ",
        temp_table,
        " (
        RefreshDate         DATETIME2(3)   NOT NULL,
        contact_skey        INT            NOT NULL,
        first_name          NVARCHAR(100)  NULL,
        last_name           NVARCHAR(100)  NULL,
        email_id            NVARCHAR(100)  NULL,
        address_line_1      NVARCHAR(100)  NULL,
        address_line_2      NVARCHAR(100)  NULL,
        postal_code         NVARCHAR(100)  NULL,
        state               NVARCHAR(100)  NULL,
        country             NVARCHAR(100)  NULL,
        company_name        NVARCHAR(100)  NULL,
        job_title           NVARCHAR(100)  NULL,
        contact_id          NVARCHAR(100)  NULL,
        source_unique_id    NVARCHAR(100)  NULL
      );"
      )
    )

    dbWriteTable(
      con,
      name = temp_table,
      value = cleaned_data,
      append = TRUE,
      overwrite = FALSE
    )

    # Update existing rows in the target table that have changed

    n_updated <- dbExecute(
      con,
      paste0(
        "
      UPDATE tgt
      SET
        tgt.RefreshDate       = src.RefreshDate,
        tgt.contact_skey      = src.contact_skey,
        tgt.first_name        = src.first_name,
        tgt.last_name         = src.last_name,
        tgt.email_id          = src.email_id,
        tgt.address_line_1    = src.address_line_1,
        tgt.address_line_2    = src.address_line_2,
        tgt.postal_code       = src.postal_code,
        tgt.state             = src.state,
        tgt.country           = src.country,
        tgt.company_name      = src.company_name,
        tgt.job_title         = src.job_title,
        tgt.contact_id        = src.contact_id,
        tgt.source_unique_id  = src.source_unique_id
      FROM ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
      JOIN ",
        temp_table,
        " src
        ON  tgt.contact_skey = src.contact_skey
        WHERE
            ISNULL(tgt.contact_skey, '')     <> ISNULL(src.contact_skey, '');
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
        contact_skey,
        first_name,
        last_name,
        email_id,
        address_line_1,
        address_line_2,
        postal_code,
        state,
        country,
        company_name,
        job_title,
        contact_id,
        source_unique_id
      )
      SELECT
        src.RefreshDate,
        src.contact_skey,
        src.first_name,
        src.last_name,
        src.email_id,
        src.address_line_1,
        src.address_line_2,
        src.postal_code,
        src.state,
        src.country,
        src.company_name,
        src.job_title,
        src.contact_id,
        src.source_unique_id
      FROM ",
        temp_table,
        " src
      LEFT JOIN ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
        ON  tgt.contact_skey = src.contact_skey
      WHERE tgt.contact_skey IS NULL;
      "
      )
    )

    # Delete old rows that don't exist in the SQL table
    n_deleted <- dbExecute(
      con,
      paste0(
        "
      DELETE tgt
        FROM ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
      LEFT JOIN ",
        temp_table,
        " src
        ON  tgt.contact_skey = src.contact_skey
      WHERE tgt.contact_skey IS NULL;
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
