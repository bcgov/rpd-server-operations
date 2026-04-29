ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "CbreStaging"
TABLE_NAME <- "fact_project_activity"
CBRE_TABLE_NAME <- "fact_project_activity_vw"

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

edp_tables <- list(
  # "dim_project_activity_vw",
  # "dim_project_role_vw",
  # "fact_budget_vw",
  # "fact_invoice_vw",
  "fact_project_activity_vw"
)
# ---- submit all exports ----
# jobs <- lapply(edp_tables, function(tbl) {
#   submit_edp_export(
#     edp_table = tbl
#   )
# })

results <- lapply(jobs, function(job) {
  retrieve_edp_export(job$file_id)
})

file_id <- jobs[[2]]$file_id

jobs[[2]]$edp_table
output <- retrieve_edp_export(file_id = file_id)

raw_data <- extract_cbre_data(CBRE_TABLE_NAME)

fact_proj_activity_data <- raw_data

write.csv(fact_proj_activity_data, "C:/Projects/fact_project_activity_vw.csv")
fact_proj_activity_data <- read.csv("C:/Projects/fact_project_activity_vw.csv")

cleaned_data <- fact_proj_activity_data |>
  select_if(~ !all(is.na(.))) |>
  select_if(~ !all(. == 0)) |>
  select_if(~ !all(. == '-1')) |>
  select_if(~ !all(. == "N/A")) |>
  select_if(~ !all(. == "-")) |>
  mutate(RefreshDate = as.POSIXct(Sys.time())) |>
  mutate(
    across(
      c(
        edp_update_ts
      ),
      as.POSIXct
    )
  ) |>
  mutate(
    across(
      c(
        paid,
        retained,
        invoiced,
        budget_adjustments_total_value,
        budget_approved_changes_total_value,
        budget_approved_adjustment_total_value,
        budget_approved_total_value,
        budget_estimated_total_value,
        budget_contract_approved_total_value,
        cost_approved_total_value,
        cost_projected_changes_total_value,
        cost_pending_changes_total_value,
        cost_pending_commitments_total_value,
        cost_approved_changes_total_value,
        cost_original_total_value,
        cost_estimated_total_value,
        balance_to_complete,
        awarded_amount,
        payables_remaining_total_value
      ),
      as.double
    )
  ) |>
  select(
    RefreshDate,
    project_skey,
    project_activity_skey,
    record_type,
    paid,
    retained,
    invoiced,
    budget_adjustments_total_value,
    budget_approved_changes_total_value,
    budget_approved_adjustment_total_value,
    budget_approved_total_value,
    budget_estimated_total_value,
    budget_contract_approved_total_value,
    cost_approved_total_value,
    cost_projected_changes_total_value,
    cost_pending_changes_total_value,
    cost_pending_commitments_total_value,
    cost_approved_changes_total_value,
    cost_original_total_value,
    cost_estimated_total_value,
    balance_to_complete,
    awarded_amount,
    payables_remaining_total_value,
    source_system_code,
    source_unique_id,
    edp_update_ts
  )

# dbRemoveTable(con, target_table)

if (!dbExistsTable(con, target_table)) {
  sql <- paste0(
    "CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    TABLE_NAME,
    " (
      RefreshDate                            DATETIME2(3)   NOT NULL,
      project_skey                           INT            NOT NULL,
      project_activity_skey                  INT            NOT NULL,
      record_type                            NVARCHAR(20)   NULL,
      paid                                   DECIMAL(18,2)  NULL,
      retained                               DECIMAL(18,2)  NULL,
      invoiced                               DECIMAL(18,2)  NULL,
      budget_adjustments_total_value         DECIMAL(18,2)  NULL,
      budget_approved_changes_total_value    DECIMAL(18,2)  NULL,
      budget_approved_adjustment_total_value DECIMAL(18,2)  NULL,
      budget_approved_total_value            DECIMAL(18,2)  NULL,
      budget_estimated_total_value           DECIMAL(18,2)  NULL,
      budget_contract_approved_total_value   DECIMAL(18,2)  NULL,
      cost_approved_total_value              DECIMAL(18,2)  NULL,
      cost_projected_changes_total_value     DECIMAL(18,2)  NULL,
      cost_pending_changes_total_value       DECIMAL(18,2)  NULL,
      cost_pending_commitments_total_value   DECIMAL(18,2)  NULL,
      cost_approved_changes_total_value      DECIMAL(18,2)  NULL,
      cost_original_total_value              DECIMAL(18,2)  NULL,
      cost_estimated_total_value             DECIMAL(18,2)  NULL,
      balance_to_complete                    DECIMAL(18,2)  NULL,
      awarded_amount                         DECIMAL(18,2)  NULL,
      payables_remaining_total_value         DECIMAL(18,2)  NULL,
      source_system_code                     NVARCHAR(50)   NULL,
      source_unique_id                       NVARCHAR(50)   NULL,
      edp_update_ts                          DATETIME2(3)   NULL,

      CONSTRAINT PK_",
    TABLE_NAME,
    " PRIMARY KEY (
        project_skey,
        project_activity_skey
      )
    );"
  )

  dbExecute(con, sql)
}

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
        RefreshDate                            DATETIME2(3)   NOT NULL,
        project_skey                           INT            NOT NULL,
        project_activity_skey                  INT            NOT NULL,
        record_type                            NVARCHAR(20)   NULL,
        paid                                   DECIMAL(18,2)  NULL,
        retained                               DECIMAL(18,2)  NULL,
        invoiced                               DECIMAL(18,2)  NULL,
        budget_adjustments_total_value         DECIMAL(18,2)  NULL,
        budget_approved_changes_total_value    DECIMAL(18,2)  NULL,
        budget_approved_adjustment_total_value DECIMAL(18,2)  NULL,
        budget_approved_total_value            DECIMAL(18,2)  NULL,
        budget_estimated_total_value           DECIMAL(18,2)  NULL,
        budget_contract_approved_total_value   DECIMAL(18,2)  NULL,
        cost_approved_total_value              DECIMAL(18,2)  NULL,
        cost_projected_changes_total_value     DECIMAL(18,2)  NULL,
        cost_pending_changes_total_value       DECIMAL(18,2)  NULL,
        cost_pending_commitments_total_value   DECIMAL(18,2)  NULL,
        cost_approved_changes_total_value      DECIMAL(18,2)  NULL,
        cost_original_total_value              DECIMAL(18,2)  NULL,
        cost_estimated_total_value             DECIMAL(18,2)  NULL,
        balance_to_complete                    DECIMAL(18,2)  NULL,
        awarded_amount                         DECIMAL(18,2)  NULL,
        payables_remaining_total_value         DECIMAL(18,2)  NULL,
        source_system_code                     NVARCHAR(50)   NULL,
        source_unique_id                       NVARCHAR(50)   NULL,
        edp_update_ts                          DATETIME2(3)   NULL
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

    n_updated <- dbExecute(
      con,
      paste0(
        "
      UPDATE tgt
      SET
        tgt.RefreshDate                            = src.RefreshDate,
        tgt.record_type                            = src.record_type,
        tgt.paid                                   = src.paid,
        tgt.retained                               = src.retained,
        tgt.invoiced                               = src.invoiced,
        tgt.budget_adjustments_total_value         = src.budget_adjustments_total_value,
        tgt.budget_approved_changes_total_value    = src.budget_approved_changes_total_value,
        tgt.budget_approved_adjustment_total_value = src.budget_approved_adjustment_total_value,
        tgt.budget_approved_total_value            = src.budget_approved_total_value,
        tgt.budget_estimated_total_value           = src.budget_estimated_total_value,
        tgt.budget_contract_approved_total_value   = src.budget_contract_approved_total_value,
        tgt.cost_approved_total_value              = src.cost_approved_total_value,
        tgt.cost_projected_changes_total_value     = src.cost_projected_changes_total_value,
        tgt.cost_pending_changes_total_value       = src.cost_pending_changes_total_value,
        tgt.cost_pending_commitments_total_value   = src.cost_pending_commitments_total_value,
        tgt.cost_approved_changes_total_value      = src.cost_approved_changes_total_value,
        tgt.cost_original_total_value              = src.cost_original_total_value,
        tgt.cost_estimated_total_value             = src.cost_estimated_total_value,
        tgt.balance_to_complete                    = src.balance_to_complete,
        tgt.awarded_amount                         = src.awarded_amount,
        tgt.payables_remaining_total_value         = src.payables_remaining_total_value,
        tgt.source_system_code                     = src.source_system_code,
        tgt.source_unique_id                       = src.source_unique_id,
        tgt.edp_update_ts                          = src.edp_update_ts
      FROM ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
      JOIN ",
        temp_table,
        " src
        ON  tgt.project_skey          = src.project_skey
        AND tgt.project_activity_skey = src.project_activity_skey;
      "
      )
    )

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
        project_activity_skey,
        record_type,
        paid,
        retained,
        invoiced,
        budget_adjustments_total_value,
        budget_approved_changes_total_value,
        budget_approved_adjustment_total_value,
        budget_approved_total_value,
        budget_estimated_total_value,
        budget_contract_approved_total_value,
        cost_approved_total_value,
        cost_projected_changes_total_value,
        cost_pending_changes_total_value,
        cost_pending_commitments_total_value,
        cost_approved_changes_total_value,
        cost_original_total_value,
        cost_estimated_total_value,
        balance_to_complete,
        awarded_amount,
        payables_remaining_total_value,
        source_system_code,
        source_unique_id,
        edp_update_ts
      )
      SELECT
        src.RefreshDate,
        src.project_skey,
        src.project_activity_skey,
        src.record_type,
        src.paid,
        src.retained,
        src.invoiced,
        src.budget_adjustments_total_value,
        src.budget_approved_changes_total_value,
        src.budget_approved_adjustment_total_value,
        src.budget_approved_total_value,
        src.budget_estimated_total_value,
        src.budget_contract_approved_total_value,
        src.cost_approved_total_value,
        src.cost_projected_changes_total_value,
        src.cost_pending_changes_total_value,
        src.cost_pending_commitments_total_value,
        src.cost_approved_changes_total_value,
        src.cost_original_total_value,
        src.cost_estimated_total_value,
        src.balance_to_complete,
        src.awarded_amount,
        src.payables_remaining_total_value,
        src.source_system_code,
        src.source_unique_id,
        src.edp_update_ts
      FROM ",
        temp_table,
        " src
      LEFT JOIN ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
        ON  tgt.project_skey          = src.project_skey
        AND tgt.project_activity_skey = src.project_activity_skey
      WHERE tgt.project_skey IS NULL;
      "
      )
    )

    dbCommit(con)

    n_inserted <<- n_inserted
    n_updated <<- n_updated
  },
  error = function(e) {
    dbRollback(con)
    stop(e)
  }
)
