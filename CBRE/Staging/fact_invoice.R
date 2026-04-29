ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "CbreStaging"
TABLE_NAME <- "fact_invoice"
CBRE_TABLE_NAME <- "fact_invoice_vw"

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
  "fact_invoice_vw",
  # "fact_project_activity_vw"
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

raw_data <- read.csv("C:/Projects/fact_invoice_vw.csv")

cleaned_data <- raw_data |>
  select_if(~ !all(is.na(.))) |>
  select_if(~ !all(. == 0)) |>
  select_if(~ !all(. == '-1')) |>
  select_if(~ !all(. == "N/A")) |>
  select_if(~ !all(. == "-")) |>
  mutate(RefreshDate = as.POSIXct(Sys.time())) |>
  mutate(
    across(
      c(
        source_modified_ts,
        source_created_ts,
        edp_update_ts
      ),
      ~ as.POSIXct(.x, format = "%Y-%m-%d %H:%M:%S %z") # "2025-11-05 05:17:54 +0000"
    )
  ) |>
  mutate(
    across(
      c(
        work_retainage,
        work_completed_to_date
      ),
      as.double
    )
  ) |>
  select(
    RefreshDate,
    project_skey,
    project_activity_skey,
    invoice_skey,
    invoice_item_skey,
    change_order_skey,
    change_order_item_skey,
    record_type,
    invoice_status,
    invoice_desc,
    invoice_item_id,
    contract_skey,
    contract_line_skey,
    to_contact_skey,
    from_contact_skey,
    to_company_skey,
    from_company_skey,
    change_order_item_id,
    line_number,
    work_completed_this_period,
    work_completed_to_date,
    work_retainage,
    work_retainage_percent,
    payables_billed_total,
    payables_withheld_total,
    payables_remitted_total,
    balance_to_finish,
    balance_to_finish_with_retainage,
    previous_total_earned,
    previous_work_completed,
    total_to_date,
    total_earned_to_date,
    total_to_date_percent,
    total_retainage,
    scheduled_value,
    current_payment_due,
    source_unique_id,
    source_partition_id,
    source_system_code,
    source_created_ts,
    source_modified_ts,
    edp_update_ts
  )

if (!dbExistsTable(con, target_table)) {
  sql <- paste0(
    "CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    TABLE_NAME,
    " (
      RefreshDate                      DATETIME2(3)   NOT NULL,
      project_skey                     INT            NOT NULL,
      project_activity_skey            INT            NULL,
      invoice_skey                     INT            NOT NULL,
      invoice_item_skey                INT            NULL,
      change_order_skey                INT            NULL,
      change_order_item_skey           INT            NULL,
      record_type                      NVARCHAR(20)   NULL,
      invoice_status                   NVARCHAR(20)   NULL,
      invoice_desc                     NVARCHAR(200)  NULL,
      invoice_item_id                  INT            NULL,
      contract_skey                    INT            NULL,
      contract_line_skey               INT            NULL,
      to_contact_skey                  INT            NULL,
      from_contact_skey                INT            NULL,
      to_company_skey                  INT            NULL,
      from_company_skey                INT            NULL,
      change_order_item_id             INT            NULL,
      line_number                      INT            NULL,
      work_completed_this_period       DECIMAL(18,2)  NULL,
      work_completed_to_date           DECIMAL(18,2)  NULL,
      work_retainage                   DECIMAL(18,2)  NULL,
      work_retainage_percent           DECIMAL(18,2)  NULL,
      payables_billed_total            DECIMAL(18,2)  NULL,
      payables_withheld_total          DECIMAL(18,2)  NULL,
      payables_remitted_total          DECIMAL(18,2)  NULL,
      balance_to_finish                DECIMAL(18,2)  NULL,
      balance_to_finish_with_retainage DECIMAL(18,2)  NULL,
      previous_total_earned            DECIMAL(18,2)  NULL,
      previous_work_completed          DECIMAL(18,2)  NULL,
      total_to_date                    DECIMAL(18,2)  NULL,
      total_earned_to_date             DECIMAL(18,2)  NULL,
      total_to_date_percent            DECIMAL(18,2)  NULL,
      total_retainage                  DECIMAL(18,2)  NULL,
      scheduled_value                  DECIMAL(18,2)  NULL,
      current_payment_due              DECIMAL(18,2)  NULL,
      source_unique_id                 INT            NULL,
      source_partition_id              INT            NULL,
      source_system_code               NVARCHAR(50)   NULL,
      source_created_ts                DATETIME2(3)   NULL,
      source_modified_ts               DATETIME2(3)   NULL,
      edp_update_ts                    DATETIME2(3)   NULL,

      CONSTRAINT PK_",
    TABLE_NAME,
    " PRIMARY KEY (
        project_skey,
        invoice_skey,
        invoice_item_skey
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
        RefreshDate                      DATETIME2(3)    NOT NULL,
        project_skey                     INT             NOT NULL,
        project_activity_skey            INT             NULL,
        invoice_skey                     INT             NOT NULL,
        invoice_item_skey                INT             NULL,
        change_order_skey                INT             NULL,
        change_order_item_skey           INT             NULL,
        record_type                      NVARCHAR(20)    NULL,
        invoice_status                   NVARCHAR(20)    NULL,
        invoice_desc                     NVARCHAR(200)   NULL,
        invoice_item_id                  INT             NULL,
        contract_skey                    INT             NULL,
        contract_line_skey               INT             NULL,
        to_contact_skey                  INT             NULL,
        from_contact_skey                INT             NULL,
        to_company_skey                  INT             NULL,
        from_company_skey                INT             NULL,
        change_order_item_id             INT             NULL,
        line_number                      INT             NULL,
        work_completed_this_period       DECIMAL(18,2)   NULL,
        work_completed_to_date           DECIMAL(18,2)   NULL,
        work_retainage                   DECIMAL(18,2)   NULL,
        work_retainage_percent           DECIMAL(18,2)   NULL,
        payables_billed_total            DECIMAL(18,2)   NULL,
        payables_withheld_total          DECIMAL(18,2)   NULL,
        payables_remitted_total          DECIMAL(18,2)   NULL,
        balance_to_finish                DECIMAL(18,2)   NULL,
        balance_to_finish_with_retainage DECIMAL(18,2)   NULL,
        previous_total_earned            DECIMAL(18,2)   NULL,
        previous_work_completed          DECIMAL(18,2)   NULL,
        total_to_date                    DECIMAL(18,2)   NULL,
        total_earned_to_date             DECIMAL(18,2)   NULL,
        total_to_date_percent            DECIMAL(18,2)   NULL,
        total_retainage                  DECIMAL(18,2)   NULL,
        scheduled_value                  DECIMAL(18,2)   NULL,
        current_payment_due              DECIMAL(18,2)   NULL,
        source_unique_id                 INT             NULL,
        source_partition_id              INT             NULL,
        source_system_code               NVARCHAR(50)    NULL,
        source_created_ts                DATETIME2(3)    NULL,
        source_modified_ts               DATETIME2(3)    NULL,
        edp_update_ts                    DATETIME2(3)    NULL
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
        tgt.RefreshDate                      = src.RefreshDate,
        tgt.project_activity_skey            = src.project_activity_skey,
        tgt.change_order_skey                = src.change_order_skey,
        tgt.change_order_item_skey           = src.change_order_item_skey,
        tgt.record_type                      = src.record_type,
        tgt.invoice_status                   = src.invoice_status,
        tgt.invoice_desc                     = src.invoice_desc,
        tgt.invoice_item_id                  = src.invoice_item_id,
        tgt.contract_skey                    = src.contract_skey,
        tgt.contract_line_skey               = src.contract_line_skey,
        tgt.to_contact_skey                  = src.to_contact_skey,
        tgt.from_contact_skey                = src.from_contact_skey,
        tgt.to_company_skey                  = src.to_company_skey,
        tgt.from_company_skey                = src.from_company_skey,
        tgt.change_order_item_id             = src.change_order_item_id,
        tgt.line_number                      = src.line_number,
        tgt.work_completed_this_period       = src.work_completed_this_period,
        tgt.work_completed_to_date           = src.work_completed_to_date,
        tgt.work_retainage                   = src.work_retainage,
        tgt.work_retainage_percent           = src.work_retainage_percent,
        tgt.payables_billed_total            = src.payables_billed_total,
        tgt.payables_withheld_total          = src.payables_withheld_total,
        tgt.payables_remitted_total          = src.payables_remitted_total,
        tgt.balance_to_finish                = src.balance_to_finish,
        tgt.balance_to_finish_with_retainage = src.balance_to_finish_with_retainage,
        tgt.previous_total_earned            = src.previous_total_earned,
        tgt.previous_work_completed          = src.previous_work_completed,
        tgt.total_to_date                    = src.total_to_date,
        tgt.total_earned_to_date             = src.total_earned_to_date,
        tgt.total_to_date_percent            = src.total_to_date_percent,
        tgt.total_retainage                  = src.total_retainage,
        tgt.scheduled_value                  = src.scheduled_value,
        tgt.current_payment_due              = src.current_payment_due,
        tgt.source_unique_id                 = src.source_unique_id,
        tgt.source_partition_id              = src.source_partition_id,
        tgt.source_system_code               = src.source_system_code,
        tgt.source_created_ts                = src.source_created_ts,
        tgt.source_modified_ts               = src.source_modified_ts,
        tgt.edp_update_ts                    = src.edp_update_ts
      FROM ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
      JOIN ",
        temp_table,
        " src
        ON  tgt.project_skey       = src.project_skey
        AND tgt.invoice_skey       = src.invoice_skey
        AND tgt.invoice_item_skey  = src.invoice_item_skey;
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
        invoice_skey,
        invoice_item_skey,
        change_order_skey,
        change_order_item_skey,
        record_type,
        invoice_status,
        invoice_desc,
        invoice_item_id,
        contract_skey,
        contract_line_skey,
        to_contact_skey,
        from_contact_skey,
        to_company_skey,
        from_company_skey,
        change_order_item_id,
        line_number,
        work_completed_this_period,
        work_completed_to_date,
        work_retainage,
        work_retainage_percent,
        payables_billed_total,
        payables_withheld_total,
        payables_remitted_total,
        balance_to_finish,
        balance_to_finish_with_retainage,
        previous_total_earned,
        previous_work_completed,
        total_to_date,
        total_earned_to_date,
        total_to_date_percent,
        total_retainage,
        scheduled_value,
        current_payment_due,
        source_unique_id,
        source_partition_id,
        source_system_code,
        source_created_ts,
        source_modified_ts,
        edp_update_ts
      )
      SELECT
        src.RefreshDate,
        src.project_skey,
        src.project_activity_skey,
        src.invoice_skey,
        src.invoice_item_skey,
        src.change_order_skey,
        src.change_order_item_skey,
        src.record_type,
        src.invoice_status,
        src.invoice_desc,
        src.invoice_item_id,
        src.contract_skey,
        src.contract_line_skey,
        src.to_contact_skey,
        src.from_contact_skey,
        src.to_company_skey,
        src.from_company_skey,
        src.change_order_item_id,
        src.line_number,
        src.work_completed_this_period,
        src.work_completed_to_date,
        src.work_retainage,
        src.work_retainage_percent,
        src.payables_billed_total,
        src.payables_withheld_total,
        src.payables_remitted_total,
        src.balance_to_finish,
        src.balance_to_finish_with_retainage,
        src.previous_total_earned,
        src.previous_work_completed,
        src.total_to_date,
        src.total_earned_to_date,
        src.total_to_date_percent,
        src.total_retainage,
        src.scheduled_value,
        src.current_payment_due,
        src.source_unique_id,
        src.source_partition_id,
        src.source_system_code,
        src.source_created_ts,
        src.source_modified_ts,
        src.edp_update_ts
      FROM ",
        temp_table,
        " src
      LEFT JOIN ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
        ON  tgt.project_skey      = src.project_skey
        AND tgt.invoice_skey      = src.invoice_skey
        AND tgt.invoice_item_skey = src.invoice_item_skey
      WHERE tgt.invoice_skey IS NULL;
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
