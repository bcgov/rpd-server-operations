ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "CbreStaging"
TABLE_NAME <- "pjm_fact_milestone"
CBRE_TABLE_NAME <- "pjm_fact_milestone_vw"

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
        source_modified_ts,
        source_created_ts,
        edp_update_ts,
        estimated_start_date,
        revised_start_date,
        actual_start_date,
        estimated_end_date,
        revised_end_date,
        actual_end_date
      ),
      ~ as.POSIXct(.x, format = "%Y-%m-%dT%H:%M:%OSZ")
    )
  ) |>
  mutate(RefreshDate = as.POSIXct(Sys.Date())) |>
  select(
    RefreshDate,
    project_skey,
    milestone_skey,
    milestone_id,
    parent_milestone_id,
    milestone_desc,
    milestone_notes,
    estimated_start_date,
    revised_start_date,
    actual_start_date,
    estimated_end_date,
    revised_end_date,
    actual_end_date,
    project_start_milestone_f,
    project_end_milestone_f,
    is_na_f,
    show_on_dashboard_f,
    responsible_contact_skey,
    serial_number,
    source_partition_id,
    source_modified_ts,
    source_created_ts,
    edp_update_ts
  )


assert_that(
  nrow(cleaned_data) ==
    nrow(
      dplyr::distinct(
        cleaned_data,
        project_skey,
        milestone_skey
      )
    ),
  msg = paste(
    "Primary key violation detected:",
    "(project_skey, contract_skey, change_order_skey) is not unique in cleaned_data"
  )
)

# future debugging option
# pk_dupes <- cleaned_data |>
#   dplyr::count(
#     project_skey,
#     contract_skey,
#     change_order_skey,
#     name = "row_count"
#   ) |>
#   dplyr::filter(row_count > 1)
#
# assert_that(
#   nrow(pk_dupes) == 0,
#   msg = paste0(
#     "Duplicate primary keys found. Example rows:\n",
#     paste(capture.output(utils::head(pk_dupes, 10)), collapse = "\n")
#   )
# )

# dbRemoveTable(con, target_table)

if (!dbExistsTable(con, target_table)) {

  sql <- paste0(
    "CREATE TABLE ", SCHEMA_NAME, ".", TABLE_NAME, " (
      RefreshDate               DATETIME2(3)  NOT NULL,
      project_skey              INT           NOT NULL,
      milestone_skey            INT           NOT NULL,
      milestone_id              INT           NOT NULL,
      parent_milestone_id       INT           NULL,
      milestone_desc            NVARCHAR(500) NULL,
      milestone_notes           NVARCHAR(1000) NULL,
      estimated_start_date      DATETIME2(3)  NULL,
      revised_start_date        DATETIME2(3)  NULL,
      actual_start_date         DATETIME2(3)  NULL,
      estimated_end_date        DATETIME2(3)  NULL,
      revised_end_date          DATETIME2(3)  NULL,
      actual_end_date           DATETIME2(3)  NULL,
      project_start_milestone_f CHAR(1)        NULL,
      project_end_milestone_f   CHAR(1)        NULL,
      is_na_f                   CHAR(1)        NULL,
      show_on_dashboard_f       CHAR(1)        NULL,
      responsible_contact_skey  INT            NULL,
      serial_number             NVARCHAR(10)   NULL,
      source_partition_id       INT            NOT NULL,
      source_modified_ts        DATETIME2(3)   NULL,
      source_created_ts         DATETIME2(3)   NULL,
      edp_update_ts             DATETIME2(3)   NULL,

      CONSTRAINT PK_", TABLE_NAME, " PRIMARY KEY (
        project_skey,
        milestone_skey
      )
    );"
  )

  dbExecute(con, sql)
}

# Database Transaction ####
# Control database transaction to ensure all steps done together or not at all
dbBegin(con)

tryCatch({

  if (dbExistsTable(con, temp_table)) {
    dbRemoveTable(con, temp_table)
  }

  dbExecute(
    con,
    paste0(
      "CREATE TABLE ", temp_table, " (
        RefreshDate               DATETIME2(3)  NOT NULL,

        project_skey              INT           NOT NULL,
        milestone_skey            INT           NOT NULL,

        milestone_id              INT           NOT NULL,
        parent_milestone_id       INT           NULL,
        milestone_desc            NVARCHAR(500) NULL,
        milestone_notes           NVARCHAR(2000) NULL,

        estimated_start_date      DATETIME2(3)  NULL,
        revised_start_date        DATETIME2(3)  NULL,
        actual_start_date         DATETIME2(3)  NULL,
        estimated_end_date        DATETIME2(3)  NULL,
        revised_end_date          DATETIME2(3)  NULL,
        actual_end_date           DATETIME2(3)  NULL,

        project_start_milestone_f CHAR(1)        NULL,
        project_end_milestone_f   CHAR(1)        NULL,
        is_na_f                   CHAR(1)        NULL,
        show_on_dashboard_f       CHAR(1)        NULL,

        responsible_contact_skey  INT            NULL,
        serial_number             NVARCHAR(10)   NULL,

        source_partition_id       INT            NOT NULL,
        source_modified_ts        DATETIME2(3)   NULL,
        source_created_ts         DATETIME2(3)   NULL,
        edp_update_ts             DATETIME2(3)   NULL
      );"
    )
  )

  dbWriteTable(
    con,
    name      = temp_table,
    value     = cleaned_data,
    append    = TRUE,
    overwrite = FALSE
  )


  # Update existing rows in the target table

  n_updated <- dbExecute(
    con,
    paste0(
      "
      UPDATE tgt
      SET
        tgt.RefreshDate               = src.RefreshDate,
        tgt.milestone_id              = src.milestone_id,
        tgt.parent_milestone_id       = src.parent_milestone_id,
        tgt.milestone_desc            = src.milestone_desc,
        tgt.milestone_notes           = src.milestone_notes,
        tgt.estimated_start_date      = src.estimated_start_date,
        tgt.revised_start_date        = src.revised_start_date,
        tgt.actual_start_date         = src.actual_start_date,
        tgt.estimated_end_date        = src.estimated_end_date,
        tgt.revised_end_date          = src.revised_end_date,
        tgt.actual_end_date           = src.actual_end_date,
        tgt.project_start_milestone_f = src.project_start_milestone_f,
        tgt.project_end_milestone_f   = src.project_end_milestone_f,
        tgt.is_na_f                   = src.is_na_f,
        tgt.show_on_dashboard_f       = src.show_on_dashboard_f,
        tgt.responsible_contact_skey  = src.responsible_contact_skey,
        tgt.serial_number             = src.serial_number,
        tgt.source_partition_id       = src.source_partition_id,
        tgt.source_modified_ts        = src.source_modified_ts,
        tgt.source_created_ts         = src.source_created_ts,
        tgt.edp_update_ts             = src.edp_update_ts
      FROM ", SCHEMA_NAME, ".", TABLE_NAME, " tgt
      JOIN ", temp_table, " src
        ON  tgt.project_skey   = src.project_skey
        AND tgt.milestone_skey = src.milestone_skey;
      "
    )
  )

  n_inserted <- dbExecute(
    con,
    paste0(
      "
      INSERT INTO ", SCHEMA_NAME, ".", TABLE_NAME, " (
        RefreshDate,
        project_skey,
        milestone_skey,
        milestone_id,
        parent_milestone_id,
        milestone_desc,
        milestone_notes,
        estimated_start_date,
        revised_start_date,
        actual_start_date,
        estimated_end_date,
        revised_end_date,
        actual_end_date,
        project_start_milestone_f,
        project_end_milestone_f,
        is_na_f,
        show_on_dashboard_f,
        responsible_contact_skey,
        serial_number,
        source_partition_id,
        source_modified_ts,
        source_created_ts,
        edp_update_ts
      )
      SELECT
        src.RefreshDate,
        src.project_skey,
        src.milestone_skey,
        src.milestone_id,
        src.parent_milestone_id,
        src.milestone_desc,
        src.milestone_notes,
        src.estimated_start_date,
        src.revised_start_date,
        src.actual_start_date,
        src.estimated_end_date,
        src.revised_end_date,
        src.actual_end_date,
        src.project_start_milestone_f,
        src.project_end_milestone_f,
        src.is_na_f,
        src.show_on_dashboard_f,
        src.responsible_contact_skey,
        src.serial_number,
        src.source_partition_id,
        src.source_modified_ts,
        src.source_created_ts,
        src.edp_update_ts
      FROM ", temp_table, " src
      LEFT JOIN ", SCHEMA_NAME, ".", TABLE_NAME, " tgt
        ON  tgt.project_skey   = src.project_skey
        AND tgt.milestone_skey = src.milestone_skey
      WHERE tgt.project_skey IS NULL;
      "
    )
  )

  # Complete the transaction
  dbCommit(con)

  n_inserted <<- n_inserted
  n_updated <<- n_updated
  # Rollback transaction on failure
}, error = function(e) {
  dbRollback(con)
  stop(e)
})
