ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "CbreStaging"
TABLE_NAME <- "pjm_dim_budget"
CBRE_TABLE_NAME <- "pjm_dim_budget_vw"

target_table <- Id(schema = SCHEMA_NAME, table = TABLE_NAME)
temp_table <- paste0("#", TABLE_NAME, "Temp")

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

# Connect to SQL database
con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = SQL_SERVER,
  database = DB_NAME,
  Trusted_Connection = "Yes"
)

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
        budget_date,
        budget_submitted_date,
        authorized_date,
        source_modified_ts,
        edp_update_ts,
        edp_create_ts
      ),
      ~ as.POSIXct(.x, format = "%Y-%m-%dT%H:%M:%OSZ")
    )
  ) |>
  mutate(RefreshDate = as.POSIXct(Sys.Date())) |>
  select(
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



if(!dbExistsTable(con, target_table)){
  sql <- paste0("CREATE TABLE ", SCHEMA_NAME, ".", TABLE_NAME,
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
                );")

  dbExecute(con, sql)
}

# Database Transaction ####
# Control database transaction to ensure all steps done together or not at all
dbBegin(con)

# Begin error handling and rollback of transaction on failure
tryCatch({

  if(dbExistsTable(con, temp_table)){
    dbRemoveTable(con, temp_table)
  }

  # Create temp table to hold new data
  dbExecute(con,
  paste0("CREATE TABLE ", temp_table, " (
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
  "))

  # Write the current RoomAllocated tibble into the temp table
  dbWriteTable(
    con,
    name   = temp_table,
    value  = cleaned_data,
    append = TRUE,
    overwrite = FALSE
  )

  # Update existing rows in the target table
  n_updated <- dbExecute(con,
  paste0("
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
    FROM ", SCHEMA_NAME, ".", TABLE_NAME, " AS tgt
    JOIN ", temp_table, " AS src
      ON  tgt.budget_skey = src.budget_skey;
  ")
  )

  # Insert new rows not already in the target
  n_inserted <- dbExecute(con,
    paste0("
    INSERT INTO ", SCHEMA_NAME, ".", TABLE_NAME, " (
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
    FROM ", temp_table, " AS src
    LEFT JOIN ", SCHEMA_NAME, ".", TABLE_NAME, " AS tgt
      ON tgt.budget_skey = src.budget_skey
      WHERE tgt.budget_skey IS NULL;;
  ")
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


