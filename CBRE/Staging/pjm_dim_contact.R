ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "CbreStaging"
TABLE_NAME <- "pjm_dim_contact"
CBRE_TABLE_NAME <- "pjm_dim_contact_vw"

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

# dbRemoveTable(con, target_table)

if (!dbExistsTable(con, target_table)) {
  sql <- paste0(
    "CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    TABLE_NAME,
    " (
      RefreshDate         DATETIME2(3)   NOT NULL,
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
        project_role_skey   INT            NOT NULL,
        project_role        NVARCHAR(100)  NULL,
        source_unique_id    NVARCHAR(100)  NOT NULL
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
        tgt.project_role_skey = src.project_role_skey,
        tgt.project_role      = src.project_role,
        tgt.source_unique_id  = src.source_unique_id
      FROM ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
      JOIN ",
        temp_table,
        " src
        ON  tgt.project_role_skey = src.project_role_skey
        WHERE
            ISNULL(tgt.project_role, '')     <> ISNULL(src.project_role, '')
         OR ISNULL(tgt.source_unique_id, '') <> ISNULL(src.source_unique_id, '');
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
        project_role_skey,
        project_role,
        source_unique_id
      )
      SELECT
        src.RefreshDate,
        src.project_role_skey,
        src.project_role,
        src.source_unique_id
      FROM ",
        temp_table,
        " src
      LEFT JOIN ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
        ON  tgt.project_role_skey = src.project_role_skey
      WHERE tgt.project_role_skey IS NULL;
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
        ON tgt.project_role_skey = src.project_role_skey
      WHERE src.project_role_skey IS NULL;"
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
