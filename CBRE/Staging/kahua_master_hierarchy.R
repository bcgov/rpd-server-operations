ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "CbreStaging"
TABLE_NAME <- "kahua_master_hierarchy"
CBRE_TABLE_NAME <- "kahua_master_hierarchy"

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
  filter(level_0 == "Province of British Columbia") |>
  mutate(RefreshDate = as.POSIXct(Sys.Date())) |>
  select(
    RefreshDate,
    id,
    parentid,
    name,
    level,
    type,
    number,
    max_level,
    path,
    level_0,
    level_1,
    level_2,
    level_3,
    level_4,
    level_5,
    level_6
  )

# dbRemoveTable(con, target_table)

if (!dbExistsTable(con, target_table)) {
  sql <- paste0(
    "CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    TABLE_NAME,
    " (
      RefreshDate  DATETIME2(3)  NOT NULL,
      id           INT           NOT NULL,
      parentid     INT           NULL,
      name         NVARCHAR(250) NULL,
      level        INT           NULL,
      type         NVARCHAR(10)  NULL,
      number       NVARCHAR(10)  NULL,
      max_level    INT           NULL,
      path         NVARCHAR(400) NULL,
      level_0      NVARCHAR(200) NULL,
      level_1      NVARCHAR(200) NULL,
      level_2      NVARCHAR(200) NULL,
      level_3      NVARCHAR(200) NULL,
      level_4      NVARCHAR(200) NULL,
      level_5      NVARCHAR(200) NULL,
      level_6      NVARCHAR(200) NULL);"
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
        RefreshDate  DATETIME2(3)  NOT NULL,
        id           INT           NOT NULL,
        parentid     INT           NULL,
        name         NVARCHAR(250) NULL,
        level        INT           NULL,
        type         NVARCHAR(10)  NULL,
        number       NVARCHAR(10)  NULL,
        max_level    INT           NULL,
        path         NVARCHAR(400) NULL,
        level_0      NVARCHAR(200) NULL,
        level_1      NVARCHAR(200) NULL,
        level_2      NVARCHAR(200) NULL,
        level_3      NVARCHAR(200) NULL,
        level_4      NVARCHAR(200) NULL,
        level_5      NVARCHAR(200) NULL,
        level_6      NVARCHAR(200) NULL
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
        tgt.parentid   = src.parentid,
        tgt.name       = src.name,
        tgt.level      = src.level,
        tgt.type       = src.type,
        tgt.number     = src.number,
        tgt.max_level  = src.max_level,
        tgt.path       = src.path,
        tgt.level_0    = src.level_0,
        tgt.level_1    = src.level_1,
        tgt.level_2    = src.level_2,
        tgt.level_3    = src.level_3,
        tgt.level_4    = src.level_4,
        tgt.level_5    = src.level_5,
        tgt.level_6    = src.level_6,
        tgt.RefreshDate = src.RefreshDate
      FROM ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
      JOIN ",
        temp_table,
        " src
        ON  tgt.id = src.id
        WHERE
            ISNULL(tgt.parentid, -1)   <> ISNULL(src.parentid, -1)
         OR ISNULL(tgt.name, '')       <> ISNULL(src.name, '')
         OR ISNULL(tgt.level, -1)      <> ISNULL(src.level, -1)
         OR ISNULL(tgt.type, '')       <> ISNULL(src.type, '')
         OR ISNULL(tgt.number, '')     <> ISNULL(src.number, '')
         OR ISNULL(tgt.max_level, -1)  <> ISNULL(src.max_level, -1)
         OR ISNULL(tgt.path, '')       <> ISNULL(src.path, '')
         OR ISNULL(tgt.level_0, '')    <> ISNULL(src.level_0, '')
         OR ISNULL(tgt.level_1, '')    <> ISNULL(src.level_1, '')
         OR ISNULL(tgt.level_2, '')    <> ISNULL(src.level_2, '')
         OR ISNULL(tgt.level_3, '')    <> ISNULL(src.level_3, '')
         OR ISNULL(tgt.level_4, '')    <> ISNULL(src.level_4, '')
         OR ISNULL(tgt.level_5, '')    <> ISNULL(src.level_5, '')
         OR ISNULL(tgt.level_6, '')    <> ISNULL(src.level_6, '');
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
        id,
        parentid,
        name,
        level,
        type,
        number,
        max_level,
        path,
        level_0,
        level_1,
        level_2,
        level_3,
        level_4,
        level_5,
        level_6
      )
      SELECT
        src.RefreshDate,
        src.id,
        src.parentid,
        src.name,
        src.level,
        src.type,
        src.number,
        src.max_level,
        src.path,
        src.level_0,
        src.level_1,
        src.level_2,
        src.level_3,
        src.level_4,
        src.level_5,
        src.level_6
      FROM ",
        temp_table,
        " src
      LEFT JOIN ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
        ON  tgt.id = src.id
      WHERE tgt.id IS NULL;
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
        ON tgt.id = src.id
      WHERE src.id IS NULL;"
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
