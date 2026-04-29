ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "CbreStaging"
TABLE_NAME <- "dim_project_activity"
CBRE_TABLE_NAME <- "dim_project_activity_vw"

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
  "dim_project_activity_vw",
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

file_id <- jobs[[1]]$file_id

jobs[[1]]$edp_table
output <- retrieve_edp_export(file_id = file_id)

raw_data <- extract_cbre_data(CBRE_TABLE_NAME)

proj_activity_data <- raw_data

write.csv(proj_activity_data, "C:/Projects/dim_project_activity_vw.csv")

cleaned_data <- proj_activity_data |>
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
      ~ as.POSIXct(.x, format = "%Y-%m-%dT%H:%M:%OSZ")
    )
  ) |>
  select(
    RefreshDate,
    project_activity_skey,
    code,
    code_type,
    code_parent,
    activity_desc,
    workbreakdown_id,
    source_system_code,
    source_partition_id,
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
      RefreshDate           DATETIME2(3)    NOT NULL,
      project_activity_skey INT             NOT NULL,
      code                  NVARCHAR(50)    NULL,
      code_type             NVARCHAR(20)    NULL,
      code_parent           NVARCHAR(50)    NULL,
      activity_desc         NVARCHAR(200)   NULL,
      workbreakdown_id      INT             NULL,
      source_system_code    NVARCHAR(50)    NULL,
      source_partition_id   INT             NULL,
      source_unique_id      NVARCHAR(50)    NULL,
      edp_update_ts         DATETIME2(3)    NULL,

      CONSTRAINT PK_",
    TABLE_NAME,
    " PRIMARY KEY (
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
        RefreshDate           DATETIME2(3)    NOT NULL,
        project_activity_skey INT             NOT NULL,
        code                  NVARCHAR(50)    NULL,
        code_type             NVARCHAR(20)    NULL,
        code_parent           NVARCHAR(50)    NULL,
        activity_desc         NVARCHAR(200)   NULL,
        workbreakdown_id      INT             NULL,
        source_system_code    NVARCHAR(50)    NULL,
        source_partition_id   INT             NULL,
        source_unique_id      NVARCHAR(50)    NULL,
        edp_update_ts         DATETIME2(3)    NULL
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
        tgt.RefreshDate           = src.RefreshDate,
        tgt.workbreakdown_id      = src.workbreakdown_id,
        tgt.code                  = src.code,
        tgt.code_type             = src.code_type,
        tgt.code_parent           = src.code_parent,
        tgt.activity_desc         = src.activity_desc,
        tgt.source_system_code    = src.source_system_code,
        tgt.source_partition_id   = src.source_partition_id,
        tgt.source_unique_id      = src.source_unique_id,
        tgt.edp_update_ts         = src.edp_update_ts
      FROM ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
      JOIN ",
        temp_table,
        " src
        ON  tgt.project_activity_skey = src.project_activity_skey;
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
        project_activity_skey,
        workbreakdown_id,
        code,
        code_type,
        code_parent,
        activity_desc,
        source_system_code,
        source_partition_id,
        source_unique_id,
        edp_update_ts
      )
      SELECT
        src.RefreshDate,
        src.project_activity_skey,
        src.workbreakdown_id,
        src.code,
        src.code_type,
        src.code_parent,
        src.activity_desc,
        src.source_system_code,
        src.source_partition_id,
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
        ON  tgt.project_activity_skey = src.project_activity_skey
      WHERE tgt.project_activity_skey IS NULL;
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
