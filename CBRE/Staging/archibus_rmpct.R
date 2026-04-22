ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "CbreStaging"
TABLE_NAME <- "RoomAllocation"
CBRE_TABLE_NAME <- "archibus_rmpct"
API_NAME <- "CBRE"
SCRIPT_NAME <- "RoomAllocation"

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

target_table <- Id(schema = SCHEMA_NAME, table = TABLE_NAME)
temp_table <- paste0("#", TABLE_NAME, "Temp")

raw_data <- extract_cbre_data(CBRE_TABLE_NAME)

cleaned_data <- raw_data |>
  select_if(~ !all(is.na(.))) |>
  select_if(~ !all(. == 0)) |>
  select_if(~ !all(. == '-1')) |>
  select_if(~ !all(. == "N/A")) |>
  select_if(~ !all(. == "-")) |>
  filter(rmpct_status_pobc == "Active") |>
  select(
    edp_update_ts,
    rmpct_date_start,
    rmpct_date_created,
    rmpct_date_end,
    rmpct_bl_id,
    rmpct_fl_id,
    rmpct_rm_id,
    rmpct_area_chargable,
    rmpct_area_rm,
    rmpct_area_comn,
    rmpct_area_comn_nocup,
    rmpct_pct_space,
    rmpct_prorate,
    rmpct_status,
    rmpct_rm_cat,
    rmpct_rm_type,
    rmpct_ls_id,
    rmpct_dv_id,
    rmpct_dp_id
  ) |>
  mutate(across(
    c(
      rmpct_area_chargable,
      rmpct_area_rm,
      rmpct_area_comn,
      rmpct_area_comn_nocup,
      rmpct_pct_space
    ),
    as.double
  )) |>
  mutate(across(
    c(
      edp_update_ts,
      rmpct_date_start,
      rmpct_date_created,
      rmpct_date_end
    ),
    as.POSIXct
  )) |>
  mutate(RefreshDate = as.POSIXct(Sys.time()), .before = everything())

# dbRemoveTable(con, target_table)
if (!dbExistsTable(con, target_table)) {
  sql <- paste0(
    "CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    TABLE_NAME,
    " (
                RefreshDate              DATETIME2(3)    NOT NULL,
                edp_update_ts            DATETIME2(3)    NOT NULL,
                rmpct_date_start         DATETIME2(3)    NULL,
                rmpct_date_created       DATETIME2(3)    NOT NULL,
                rmpct_date_end           DATETIME2(3)    NULL,
                rmpct_bl_id              NVARCHAR(20)    NOT NULL,
                rmpct_fl_id              NVARCHAR(10)    NOT NULL,
                rmpct_rm_id              NVARCHAR(20)    NOT NULL,
                rmpct_area_chargable     DECIMAL(18,5)   NULL,
                rmpct_area_rm            DECIMAL(18,5)   NULL,
                rmpct_area_comn          DECIMAL(18,5)   NULL,
                rmpct_area_comn_nocup    DECIMAL(18,5)   NULL,
                rmpct_pct_space          DECIMAL(9,5)    NULL,
                rmpct_prorate            NVARCHAR(20)    NULL,
                rmpct_status             NVARCHAR(10)    NULL,
                rmpct_rm_cat             NVARCHAR(50)    NULL,
                rmpct_rm_type            NVARCHAR(30)    NULL,
                rmpct_ls_id              NVARCHAR(50)    NULL,
                rmpct_dv_id              NVARCHAR(20)    NULL,
                rmpct_dp_id              NVARCHAR(20)    NULL
              );"
  )
  dbExecute(con, sql)
}

# Database Transaction ####
# Control database transaction to ensure all steps done together or not at all

etl_start_time <- Sys.time()

etl_error <- NULL

dbBegin(con)

# Begin error handling and rollback of transaction on failure
tryCatch(
  {
    if (dbExistsTable(con, temp_table)) {
      dbRemoveTable(con, temp_table)
    }

    # Create temp table to hold new data
    dbExecute(
      con,
      paste0(
        "CREATE TABLE ",
        temp_table,
        " (
                  RefreshDate              DATETIME2(3)    NOT NULL,
                  edp_update_ts            DATETIME2(3)    NOT NULL,
                  rmpct_date_start         DATETIME2(3)    NULL,
                  rmpct_date_created       DATETIME2(3)    NOT NULL,
                  rmpct_date_end           DATETIME2(3)    NULL,
                  rmpct_bl_id              NVARCHAR(20)    NOT NULL,
                  rmpct_fl_id              NVARCHAR(10)    NOT NULL,
                  rmpct_rm_id              NVARCHAR(20)    NOT NULL,
                  rmpct_area_chargable     DECIMAL(18,5)   NULL,
                  rmpct_area_rm            DECIMAL(18,5)   NULL,
                  rmpct_area_comn          DECIMAL(18,5)   NULL,
                  rmpct_area_comn_nocup    DECIMAL(18,5)   NULL,
                  rmpct_pct_space          DECIMAL(9,5)    NULL,
                  rmpct_prorate            NVARCHAR(20)    NULL,
                  rmpct_status             NVARCHAR(10)    NULL,
                  rmpct_rm_cat             NVARCHAR(50)    NULL,
                  rmpct_rm_type            NVARCHAR(30)    NULL,
                  rmpct_ls_id              NVARCHAR(50)    NULL,
                  rmpct_dv_id              NVARCHAR(20)    NULL,
                  rmpct_dp_id              NVARCHAR(20)    NULL
                  );"
      )
    )

    # Write the current tibble into the temp table
    dbWriteTable(
      con,
      name = temp_table,
      value = cleaned_data,
      append = TRUE,
      overwrite = FALSE
    )

    # Update existing rows in the target table
    n_updated <- dbExecute(
      con,
      paste0(
        "
    UPDATE tgt
    SET
      tgt.RefreshDate            = src.RefreshDate,
      tgt.edp_update_ts          = src.edp_update_ts,
      tgt.rmpct_date_start       = src.rmpct_date_start,
      tgt.rmpct_date_end         = src.rmpct_date_end,
      tgt.rmpct_area_chargable   = src.rmpct_area_chargable,
      tgt.rmpct_area_rm          = src.rmpct_area_rm,
      tgt.rmpct_area_comn        = src.rmpct_area_comn,
      tgt.rmpct_area_comn_nocup  = src.rmpct_area_comn_nocup,
      tgt.rmpct_pct_space        = src.rmpct_pct_space,
      tgt.rmpct_prorate          = src.rmpct_prorate,
      tgt.rmpct_status           = src.rmpct_status,
      tgt.rmpct_rm_cat           = src.rmpct_rm_cat,
      tgt.rmpct_rm_type          = src.rmpct_rm_type,
      tgt.rmpct_dv_id            = src.rmpct_dv_id,
      tgt.rmpct_dp_id            = src.rmpct_dp_id
    FROM ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " AS tgt
        JOIN ",
        temp_table,
        " AS src
    ON tgt.rmpct_bl_id         = src.rmpct_bl_id
    AND tgt.rmpct_fl_id        = src.rmpct_fl_id
    AND tgt.rmpct_rm_id        = src.rmpct_rm_id
    AND tgt.rmpct_date_created = src.rmpct_date_created
    AND (tgt.rmpct_ls_id = src.rmpct_ls_id
         OR (tgt.rmpct_ls_id IS NULL AND src.rmpct_ls_id IS NULL));
      "
      )
    )

    # Insert new rows not already in the target
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
    edp_update_ts,
    rmpct_date_start,
    rmpct_date_created,
    rmpct_date_end,
    rmpct_bl_id,
    rmpct_fl_id,
    rmpct_rm_id,
    rmpct_area_chargable,
    rmpct_area_rm,
    rmpct_area_comn,
    rmpct_area_comn_nocup,
    rmpct_pct_space,
    rmpct_prorate,
    rmpct_status,
    rmpct_rm_cat,
    rmpct_rm_type,
    rmpct_ls_id,
    rmpct_dv_id,
    rmpct_dp_id
)
SELECT
    src.RefreshDate,
    src.edp_update_ts,
    src.rmpct_date_start,
    src.rmpct_date_created,
    src.rmpct_date_end,
    src.rmpct_bl_id,
    src.rmpct_fl_id,
    src.rmpct_rm_id,
    src.rmpct_area_chargable,
    src.rmpct_area_rm,
    src.rmpct_area_comn,
    src.rmpct_area_comn_nocup,
    src.rmpct_pct_space,
    src.rmpct_prorate,
    src.rmpct_status,
    src.rmpct_rm_cat,
    src.rmpct_rm_type,
    src.rmpct_ls_id,
    src.rmpct_dv_id,
    src.rmpct_dp_id
FROM ",
        temp_table,
        " AS src
LEFT JOIN ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " AS tgt
  ON tgt.rmpct_bl_id        = src.rmpct_bl_id
 AND tgt.rmpct_fl_id        = src.rmpct_fl_id
 AND tgt.rmpct_rm_id        = src.rmpct_rm_id
 AND tgt.rmpct_date_created = src.rmpct_date_created
 AND (tgt.rmpct_ls_id = src.rmpct_ls_id
      OR (tgt.rmpct_ls_id IS NULL AND src.rmpct_ls_id IS NULL))
WHERE tgt.rmpct_bl_id IS NULL;
"
      )
    )

    # Delete erroneous active rows
    n_deleted <- dbExecute(
      con,
      paste0(
        "
DELETE tgt
FROM ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        "  tgt
LEFT JOIN ",
        temp_table,
        " src
ON tgt.rmpct_bl_id        = src.rmpct_bl_id
AND tgt.rmpct_fl_id        = src.rmpct_fl_id
AND tgt.rmpct_rm_id        = src.rmpct_rm_id
AND tgt.rmpct_date_created = src.rmpct_date_created
AND (tgt.rmpct_ls_id = src.rmpct_ls_id
     OR (tgt.rmpct_ls_id IS NULL AND src.rmpct_ls_id IS NULL))
WHERE src.rmpct_bl_id IS NULL
        AND tgt.rmpct_status IN ('1','2');"
      )
    )

    # Complete the transaction
    dbCommit(con)
    n_deleted <<- n_deleted
    n_inserted <<- n_inserted
    n_updated <<- n_updated
    # Rollback transaction on failure
  },
  error = function(e) {
    dbRollback(con)
    etl_error <<- e
    # stop(e)
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
    n_deleted = n_deleted,
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

# Identity Stability Test

query <- dbSendQuery(
  con,
  paste0(
    "SELECT
rmpct_bl_id, rmpct_fl_id, rmpct_rm_id, rmpct_date_created, rmpct_ls_id,
COUNT(*) AS cnt
FROM ",
    SCHEMA_NAME,
    ".",
    TABLE_NAME,
    "
GROUP BY
rmpct_bl_id, rmpct_fl_id, rmpct_rm_id, rmpct_date_created, rmpct_ls_id
HAVING COUNT(*) > 1;"
  )
)

IdentityTest <- dbFetch(query, n = -1)
dbClearResult(query)
