ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "CbreStaging"
TABLE_NAME <- "RoomTotal"
CBRE_TABLE_NAME <- "archibus_rm"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
API_NAME <- "CBRE"
SCRIPT_NAME <- "RoomTotal"

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

clean_data <- raw_data |>
  select_if(~ !all(is.na(.))) |>
  select_if(~ !all(. == 0)) |>
  select_if(~ !all(. == '-1')) |>
  select_if(~ !all(. == "N/A")) |>
  select_if(~ !all(. == "-")) |>
  filter(rm_status_pobc == "Active") |>
  select(
    edp_update_ts,
    # rm_date_costs_end may be a sign of things to filter out
    rm_date_costs_end,
    rm_bl_id,
    rm_fl_id,
    rm_rm_id,
    rm_area,
    rm_area_alloc,
    rm_area_chargable,
    rm_area_comn,
    rm_area_comn_nocup,
    rm_area_manual,
    rm_area_unalloc,
    rm_rm_cat,
    rm_rm_type,
    rm_name,
    rm_prorate,
    rm_ls_id,
    rm_dv_id,
    rm_dp_id
  ) |>
  mutate(
    across(
      c(
        rm_area,
        rm_area_alloc,
        rm_area_chargable,
        rm_area_comn,
        rm_area_comn_nocup,
        rm_area_manual,
        rm_area_unalloc
      ),
      as.double
    )
  ) |>
  # mutate(rm_date_costs_end = case_when(rm_date_costs_end == "4712-12-31T00:00:00Z" ~ NA,
  #                                      .default = rm_date_costs_end)) |>
  mutate(
    across(
      c(
        edp_update_ts,
        rm_date_costs_end
      ),
      as.POSIXct
    )
  ) |>
  mutate(RefreshDate = as.POSIXct(Sys.time()), .before = everything())

# Database Transaction ####
# dbRemoveTable(con, Id(schema = SCHEMA_NAME, table = TABLE_NAME))
if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- paste0(
    "CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    TABLE_NAME,
    " (
        RefreshDate           DATETIME2(3)    NOT NULL,
        edp_update_ts         DATETIME2(3)    NOT NULL,
        rm_date_costs_end     DATETIME2(3)    NULL,
        rm_bl_id              NVARCHAR(20)    NOT NULL,
        rm_fl_id              NVARCHAR(20)    NOT NULL,
        rm_rm_id              NVARCHAR(20)    NOT NULL,
        rm_area               DECIMAL(18,5)   NULL,
        rm_area_alloc         DECIMAL(18,5)   NULL,
        rm_area_chargable     DECIMAL(18,5)   NULL,
        rm_area_comn          DECIMAL(18,5)   NULL,
        rm_area_comn_nocup    DECIMAL(18,5)   NULL,
        rm_area_manual        DECIMAL(18,5)   NULL,
        rm_area_unalloc       DECIMAL(18,5)   NULL,
        rm_rm_cat             NVARCHAR(50)    NULL,
        rm_rm_type            NVARCHAR(50)    NULL,
        rm_name               NVARCHAR(150)   NULL,
        rm_prorate            NVARCHAR(20)    NULL,
        rm_ls_id              NVARCHAR(50)    NULL,
        rm_dv_id              NVARCHAR(20)    NULL,
        rm_dp_id              NVARCHAR(20)    NULL
      );"
  )
  dbExecute(con, sql)
}

etl_start_time <- Sys.time()

etl_error <- NULL

# Control database transaction to ensure all steps done together or not at all
dbBegin(con)

# Begin error handling and roll back on transaction failure
tryCatch(
  {
    if (dbExistsTable(con, TEMP_TABLE)) {
      dbRemoveTable(con, TEMP_TABLE)
    }

    # Create temp table to hold new data
    dbExecute(
      con,
      paste0(
        "
    CREATE TABLE  ",
        SCHEMA_NAME,
        ".",
        TEMP_TABLE,
        " (
        RefreshDate           DATETIME2(3)    NOT NULL,
        edp_update_ts         DATETIME2(3)    NOT NULL,
        rm_date_costs_end     DATETIME2(3)    NULL,
        rm_bl_id              NVARCHAR(20)    NOT NULL,
        rm_fl_id              NVARCHAR(20)    NOT NULL,
        rm_rm_id              NVARCHAR(20)    NOT NULL,
        rm_area               DECIMAL(18,5)   NULL,
        rm_area_alloc         DECIMAL(18,5)   NULL,
        rm_area_chargable     DECIMAL(18,5)   NULL,
        rm_area_comn          DECIMAL(18,5)   NULL,
        rm_area_comn_nocup    DECIMAL(18,5)   NULL,
        rm_area_manual        DECIMAL(18,5)   NULL,
        rm_area_unalloc       DECIMAL(18,5)   NULL,
        rm_rm_cat             NVARCHAR(50)    NULL,
        rm_rm_type            NVARCHAR(50)    NULL,
        rm_name               NVARCHAR(150)   NULL,
        rm_prorate            NVARCHAR(20)    NULL,
        rm_ls_id              NVARCHAR(50)    NULL,
        rm_dv_id              NVARCHAR(20)    NULL,
        rm_dp_id              NVARCHAR(20)    NULL
    );
  "
      )
    )

    dbWriteTable(
      con,
      name = TEMP_TABLE,
      value = clean_data,
      append = TRUE,
      overwrite = FALSE
    )

    dbExecute(
      con,
      paste0(
        "DELETE FROM ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        ";"
      )
    )

    n_inserted <- dbExecute(
      con,
      paste0(
        "INSERT INTO ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        "(
        RefreshDate,
        edp_update_ts,
        rm_date_costs_end,
        rm_bl_id,
        rm_fl_id,
        rm_rm_id,
        rm_area,
        rm_area_alloc,
        rm_area_chargable,
        rm_area_comn,
        rm_area_comn_nocup,
        rm_area_manual,
        rm_area_unalloc,
        rm_rm_cat,
        rm_rm_type,
        rm_name,
        rm_prorate,
        rm_ls_id,
        rm_dv_id,
        rm_dp_id
      )
       SELECT * FROM ",
        TEMP_TABLE,
        ";"
      )
    )

    # Complete the transaction
    dbCommit(con)
    #     n_deleted <<- n_deleted
    n_inserted <<- n_inserted
    #     n_updated <<- n_updated
    #     # Rollback transaction on failure
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
    n_updated = NA,
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
