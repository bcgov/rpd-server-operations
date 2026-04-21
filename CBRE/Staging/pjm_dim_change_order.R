ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "CbreStaging"
TABLE_NAME <- "pjm_dim_change_order"
CBRE_TABLE_NAME <- "pjm_dim_change_order_vw"
target_table <- Id(schema = SCHEMA_NAME, table = TABLE_NAME)
temp_table <- paste0("#", TABLE_NAME, "Temp")

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
        edp_create_ts
      ),
      ~ as.POSIXct(.x, format = "%Y-%m-%dT%H:%M:%OSZ")
    )
  ) |>
  mutate(
    across(
      c(
        change_order_amount
      ),
      as.double
    )
  ) |>
  mutate(RefreshDate = as.POSIXct(Sys.Date())) |>
  select(
    RefreshDate,
    project_skey,
    contract_skey,
    change_order_skey,
    change_order_id,
    change_order_number,
    status,
    change_order_form_template,
    change_order_amount,
    change_order_desc,
    source_unique_id,
    source_modified_ts,
    source_created_ts,
    edp_update_ts,
    edp_create_ts
  )


assert_that(
  nrow(cleaned_data) ==
    nrow(
      dplyr::distinct(
        cleaned_data,
        project_skey,
        contract_skey,
        change_order_skey
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

if(!dbExistsTable(con, target_table)){
  sql <- paste0("CREATE TABLE ", SCHEMA_NAME, ".", TABLE_NAME,
                " (
                RefreshDate                DATETIME2(3)   NOT NULL,
                project_skey               INT            NOT NULL,
                contract_skey              INT            NOT NULL,
                change_order_skey          INT            NOT NULL,
                change_order_id            NVARCHAR(20)   NOT NULL,
                change_order_number        NVARCHAR(10)   NOT NULL,
                status                     NVARCHAR(50)   NULL,
                change_order_form_template NVARCHAR(100)  NULL,
                change_order_amount        DECIMAL(18,2)  NULL,
                change_order_desc          NVARCHAR(2000)  NULL,
                source_unique_id           NVARCHAR(20)   NOT NULL,
                source_modified_ts         DATETIME2(3)   NULL,
                source_created_ts          DATETIME2(3)   NULL,
                edp_update_ts              DATETIME2(3)   NULL,
                edp_create_ts              DATETIME2(3)   NULL,

                CONSTRAINT PK_", TABLE_NAME, " PRIMARY KEY (
                  project_skey,
                  contract_skey,
                  change_order_skey)
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
            RefreshDate                DATETIME2(3)   NOT NULL,
            project_skey               INT            NOT NULL,
            contract_skey              INT            NOT NULL,
            change_order_skey          INT            NOT NULL,
            change_order_id            NVARCHAR(20)   NOT NULL,
            change_order_number        NVARCHAR(10)   NOT NULL,
            status                     NVARCHAR(50)   NULL,
            change_order_form_template NVARCHAR(100)  NULL,
            change_order_amount        DECIMAL(18,2)  NULL,
            change_order_desc          NVARCHAR(2000)  NULL,
            source_unique_id           NVARCHAR(20)   NOT NULL,
            source_modified_ts         DATETIME2(3)   NULL,
            source_created_ts          DATETIME2(3)   NULL,
            edp_update_ts              DATETIME2(3)   NULL,
            edp_create_ts              DATETIME2(3)   NULL
            );
  "))

  # Write the current tibble into the temp table
  dbWriteTable(
    con,
    name   = temp_table,
    value  = cleaned_data,
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
        tgt.RefreshDate                 = src.RefreshDate,
        tgt.change_order_id             = src.change_order_id,
        tgt.change_order_number         = src.change_order_number,
        tgt.status                      = src.status,
        tgt.change_order_form_template  = src.change_order_form_template,
        tgt.change_order_amount         = src.change_order_amount,
        tgt.change_order_desc           = src.change_order_desc,
        tgt.source_unique_id            = src.source_unique_id,
        tgt.source_modified_ts          = src.source_modified_ts,
        tgt.source_created_ts           = src.source_created_ts,
        tgt.edp_update_ts               = src.edp_update_ts,
        tgt.edp_create_ts               = src.edp_create_ts
      FROM ", SCHEMA_NAME, ".", TABLE_NAME, " tgt
      JOIN ", temp_table, " src
        ON  tgt.project_skey      = src.project_skey
        AND tgt.contract_skey     = src.contract_skey
        AND tgt.change_order_skey = src.change_order_skey;
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
        contract_skey,
        change_order_skey,
        change_order_id,
        change_order_number,
        status,
        change_order_form_template,
        change_order_amount,
        change_order_desc,
        source_unique_id,
        source_modified_ts,
        source_created_ts,
        edp_update_ts,
        edp_create_ts
      )
      SELECT
        src.RefreshDate,
        src.project_skey,
        src.contract_skey,
        src.change_order_skey,
        src.change_order_id,
        src.change_order_number,
        src.status,
        src.change_order_form_template,
        src.change_order_amount,
        src.change_order_desc,
        src.source_unique_id,
        src.source_modified_ts,
        src.source_created_ts,
        src.edp_update_ts,
        src.edp_create_ts
      FROM ", temp_table, " src
      LEFT JOIN ", SCHEMA_NAME, ".", TABLE_NAME, " tgt
        ON  tgt.project_skey      = src.project_skey
        AND tgt.contract_skey     = src.contract_skey
        AND tgt.change_order_skey = src.change_order_skey
      WHERE tgt.project_skey IS NULL;
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
