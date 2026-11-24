#' @title Archibus Division
#'
#' @description
#' Loads the current division inventory from InfBronze.archibus_dv into the
#' Silver layer. The script selects the latest version of each division
#' record, applies business-friendly naming and data-type conversions, and
#' publishes a current-state division dataset for reporting and analytical
#' consumption.
#'
#' @database BuildingIntelligence
#' @schema InfSilver
#' @table ArchibusDivision
#'
#' @source InfBronze.archibus_dv
#'
#' @load_type Full refresh from latest Bronze records
#' @grain One row per division
#'
#' @filters Only the most recent Bronze record version is retained for each
#'   division identifier
#' @filters Records are excluded if no current version exists in Bronze
#'
#' @consumers TODO
#'
#' @column DivisionId Division identifier
#' @column DivisionName Division name
#' @column BusinessId Associated business-unit identifier
#' @column DivisionStatus Current division status
#' @column AreaRoom Total room area associated with the division
#' @column ChargableArea Chargeable area associated with the division
#' @column AreaOccupied Occupied area associated with the division
#' @column BronzeBatchId Source Bronze batch identifier
#' @column BronzeLoadTs Timestamp of the Bronze record version selected
#'
#' @notes
#' Data is sourced from InfBronze.archibus_dv.
#'
#' This table provides the current-state division inventory used for
#' organizational reporting, space allocation, occupancy analysis, and
#' departmental rollup reporting.
#'
#' Historical division versions are retained in the Bronze layer. Only the
#' most recent version of each division record is published to Silver.
#'
#' Frequently used business attributes are promoted to business-friendly
#' column names, while less commonly consumed Archibus attributes retain
#' names closely aligned with the source system.
#'
#' @owner David Rattray
#' @last_reviewed 2026-11-23

# For server logging
# Begin timer
task_start <- Sys.time()

# Setup necessary variables
ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "InfSilver"
TABLE_NAME <- "ArchibusDivision"
SOURCE_SCHEMA <- "InfBronze"
SOURCE_TABLE <- "archibus_dv"
SOURCE_PRIMARY_KEY <- c("dv_dv_id")
TARGET_PRIMARY_KEY <- c("DivisionId")
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
API_NAME <- "CBRE"
SCRIPT_NAME <- if (ETL_STATUS == "PROD") {
  paste0("PROD-", TABLE_NAME)
} else {
  paste0("TEST-", TABLE_NAME)
}
BATCH_ID <- as.numeric(format(task_start, "%Y%m%d%H%M%S", tz = "UTC"))
AUDIT_TABLE <- DBI::Id(schema = "ServerLogs", table = SCHEMA_NAME)
MIN_ROW_RATIO <- 0.9 # abort if the new load is under 90% of the current row count

# Connect to SQL database
con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = SQL_SERVER,
  database = DB_NAME,
  Trusted_Connection = "Yes"
)

# Query Datasets ####
# query <- dbSendQuery(con, "SELECT * FROM InfBronze.archibus_dv")
# DivData <- dbFetch(query, n = -1)
# dbClearResult(query)
# archibus_rm

pk_cols <- paste(SOURCE_PRIMARY_KEY, collapse = ", ")
query <- dbSendQuery(
  con,
  glue::glue(
    "
  SELECT *
    FROM (
      SELECT
      {pk_cols},
      dv_area_rm,
      dv_area_chargable,
      dv_area_comn,
      dv_area_comn_nocup,
      dv_area_comn_ocup,
      dv_area_ocup,
      dv_bu_id,
      dv_hpattern_acad,
      dv_name,
      dv_option1,
      dv_status,
      row_hash,
      bronze_batch_id,
      bronze_load_ts,
      ROW_NUMBER() OVER (
        PARTITION BY {pk_cols} ORDER BY bronze_load_ts DESC
      ) as rn
      FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}
    ) ranked
  WHERE rn = 1
  "
  )
)

bronze_data <- dbFetch(query, n = -1)
dbClearResult(query)

data <- bronze_data |>
  select(-c(rn)) |>
  # mutate(across(everything(), trimws)) |> # I haven't noticed any need to trim whitespace but maybe need at some point
  mutate(
    DivisionId = dv_dv_id,
    DivisionName = dv_name,
    BusinessId = dv_bu_id,
    DivisionStatus = dv_status,
    AreaRoom = as.double(dv_area_rm),
    ChargableArea = as.double(dv_area_chargable),
    AreaOccupied = as.double(dv_area_ocup),
    CommonArea = as.double(dv_area_comn),
    OccupiedCommonArea = as.double(dv_area_comn_ocup),
    UnoccupiedCommonArea = as.double(dv_area_comn_nocup),
    dv_hpattern_acad = as.character(dv_hpattern_acad),
    BronzeBatchId = as.numeric(bronze_batch_id),
    BronzeLoadTs = as.POSIXct(bronze_load_ts, tz = "UTC"),
    .keep = "none",
    .before = everything()
  )

# dbRemoveTable(con, TARGET_TABLE)
if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- glue::glue(
    "CREATE TABLE
    {SCHEMA_NAME}.{TABLE_NAME}
    (
      DivisionId               VARCHAR(20)   NOT NULL,
      DivisionName             VARCHAR(100)  NULL,
      BusinessId               VARCHAR(20)   NULL,
      DivisionStatus           VARCHAR(10)   NULL,
      AreaRoom                 DECIMAL(18,2) NULL,
      ChargableArea            DECIMAL(18,2) NULL,
      AreaOccupied             DECIMAL(18,2) NULL,
      CommonArea               DECIMAL(18,2) NULL,
      OccupiedCommonArea       DECIMAL(18,2) NULL,
      UnoccupiedCommonArea     DECIMAL(18,2) NULL,
      dv_hpattern_acad         VARCHAR(50)   NULL,
      BronzeBatchId            BIGINT        NOT NULL,
      BronzeLoadTs             DATETIME2(0)  NOT NULL,
      CONSTRAINT PK_silver_division PRIMARY KEY CLUSTERED ({paste(TARGET_PRIMARY_KEY, collapse = ', ')})
    );"
  )
  dbExecute(con, sql)
}

etl_error <- NULL

dbBegin(con)

tryCatch(
  {
    prior_n <- dbGetQuery(
      con,
      glue::glue("SELECT COUNT(*) AS n FROM {SCHEMA_NAME}.{TABLE_NAME}")
    )$n

    # Guard 1: nothing came back from bronze
    if (nrow(data) == 0) {
      stop(
        "Bronze query returned 0 rows; aborting to protect the silver table."
      )
    }

    # Guard 2: suspiciously small load compared to what's there now
    # (skipped on the first run, when the table is empty)
    if (prior_n > 0 && nrow(data) < prior_n * MIN_ROW_RATIO) {
      stop(sprintf(
        "Partial load suspected: %d rows to load vs %d currently in silver (threshold %.0f%%).",
        nrow(data),
        prior_n,
        MIN_ROW_RATIO * 100
      ))
    }

    dbExecute(con, glue::glue("DELETE FROM {SCHEMA_NAME}.{TABLE_NAME}"))
    dbAppendTable(con, TARGET_TABLE, data)

    # Guard 3: confirm everything actually landed before committing
    new_n <- dbGetQuery(
      con,
      glue::glue("SELECT COUNT(*) AS n FROM {SCHEMA_NAME}.{TABLE_NAME}")
    )$n
    if (new_n != nrow(data)) {
      stop(sprintf(
        "Row count mismatch after insert: expected %d, found %d.",
        nrow(data),
        new_n
      ))
    }
    audit_row <<- tibble(
      load_ts = as.POSIXct(task_start, tz = "UTC"),
      batch_id = as.numeric(format(task_start, "%Y%m%d%H%M%S", tz = "UTC")),
      source_system = API_NAME,
      source_table = SOURCE_TABLE,
      output_table = TABLE_NAME,
      data_rows = nrow(data)
    )
    dbCommit(con)
    cat(
      "ETL complete — Audit Row Created:",
      audit_row$data_rows,
      " rows of data.",
      "\n"
    )
  },
  error = function(e) {
    dbRollback(con)
    etl_error <<- e
    # Good spot for the audit-table failure record once that's built
    stop(e)
  }
)

task_end <- Sys.time()
task_duration <- interval(task_start, task_end) / dseconds()

audit_row <- audit_row |>
  mutate(Duration = round(task_duration, digits = 2))

DBI::dbAppendTable(con, AUDIT_TABLE, audit_row)

if (is.null(etl_error)) {
  log_daily_etl_run(
    api_name = API_NAME,
    script_name = SCRIPT_NAME,
    table_name = TABLE_NAME,
    duration = task_duration,
    status = "SUCCESS",
    n_inserted = nrow(data),
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
