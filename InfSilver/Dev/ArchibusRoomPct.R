#' @title Archibus Room Occupancy
#'
#' @description
#' Loads the current room occupancy and space-allocation inventory from
#' InfBronze.archibus_rmpct into the Silver layer. The script selects the
#' latest version of each room occupancy record, applies business-friendly
#' naming and data-type conversions, and publishes a current-state occupancy
#' dataset for reporting and analytical consumption.
#'
#' @database BuildingIntelligence
#' @schema InfSilver
#' @table ArchibusRoomPct
#'
#' @source InfBronze.archibus_rmpct
#'
#' @load_type Full refresh from latest Bronze records
#' @grain One row per room occupancy assignment
#'
#' @filters Only the most recent Bronze record version is retained for each
#'   occupancy assignment identifier
#' @filters Records are excluded if no current version exists in Bronze
#'
#' @consumers TODO
#'
#' @column RoomPctId Unique room occupancy assignment identifier
#' @column BuildingId Building identifier
#' @column FloorId Floor identifier
#' @column RoomId Room identifier
#' @column RoomStatus Occupancy status (0 = Proposed, 1 = Active, 2 = VBR, 3 = Retired, 4 = Rejected)
#' @column PoBCStatus Occupancy lifecycle status
#' @column LeaseId Associated lease identifier
#' @column DivisionId Associated division identifier
#' @column DepartmentId Associated department identifier
#' @column RoomType Room type classification
#' @column RoomCategory Room category classification
#' @column RoomArea Charged room area associated with the assignment
#' @column RoomPctSpace Percentage of room space allocated
#' @column RoomPctTime Percentage of occupant time allocated
#' @column DateStart Occupancy assignment start date
#' @column DateEnd Occupancy assignment end date
#' @column RoomResources This appears to be a repurposed field that is used for comments
#' @column BronzeBatchId Source Bronze batch identifier
#' @column BronzeLoadTs Timestamp of the Bronze record version selected
#'
#' @notes
#' Data is sourced from InfBronze.archibus_rmpct.
#'
#' This table represents room occupancy allocations and space assignments,
#' linking organizational units and lease relationships to individual rooms.
#'
#' Historical occupancy versions are retained in the Bronze layer. Only the
#' most recent version of each occupancy record is published to Silver.
#'
#' Technical source-system attribute names are transformed into
#' business-oriented field names and analytical data types where appropriate.
#'
#' RoomPctSpace and RoomPctTime support partial occupancy and shared-space
#' allocation reporting scenarios.
#'
#' @owner David Rattray
#' @last_reviewed 2026-11-25

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
TABLE_NAME <- "ArchibusRoomPct"
SOURCE_SCHEMA <- "InfBronze"
SOURCE_TABLE <- "archibus_rmpct"
SOURCE_PRIMARY_KEY <- c("rmpct_pct_id")
TARGET_PRIMARY_KEY <- c("RoomPctId")
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
pk_cols <- paste(SOURCE_PRIMARY_KEY, collapse = ", ")
query <- dbSendQuery(
  con,
  glue::glue(
    "
  SELECT *
    FROM (
      SELECT
      {pk_cols},
      rmpct_bl_id,
      rmpct_fl_id,
      rmpct_rm_id,
      rmpct_status,
      rmpct_status_pobc,
      rmpct_ls_id,
      rmpct_dv_id,
      rmpct_dp_id,
      rmpct_rm_type,
      rmpct_rm_cat,
      rmpct_area_chargable,
      rmpct_area_comn,
      rmpct_area_comn_nocup,
      rmpct_area_rm,
      rmpct_date_created,
      rmpct_date_deleted,
      rmpct_date_end,
      rmpct_date_end_pobc,
      rmpct_date_last_calc,
      rmpct_date_last_modified,
      rmpct_date_start,
      rmpct_date_start_pobc,
      rmpct_parent_pct_id,
      rmpct_pct_space,
      rmpct_pct_time,
      rmpct_primary_rm,
      rmpct_prorate,
      rmpct_resources,
      rmpct_sub_id,
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
    RoomPctId = rmpct_pct_id,
    BuildingId = rmpct_bl_id,
    FloorId = rmpct_fl_id,
    RoomId = rmpct_rm_id,
    RoomStatus = rmpct_status,
    PoBCStatus = rmpct_status_pobc,
    LeaseId = rmpct_ls_id,
    DivisionId = rmpct_dv_id,
    DepartmentId = rmpct_dp_id,
    RoomType = rmpct_rm_type,
    RoomCategory = rmpct_rm_cat,
    RoomAreaChargable = as.double(rmpct_area_chargable),
    RoomAreaCommon = as.double(rmpct_area_comn),
    RoomAreaCommonNonOccupied = as.double(rmpct_area_comn_nocup),
    RoomArea = as.double(rmpct_area_rm),
    DateCreated = as.POSIXct(rmpct_date_created, tz = "UTC"),
    DateDeleted = as.POSIXct(rmpct_date_deleted, tz = "UTC"),
    DateEnd = as.POSIXct(rmpct_date_end, tz = "UTC"),
    DateEndPoBC = as.POSIXct(rmpct_date_end_pobc, tz = "UTC"),
    DateLastCalc = as.POSIXct(rmpct_date_last_calc, tz = "UTC"),
    DateLastModified = as.POSIXct(rmpct_date_last_modified, tz = "UTC"),
    DateStart = as.POSIXct(rmpct_date_start, tz = "UTC"),
    DateStartPoBC = as.POSIXct(rmpct_date_start_pobc, tz = "UTC"),
    ParentRoomPctId = rmpct_parent_pct_id,
    RoomPctSpace = as.double(rmpct_pct_space),
    RoomPctTime = as.double(rmpct_pct_time),
    PrimaryRoom = rmpct_primary_rm,
    RoomProrate = rmpct_prorate,
    RoomResources = rmpct_resources,
    RoomSubId = rmpct_sub_id,
    BronzeBatchId = as.numeric(bronze_batch_id),
    BronzeLoadTs = as.POSIXct(bronze_load_ts, tz = "UTC"),
    .keep = "none",
    .before = everything()
  ) |>
  mutate(
    RoomStatus = case_when(
      RoomStatus == "0" ~ "Proposed",
      RoomStatus == "1" ~ "Active",
      RoomStatus == "2" ~ "VBR",
      RoomStatus == "3" ~ "Retired",
      RoomStatus == "4" ~ "Rejected"
    )
  )

# dbRemoveTable(con, TARGET_TABLE)
if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- glue::glue(
    "CREATE TABLE
    {SCHEMA_NAME}.{TABLE_NAME}
    (
      RoomPctId                 VARCHAR(5)    NOT NULL,
      BuildingId                VARCHAR(8)    NOT NULL,
      FloorId                   VARCHAR(4)    NOT NULL,
      RoomId                    VARCHAR(8)    NOT NULL,
      RoomStatus                VARCHAR(15)   NULL,
      PoBCStatus                VARCHAR(10)   NULL,
      LeaseId                   VARCHAR(20)   NULL,
      DivisionId                VARCHAR(10)   NULL,
      DepartmentId              VARCHAR(10)   NULL,
      RoomType                  VARCHAR(20)   NULL,
      RoomCategory              VARCHAR(20)   NULL,
      RoomAreaChargable         DECIMAL(18,2) NULL,
      RoomAreaCommon            DECIMAL(18,2) NULL,
      RoomAreaCommonNonOccupied DECIMAL(18,2) NULL,
      RoomArea                  DECIMAL(18,2) NULL,
      DateCreated               DATETIME2     NULL,
      DateDeleted               DATETIME2     NULL,
      DateEnd                   DATETIME2     NULL,
      DateEndPoBC               DATETIME2     NULL,
      DateLastCalc              DATETIME2     NULL,
      DateLastModified          DATETIME2     NULL,
      DateStart                 DATETIME2     NULL,
      DateStartPoBC             DATETIME2     NULL,
      ParentRoomPctId           VARCHAR(10)   NULL,
      RoomPctSpace              DECIMAL(18,2) NULL,
      RoomPctTime               DECIMAL(18,2) NULL,
      PrimaryRoom               VARCHAR(5)    NULL,
      RoomProrate               VARCHAR(10)   NULL,
      RoomResources             VARCHAR(1000) NULL,
      RoomSubId                 VARCHAR(10)   NULL,
      BronzeBatchId             BIGINT        NOT NULL,
      BronzeLoadTs              DATETIME2(0)  NOT NULL,
      CONSTRAINT PK_silver_rmpct
        PRIMARY KEY CLUSTERED ({paste(TARGET_PRIMARY_KEY, collapse = ', ')})
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
