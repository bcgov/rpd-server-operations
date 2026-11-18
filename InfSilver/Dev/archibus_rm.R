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
TABLE_NAME <- "ArchibusRoom"
SOURCE_SCHEMA <- "InfBronze"
SOURCE_TABLE <- "archibus_rm"
SOURCE_PRIMARY_KEY <- c("rm_bl_id", "rm_fl_id", "rm_rm_id")
TARGET_PRIMARY_KEY <- c("BuildingId", "FloorId", "RoomId")
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
query <- dbSendQuery(con, "SELECT * FROM InfBronze.archibus_rm")
RoomData <- dbFetch(query, n = -1)
dbClearResult(query)
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
      rm_area,
      rm_area_alloc,
      rm_area_chargable,
      rm_area_comn,
      rm_area_comn_nocup,
      rm_area_comn_ocup,
      rm_area_manual,
      rm_area_unalloc,
      rm_cost_space,
      rm_date_costs_end,
      rm_date_costs_start,
      rm_date_end_pobc,
      rm_date_start_pobc,
      rm_dp_id,
      rm_dv_id,
      rm_dwgname,
      rm_ehandle,
      rm_handicap_space,
      rm_length,
      rm_ls_id,
      rm_name,
      rm_option1,
      rm_option2,
      rm_plug_space,
      rm_prorate,
      rm_rm_cat,
      rm_rm_type,
      rm_status,
      rm_status_pobc,
      rm_transfer_status,
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

archibus_rm <- dbFetch(query, n = -1)
dbClearResult(query)

data <- archibus_rm |>
  select(-c(rn)) |>
  # mutate(across(everything(), trimws)) |> # I haven't noticed any need to trim whitespace but maybe need at some point
  mutate(
    BuildingId = rm_bl_id,
    FloorId = rm_fl_id,
    RoomId = rm_rm_id,
    Name = rm_name,
    Category = rm_rm_cat,
    Type = rm_rm_type,
    Status = rm_status_pobc,
    Area = as.double(rm_area),
    AllocatedArea = as.double(rm_area_alloc),
    UnallocatedArea = as.double(rm_area_unalloc),
    ChargableArea = as.double(rm_area_chargable),
    CommonArea = as.double(rm_area_comn),
    OccupiedCommonArea = as.double(rm_area_comn_ocup),
    UnoccupiedCommonArea = as.double(rm_area_comn_nocup),
    ManualArea = as.double(rm_area_manual),
    Length = as.double(rm_length),
    SpaceCost = as.double(rm_cost_space),
    DateCostsStart = as.POSIXct(rm_date_costs_start, tz = "UTC"),
    DateCostsEnd = as.POSIXct(rm_date_costs_end, tz = "UTC"),
    DatePoBCStart = as.POSIXct(rm_date_start_pobc, tz = "UTC"),
    DatePoBCEnd = as.POSIXct(rm_date_end_pobc, tz = "UTC"),
    LeaseId = rm_ls_id,
    DivisionId = rm_dv_id,
    DepartmentId = rm_dp_id,
    DrawingName = rm_dwgname,
    Ehandle = rm_ehandle,
    PlugSpace = as.logical(stringr::str_to_upper(rm_plug_space)),
    HandicapSpace = as.logical(stringr::str_to_upper(rm_handicap_space)),
    TransferStatus = rm_transfer_status,
    BronzeBatchId = as.numeric(bronze_batch_id),
    BronzeLoadTs = as.POSIXct(bronze_load_ts, tz = "UTC"),
    .keep = "none"
  )

# dbRemoveTable(con, TARGET_TABLE)
if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- glue::glue(
    "CREATE TABLE
    {SCHEMA_NAME}.{TABLE_NAME}
    (
      BuildingId               VARCHAR(8)    NOT NULL,
      FloorId                  VARCHAR(4)    NOT NULL,
      RoomId                   VARCHAR(8)    NOT NULL,
      Name                     VARCHAR(100)  NULL,
      Category                 VARCHAR(20)   NULL,
      Type                     VARCHAR(20)   NULL,
      Status                   VARCHAR(20)   NULL,
      Area                     DECIMAL(18,2) NULL,
      AllocatedArea            DECIMAL(18,2) NULL,
      UnallocatedArea          DECIMAL(18,2) NULL,
      ChargableArea            DECIMAL(18,2) NULL,
      CommonArea               DECIMAL(18,2) NULL,
      OccupiedCommonArea       DECIMAL(18,2) NULL,
      UnoccupiedCommonArea     DECIMAL(18,2) NULL,
      ManualArea               DECIMAL(18,2) NULL,
      Length                   DECIMAL(18,2) NULL,
      SpaceCost                DECIMAL(18,2) NULL,
      DateCostsStart           DATETIME2     NULL,
      DateCostsEnd             DATETIME2     NULL,
      DatePoBCStart            DATETIME2     NULL,
      DatePoBCEnd              DATETIME2     NULL,
      LeaseId                  VARCHAR(20)   NULL,
      DivisionId               VARCHAR(10)   NULL,
      DepartmentId             VARCHAR(10)   NULL,
      DrawingName              VARCHAR(25)   NULL,
      Ehandle                  VARCHAR(10)   NULL,
      PlugSpace                BIT           NULL,
      HandicapSpace            BIT           NULL,
      TransferStatus           VARCHAR(10)   NULL,
      BronzeBatchId            BIGINT        NOT NULL,
      BronzeLoadTs             DATETIME2(0)  NOT NULL,
      CONSTRAINT PK_silver_room PRIMARY KEY ({paste(TARGET_PRIMARY_KEY, collapse = ', ')})
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
