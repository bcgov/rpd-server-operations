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
SCHEMA_NAME <- "InfBronze"
TABLE_NAME <- "archibus_bl"
CBRE_TABLE_NAME <- "archibus_bl"
PRIMARY_KEY <- "bl_bl_id_key"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
API_NAME <- "CBRE"
SCRIPT_NAME <- if (ETL_STATUS == "PROD") {
  paste0("PROD-", TABLE_NAME)
} else {
  paste0("TEST-", TABLE_NAME)
}

# Connect to SQL database
con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = SQL_SERVER,
  database = DB_NAME,
  Trusted_Connection = "Yes"
)

raw_data <- call_cbre_api(
  CBRE_TABLE_NAME,
  start_time = etl_window$cbre_start_time,
  end_time = etl_window$cbre_end_time
)

if (raw_data$status == "partial") {
  # True API/network failure
  error_msg <- paste0(
    "API extraction failed for table '",
    CBRE_TABLE_NAME,
    "' ",
    "(window ",
    etl_window$start_time,
    " to ",
    etl_window$end_time,
    "): ",
    raw_data$error
  )
  log_daily_etl_run(
    api_name = API_NAME,
    script_name = SCRIPT_NAME,
    table_name = TABLE_NAME,
    duration = as.numeric(difftime(Sys.time(), task_start, units = "secs")),
    status = "FAILURE",
    message = error_msg
  )
  stop(error_msg)
}

if (raw_data$status == "no_data") {
  # API succeeded, nothing to load
  no_data_msg <- paste0(
    "No data returned from API for window ",
    etl_window$start_time,
    " to ",
    etl_window$end_time
  )
  cat(no_data_msg, "— nothing to load. Exiting gracefully.\n")
  log_daily_etl_run(
    api_name = API_NAME,
    script_name = SCRIPT_NAME,
    table_name = TABLE_NAME,
    duration = as.numeric(difftime(Sys.time(), task_start, units = "secs")),
    status = "NO_DATA",
    message = no_data_msg
  )
  cond <- structure(
    class = c("no_data_condition", "condition"),
    list(message = no_data_msg)
  )
  stop(cond)
}

EXCLUDED_FROM_HASH <- c(
  "bl_bl_id_key", # natural key itself — not a "value" to hash
  "md5_hash", # partner-supplied hash — not used
  "edp_last_updated_timestamp",
  "edp_update_ts",
  "source_system",
  "source_account_name",
  "bl_source_time_update", # source-side update timestamp; can tick
  "bl_source_date_update" # without any tracked field actually changing
)

# Initial Setup ####
# tracked_cols <- get_tracked_cols(clean_data)
#
# hashed <- add_row_hash(clean_data, tracked_cols) |>
#   mutate(bronze_load_ts = as.POSIXct(Sys.time(), tz = "UTC"))
#
# dbWriteTable(con, Id(schema = SCHEMA_NAME, table = TABLE_NAME), hashed)

# Regular run ####
data <- raw_data |>
  purrr::pluck("data")

tracked_cols <- get_tracked_cols(data)

input_data <- data |>
  rowwise() |>
  mutate(
    row_hash = purrr::pmap_chr(
      pick(all_of(tracked_cols)),
      ~ digest::digest(c(...), algo = "md5")
    )
  ) |>
  ungroup() |>
  mutate(
    bronze_load_ts = as.POSIXct(Sys.time(), tz = "UTC")
  )

classified_data <- classify_incoming(
  input_data,
  con,
  SCHEMA_NAME,
  TABLE_NAME,
  PRIMARY_KEY
)

last_bronze <- bronze_table |>
  slice_max(bronze_load_ts, n = 1, by = bl_bl_id_key) |>
  select(bl_bl_id_key, row_hash)

compared <- incoming |>
  left_join(last_bronze, by = "bl_bl_id_key", suffix = c("", "_prev"))

to_insert <- compared |>
  filter(is.na(row_hash_prev) | row_hash != row_hash_prev)
audit_rows <- compared |>
  mutate(
    action = case_when(
      is.na(row_hash_prev) ~ "NEW",
      row_hash != row_hash_prev ~ "CHANGED",
      TRUE ~ "UNCHANGED"
    )
  )

# write audit_rows to etl_ingestion_log always
# write to_insert to bronze_table only
# Database Transaction ####
# dbRemoveTable(con, Id(schema = SCHEMA_NAME, table = TABLE_NAME))
if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- paste0(
    "CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    TABLE_NAME,
    " (
        RefreshDate             DATETIME2(3)  NOT NULL,
        edp_update_ts           DATETIME2(3)  NOT NULL,
        PobcStatus              NVARCHAR(30)  NULL,
        BuildingId              NVARCHAR(20)  NOT NULL,
        PropertyId              NVARCHAR(20)  NULL,
        SiteId                  NVARCHAR(20)  NULL,
        linkAddress             NVARCHAR(150) NULL,
        linkCity                NVARCHAR(50)  NULL,
        Name                    NVARCHAR(150) NULL,
        Tenure                  NVARCHAR(30)  NULL,
        PrimaryUse              NVARCHAR(50)  NULL,
        StrategicClassification NVARCHAR(50)  NULL,
        FacilityType            NVARCHAR(50)  NULL,
        BuildingDate            NVARCHAR(50)  NULL,
        PricingMethod           NVARCHAR(50)  NULL,
        BookValueDate           DATETIME2(3)  NULL,
        BookValue               DECIMAL(18,2) NULL,
        CostsStartDate          DATETIME2(3)  NULL,
        PobcStartDate           DATETIME2(3)  NULL,
        PobcEndDate             DATETIME2(3)  NULL,
        MarketValueDate         DATETIME2(3)  NULL,
        MarketValue             DECIMAL(18,2) NULL,
        OccupancyStatus         NVARCHAR(30)  NULL,
        Option1                 BIT           NULL,
        bl_mam_predom_use       NVARCHAR(50)  NULL,
        bl_area_bl_comn_nocup   DECIMAL(18,5) NULL,
        bl_area_bl_comn_serv    DECIMAL(18,5) NULL,
        bl_area_ext_wall        DECIMAL(18,5) NULL,
        bl_area_gross_ext       DECIMAL(18,5) NULL,
        bl_area_gross_int       DECIMAL(18,5) NULL,
        bl_area_ls_negotiated   DECIMAL(18,5) NULL,
        bl_area_nocup           DECIMAL(18,5) NULL,
        bl_area_nocup_comn      DECIMAL(18,5) NULL,
        bl_area_nocup_dp        DECIMAL(18,5) NULL,
        bl_area_ocup            DECIMAL(18,5) NULL,
        bl_area_ocup_dp         DECIMAL(18,5) NULL,
        bl_area_remain          DECIMAL(18,5) NULL,
        bl_area_rentable        DECIMAL(18,5) NULL,
        bl_area_rm              DECIMAL(18,5) NULL,
        bl_area_rm_comn         DECIMAL(18,5) NULL,
        bl_area_rm_dp           DECIMAL(18,5) NULL,
        bl_area_serv            DECIMAL(18,5) NULL,
        bl_area_usable          DECIMAL(18,5) NULL,
        bl_area_vert_pen        DECIMAL(18,5) NULL,
        bl_lat                  DECIMAL(9,6)  NULL,
        bl_lon                  DECIMAL(9,6)  NULL
      );"
  )
  dbExecute(con, sql)
}

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
    CREATE TABLE ",
        SCHEMA_NAME,
        ".",
        TEMP_TABLE,
        " (
      RefreshDate             DATETIME2(3)  NOT NULL,
      edp_update_ts           DATETIME2(3)  NOT NULL,
      PobcStatus              NVARCHAR(30)  NULL,
      BuildingId              NVARCHAR(20)  NOT NULL,
      PropertyId              NVARCHAR(20)  NULL,
      SiteId                  NVARCHAR(20)  NULL,
      linkAddress             NVARCHAR(150) NULL,
      linkCity                NVARCHAR(50)  NULL,
      Name                    NVARCHAR(150) NULL,
      Tenure                  NVARCHAR(30)  NULL,
      PrimaryUse              NVARCHAR(50)  NULL,
      StrategicClassification NVARCHAR(50)  NULL,
      FacilityType            NVARCHAR(50)  NULL,
      BuildingDate            NVARCHAR(50)  NULL,
      PricingMethod           NVARCHAR(50)  NULL,
      BookValueDate           DATETIME2(3)  NULL,
      BookValue               DECIMAL(18,2) NULL,
      CostsStartDate          DATETIME2(3)  NULL,
      PobcStartDate           DATETIME2(3)  NULL,
      PobcEndDate             DATETIME2(3)  NULL,
      MarketValueDate         DATETIME2(3)  NULL,
      MarketValue             DECIMAL(18,2) NULL,
      OccupancyStatus         NVARCHAR(30)  NULL,
      Option1                 BIT           NULL,
      bl_mam_predom_use       NVARCHAR(50)  NULL,
      bl_area_bl_comn_nocup   DECIMAL(18,5) NULL,
      bl_area_bl_comn_serv    DECIMAL(18,5) NULL,
      bl_area_ext_wall        DECIMAL(18,5) NULL,
      bl_area_gross_ext       DECIMAL(18,5) NULL,
      bl_area_gross_int       DECIMAL(18,5) NULL,
      bl_area_ls_negotiated   DECIMAL(18,5) NULL,
      bl_area_nocup           DECIMAL(18,5) NULL,
      bl_area_nocup_comn      DECIMAL(18,5) NULL,
      bl_area_nocup_dp        DECIMAL(18,5) NULL,
      bl_area_ocup            DECIMAL(18,5) NULL,
      bl_area_ocup_dp         DECIMAL(18,5) NULL,
      bl_area_remain          DECIMAL(18,5) NULL,
      bl_area_rentable        DECIMAL(18,5) NULL,
      bl_area_rm              DECIMAL(18,5) NULL,
      bl_area_rm_comn         DECIMAL(18,5) NULL,
      bl_area_rm_dp           DECIMAL(18,5) NULL,
      bl_area_serv            DECIMAL(18,5) NULL,
      bl_area_usable          DECIMAL(18,5) NULL,
      bl_area_vert_pen        DECIMAL(18,5) NULL,
      bl_lat                  DECIMAL(9, 6) NULL,
      bl_lon                  DECIMAL(9, 6) NULL
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
        PobcStatus,
        BuildingId,
        PropertyId,
        SiteId,
        linkAddress,
        linkCity,
        Name,
        Tenure,
        PrimaryUse,
        StrategicClassification,
        FacilityType,
        BuildingDate,
        PricingMethod,
        BookValueDate,
        BookValue,
        CostsStartDate,
        PobcStartDate,
        PobcEndDate,
        MarketValueDate,
        MarketValue,
        OccupancyStatus,
        Option1,
        bl_mam_predom_use,
        bl_area_bl_comn_nocup,
        bl_area_bl_comn_serv,
        bl_area_ext_wall,
        bl_area_gross_ext,
        bl_area_gross_int,
        bl_area_ls_negotiated,
        bl_area_nocup,
        bl_area_nocup_comn,
        bl_area_nocup_dp,
        bl_area_ocup,
        bl_area_ocup_dp,
        bl_area_remain,
        bl_area_rentable,
        bl_area_rm,
        bl_area_rm_comn,
        bl_area_rm_dp,
        bl_area_serv,
        bl_area_usable,
        bl_area_vert_pen,
        bl_lat,
        bl_lon
      )
       SELECT * FROM ",
        TEMP_TABLE,
        ";"
      )
    )

    # Complete the transaction
    dbCommit(con)

    # Hoist to main environment
    n_inserted <<- n_inserted
    cat("ETL complete — inserted:", n_inserted, "\n")
    # Rollback transaction on failure
  },
  error = function(e) {
    dbRollback(con)
    etl_error <<- e
  }
)

task_end <- Sys.time()
task_duration <- interval(task_start, task_end) / dseconds()

if (is.null(etl_error)) {
  log_daily_etl_run(
    api_name = API_NAME,
    script_name = SCRIPT_NAME,
    table_name = TABLE_NAME,
    duration = task_duration,
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
