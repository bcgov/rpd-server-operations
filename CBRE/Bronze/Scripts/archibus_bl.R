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
BATCH_ID <- as.numeric(format(task_start, "%Y%m%d%H%M%S", tz = "UTC"))
AUDIT_TABLE <- DBI::Id(schema = "ServerLogs", table = SCHEMA_NAME)

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


# dbRemoveTable(con, TARGET_TABLE)
if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- glue::glue(
    "CREATE TABLE
    {SCHEMA_NAME}.{TABLE_NAME}
    (
      bl_ac_id                         VARCHAR(100) NULL,
      bl_address1                      VARCHAR(100) NULL,
      bl_address2                      VARCHAR(100) NULL,
      bl_age                           VARCHAR(50)  NULL,
      bl_area_avg_em                   VARCHAR(50)  NULL,
      bl_area_avg_floor                VARCHAR(50)  NULL,
      bl_area_bl_comn_gp               VARCHAR(50)  NULL,
      bl_area_bl_comn_nocup            VARCHAR(50)  NULL,
      bl_area_bl_comn_ocup             VARCHAR(50)  NULL,
      bl_area_bl_comn_rm               VARCHAR(50)  NULL,
      bl_area_bl_comn_serv             VARCHAR(50)  NULL,
      bl_area_em_dp                    VARCHAR(50)  NULL,
      bl_area_ext_wall                 VARCHAR(50)  NULL,
      bl_area_gp                       VARCHAR(50)  NULL,
      bl_area_gp_comn                  VARCHAR(50)  NULL,
      bl_area_gp_dp                    VARCHAR(50)  NULL,
      bl_area_gross_ext                VARCHAR(50)  NULL,
      bl_area_gross_int                VARCHAR(50)  NULL,
      bl_area_ls_negotiated            VARCHAR(50)  NULL,
      bl_area_nocup                    VARCHAR(50)  NULL,
      bl_area_nocup_comn               VARCHAR(50)  NULL,
      bl_area_nocup_dp                 VARCHAR(50)  NULL,
      bl_area_ocup                     VARCHAR(50)  NULL,
      bl_area_ocup_comn                VARCHAR(50)  NULL,
      bl_area_ocup_dp                  VARCHAR(50)  NULL,
      bl_area_remain                   VARCHAR(50)  NULL,
      bl_area_rentable                 VARCHAR(50)  NULL,
      bl_area_rm                       VARCHAR(50)  NULL,
      bl_area_rm_comn                  VARCHAR(50)  NULL,
      bl_area_rm_dp                    VARCHAR(50)  NULL,
      bl_area_serv                     VARCHAR(50)  NULL,
      bl_area_su                       VARCHAR(50)  NULL,
      bl_area_usable                   VARCHAR(50)  NULL,
      bl_area_vert_pen                 VARCHAR(50)  NULL,
      bl_auto_est_balance_points       VARCHAR(50)  NULL,
      bl_bl_ci                         VARCHAR(100) NULL,
      bl_bl_id_key                     VARCHAR(50)  NOT NULL,
      bl_bl_number                     VARCHAR(100) NULL,
      bl_bl_status_year                VARCHAR(100) NULL,
      bl_bl_use_val                    VARCHAR(100) NULL,
      bl_bldg_photo                    VARCHAR(100) NULL,
      bl_campus                        VARCHAR(100) NULL,
      bl_campus_id                     VARCHAR(100) NULL,
      bl_city_id                       VARCHAR(100) NULL,
      bl_comment_disposal              VARCHAR(500) NULL,
      bl_comments                      VARCHAR(500) NULL,
      bl_complex_id                    VARCHAR(100) NULL,
      bl_condition                     VARCHAR(50)  NULL,
      bl_construction_type             VARCHAR(50)  NULL,
      bl_construction_type_val         VARCHAR(100) NULL,
      bl_contact_email                 VARCHAR(100) NULL,
      bl_contact_name                  VARCHAR(100) NULL,
      bl_contact_phone                 VARCHAR(100) NULL,
      bl_cooling_balance_point         VARCHAR(50)  NULL,
      bl_cooling_balance_point_manual  VARCHAR(50)  NULL,
      bl_cost_operating_total          VARCHAR(50)  NULL,
      bl_cost_other_total              VARCHAR(50)  NULL,
      bl_cost_replace                  VARCHAR(50)  NULL,
      bl_cost_sqft                     VARCHAR(50)  NULL,
      bl_cost_tax_total                VARCHAR(50)  NULL,
      bl_cost_utility_total            VARCHAR(50)  NULL,
      bl_count_em                      VARCHAR(50)  NULL,
      bl_count_fl                      VARCHAR(50)  NULL,
      bl_count_ls                      VARCHAR(50)  NULL,
      bl_count_max_occup               VARCHAR(50)  NULL,
      bl_count_occup                   VARCHAR(50)  NULL,
      bl_criticality                   VARCHAR(50)  NULL,
      bl_ctry_id                       VARCHAR(50)  NULL,
      bl_date_bl                       VARCHAR(30)  NULL,
      bl_date_book_val                 VARCHAR(30)  NULL,
      bl_date_costs_end                VARCHAR(30)  NULL,
      bl_date_costs_last_calcd         VARCHAR(30)  NULL,
      bl_date_costs_start              VARCHAR(30)  NULL,
      bl_date_disposal                 VARCHAR(30)  NULL,
      bl_date_end_pobc                 VARCHAR(30)  NULL,
      bl_date_market_val               VARCHAR(30)  NULL,
      bl_date_rehab                    VARCHAR(30)  NULL,
      bl_date_start_pobc               VARCHAR(30)  NULL,
      bl_detail_dwg                    VARCHAR(100) NULL,
      bl_disposal_type                 VARCHAR(50)  NULL,
      bl_dwgname                       VARCHAR(100) NULL,
      bl_ehandle                       VARCHAR(100) NULL,
      bl_energy_baseline_year          VARCHAR(50)  NULL,
      bl_facility_type                 VARCHAR(100) NULL,
      bl_fasb_ls_type                  VARCHAR(50)  NULL,
      bl_geo_objectid                  VARCHAR(100) NULL,
      bl_grp_uid                       VARCHAR(100) NULL,
      bl_heating_balance_point         VARCHAR(50)  NULL,
      bl_heating_balance_point_manual  VARCHAR(50)  NULL,
      bl_image_file                    VARCHAR(100) NULL,
      bl_income_total                  VARCHAR(50)  NULL,
      bl_is_bl_addacc                  VARCHAR(50)  NULL,
      bl_is_bl_hist                    VARCHAR(50)  NULL,
      bl_is_bl_hsecurity               VARCHAR(50)  NULL,
      bl_is_child_occupied             VARCHAR(50)  NULL,
      bl_lat                           VARCHAR(100) NULL,
      bl_leed_other                    VARCHAR(50)  NULL,
      bl_leedcl                        VARCHAR(100) NULL,
      bl_leedeb                        VARCHAR(100) NULL,
      bl_leednc                        VARCHAR(100) NULL,
      bl_legal_id                      VARCHAR(100) NULL,
      bl_lon                           VARCHAR(100) NULL,
      bl_mam_predom_use                VARCHAR(50)  NULL,
      bl_name                          VARCHAR(100) NULL,
      bl_occ_status                    VARCHAR(50)  NULL,
      bl_occup_target                  VARCHAR(50)  NULL,
      bl_option1                       VARCHAR(50)  NULL,
      bl_option2                       VARCHAR(100) NULL,
      bl_pending_action                VARCHAR(50)  NULL,
      bl_perimeter                     VARCHAR(100) NULL,
      bl_pr_id                         VARCHAR(50)  NULL,
      bl_pricing_method                VARCHAR(100) NULL,
      bl_primary_use                   VARCHAR(100) NULL,
      bl_qty_life_expect               VARCHAR(50)  NULL,
      bl_ratio_ru                      VARCHAR(50)  NULL,
      bl_ratio_ur                      VARCHAR(50)  NULL,
      bl_regn_id                       VARCHAR(100) NULL,
      bl_serv_provider                 VARCHAR(100) NULL,
      bl_site_id                       VARCHAR(50)  NULL,
      bl_social_distance               VARCHAR(50)  NULL,
      bl_source_date_update            VARCHAR(30)  NULL,
      bl_source_feed_comments          VARCHAR(500) NULL,
      bl_source_record_id              VARCHAR(100) NULL,
      bl_source_status                 VARCHAR(50)  NULL,
      bl_source_system_id              VARCHAR(100) NULL,
      bl_source_table                  VARCHAR(100) NULL,
      bl_source_time_update            VARCHAR(30)  NULL,
      bl_stat_life_remain              VARCHAR(50)  NULL,
      bl_state_id                      VARCHAR(50)  NULL,
      bl_status                        VARCHAR(50)  NULL,
      bl_status_pobc                   VARCHAR(50)  NULL,
      bl_std_area_per_em               VARCHAR(50)  NULL,
      bl_strategic_class               VARCHAR(100) NULL,
      bl_structure_type                VARCHAR(50)  NULL,
      bl_url                           VARCHAR(500) NULL,
      bl_use1                          VARCHAR(100) NULL,
      bl_utility_type_cool             VARCHAR(100) NULL,
      bl_utility_type_heat             VARCHAR(100) NULL,
      bl_value_bldg                    VARCHAR(50)  NULL,
      bl_value_book                    VARCHAR(50)  NULL,
      bl_value_deprec_remain           VARCHAR(50)  NULL,
      bl_value_extras                  VARCHAR(50)  NULL,
      bl_value_land                    VARCHAR(50)  NULL,
      bl_value_market                  VARCHAR(100) NULL,
      bl_weather_source_id             VARCHAR(100) NULL,
      bl_weather_station_id            VARCHAR(100) NULL,
      bl_zip                           VARCHAR(50)  NULL,
      md5_hash                         CHAR(32)     NULL,
      edp_last_updated_timestamp       VARCHAR(30)  NULL,
      source_system                    VARCHAR(50)  NULL,
      source_account_name              VARCHAR(50)  NULL,
      edp_update_ts                    VARCHAR(30)  NULL,
      row_hash                         CHAR(32)     NOT NULL,
      bronze_batch_id                  BIGINT       NOT NULL,
      bronze_load_ts                   DATETIME2(0) NOT NULL,
      CONSTRAINT PK_bronze_building PRIMARY KEY CLUSTERED ({PRIMARY_KEY}, bronze_load_ts)
    );"
  )
  dbExecute(con, sql)
}

# Initial Setup ####
# data <- raw_data |>
#   purrr::pluck("data")
#
# tracked_cols <- get_tracked_cols(data)
#
# hashed <- add_row_hash(data, tracked_cols) |>
#   mutate(
#     bronze_load_ts = as.POSIXct(task_start, tz = "UTC"),
#     bronze_batch_id = BATCH_ID
#   )
#
# DBI::dbAppendTable(con, TARGET_TABLE, hashed)

# Regular run ####
data <- raw_data |>
  purrr::pluck("data")

tracked_cols <- get_tracked_cols(data, EXCLUDED_FROM_HASH)

hashed <- data |>
  add_row_hash(PRIMARY_KEY, tracked_cols) |>
  mutate(
    bronze_load_ts = as.POSIXct(task_start, tz = "UTC"),
    bronze_batch_id = BATCH_ID
  )

classified_data <- classify_incoming(
  hashed,
  con,
  SCHEMA_NAME,
  TABLE_NAME,
  PRIMARY_KEY
)

apply_hash_gate(
  con,
  classified_data,
  PRIMARY_KEY,
  TARGET_TABLE,
  AUDIT_TABLE,
  API_NAME,
  CBRE_TABLE_NAME,
  BATCH_ID,
  task_start
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
