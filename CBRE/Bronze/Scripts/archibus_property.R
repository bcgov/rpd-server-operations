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
TABLE_NAME <- "archibus_property"
CBRE_TABLE_NAME <- "archibus_property"
PRIMARY_KEY <- "property_pr_id"
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
  "property_pr_id", # natural key itself — not a "value" to hash
  "md5_hash", # partner-supplied hash — not used
  "edp_last_updated_timestamp",
  "edp_update_ts",
  "source_system",
  "source_account_name"
)

# dbRemoveTable(con, TARGET_TABLE)
if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- glue::glue(
    "CREATE TABLE
    {SCHEMA_NAME}.{TABLE_NAME}
    (
      property_ac_id                      VARCHAR(100) NULL,
      property_address1                   VARCHAR(100) NULL,
      property_address2                   VARCHAR(100) NULL,
      property_air_dist                   VARCHAR(50)  NULL,
      property_air_name                   VARCHAR(100) NULL,
      property_area_bl_est_rentable       VARCHAR(50)  NULL,
      property_area_bl_gross_int          VARCHAR(50)  NULL,
      property_area_bl_rentable           VARCHAR(50)  NULL,
      property_area_bl_usable             VARCHAR(50)  NULL,
      property_area_cad                   VARCHAR(50)  NULL,
      property_area_land_acres            VARCHAR(50)  NULL,
      property_area_lease_meas            VARCHAR(50)  NULL,
      property_area_lease_neg             VARCHAR(50)  NULL,
      property_area_manual                VARCHAR(50)  NULL,
      property_area_non_permeable         VARCHAR(50)  NULL,
      property_area_parcel                VARCHAR(50)  NULL,
      property_area_parking_total         VARCHAR(50)  NULL,
      property_area_prkg_acres            VARCHAR(50)  NULL,
      property_area_total_permeable       VARCHAR(50)  NULL,
      property_city_id                    VARCHAR(100) NULL,
      property_comment_disposal           VARCHAR(500) NULL,
      property_comments                   VARCHAR(500) NULL,
      property_condition                  VARCHAR(50)  NULL,
      property_contact1                   VARCHAR(100) NULL,
      property_contact2                   VARCHAR(100) NULL,
      property_cost_irr                   VARCHAR(50)  NULL,
      property_cost_operating_total       VARCHAR(50)  NULL,
      property_cost_other_total           VARCHAR(50)  NULL,
      property_cost_purchase              VARCHAR(50)  NULL,
      property_cost_roi                   VARCHAR(50)  NULL,
      property_cost_selling               VARCHAR(50)  NULL,
      property_cost_tax_total             VARCHAR(50)  NULL,
      property_cost_utility_total         VARCHAR(50)  NULL,
      property_county_id                  VARCHAR(100) NULL,
      property_criticality                VARCHAR(50)  NULL,
      property_ctry_id                    VARCHAR(50)  NULL,
      property_date_book_val              VARCHAR(30)  NULL,
      property_date_costs_end             VARCHAR(30)  NULL,
      property_date_costs_last_calcd      VARCHAR(30)  NULL,
      property_date_costs_start           VARCHAR(30)  NULL,
      property_date_disposal              VARCHAR(30)  NULL,
      property_date_end_pobc              VARCHAR(30)  NULL,
      property_date_market_val            VARCHAR(30)  NULL,
      property_date_purchase              VARCHAR(30)  NULL,
      property_date_sold                  VARCHAR(30)  NULL,
      property_date_start_pobc            VARCHAR(30)  NULL,
      property_description                VARCHAR(500) NULL,
      property_detail_dwg                 VARCHAR(100) NULL,
      property_disposal_type              VARCHAR(50)  NULL,
      property_dwgname                    VARCHAR(100) NULL,
      property_ehandle                    VARCHAR(100) NULL,
      property_fronts                     VARCHAR(100) NULL,
      property_geo_objectid               VARCHAR(100) NULL,
      property_grp_uid                    VARCHAR(100) NULL,
      property_image_file                 VARCHAR(100) NULL,
      property_image_map                  VARCHAR(100) NULL,
      property_income_total               VARCHAR(50)  NULL,
      property_int_dist                   VARCHAR(50)  NULL,
      property_int_name                   VARCHAR(100) NULL,
      property_land_is                    VARCHAR(50)  NULL,
      property_lat                        VARCHAR(100) NULL,
      property_latlon_verify              VARCHAR(50)  NULL,
      property_lon                        VARCHAR(100) NULL,
      property_name                       VARCHAR(100) NULL,
      property_occ_status                 VARCHAR(50)  NULL,
      property_option1                    VARCHAR(50)  NULL,
      property_option2                    VARCHAR(100) NULL,
      property_other_name                 VARCHAR(100) NULL,
      property_pct_own                    VARCHAR(50)  NULL,
      property_pending_action             VARCHAR(50)  NULL,
      property_pr_id                      VARCHAR(50)  NOT NULL,
      property_pricing_method             VARCHAR(100) NULL,
      property_primary_use                VARCHAR(100) NULL,
      property_prop_is                    VARCHAR(50)  NULL,
      property_prop_photo                 VARCHAR(100) NULL,
      property_property_type              VARCHAR(100) NULL,
      property_purchased_from             VARCHAR(100) NULL,
      property_qty_headcount              VARCHAR(50)  NULL,
      property_qty_no_bldgs               VARCHAR(50)  NULL,
      property_qty_no_bldgs_calc          VARCHAR(50)  NULL,
      property_qty_no_spaces              VARCHAR(50)  NULL,
      property_qty_no_spaces_calc         VARCHAR(50)  NULL,
      property_qty_occupancy              VARCHAR(50)  NULL,
      property_qty_su_occupancy           VARCHAR(50)  NULL,
      property_regn_id                    VARCHAR(100) NULL,
      property_selling_broker             VARCHAR(100) NULL,
      property_selling_commission         VARCHAR(50)  NULL,
      property_serv_provider              VARCHAR(100) NULL,
      property_services                   VARCHAR(100) NULL,
      property_site_id                    VARCHAR(50)  NULL,
      property_sold_to                    VARCHAR(100) NULL,
      property_state_id                   VARCHAR(50)  NULL,
      property_status                     VARCHAR(50)  NULL,
      property_status_pobc                VARCHAR(50)  NULL,
      property_strategic_class            VARCHAR(100) NULL,
      property_street                     VARCHAR(100) NULL,
      property_tax_rate_prop              VARCHAR(50)  NULL,
      property_tax_rate_school            VARCHAR(50)  NULL,
      property_unit                       VARCHAR(50)  NULL,
      property_url                        VARCHAR(500) NULL,
      property_use1                       VARCHAR(100) NULL,
      property_value_assessed_prop_tax    VARCHAR(50)  NULL,
      property_value_assessed_school_tax  VARCHAR(50)  NULL,
      property_value_bldg                 VARCHAR(50)  NULL,
      property_value_book                 VARCHAR(50)  NULL,
      property_value_extras               VARCHAR(50)  NULL,
      property_value_land                 VARCHAR(50)  NULL,
      property_value_market               VARCHAR(50)  NULL,
      property_vicinity                   VARCHAR(100) NULL,
      property_zip                        VARCHAR(50)  NULL,
      property_zoning                     VARCHAR(50)  NULL,
      md5_hash                            CHAR(32)     NULL,
      source_system                       VARCHAR(50)  NULL,
      source_account_name                 VARCHAR(50)  NULL,
      edp_last_updated_timestamp          VARCHAR(100) NULL,
      edp_update_ts                       VARCHAR(30)  NULL,
      row_hash                            CHAR(32)     NOT NULL,
      bronze_batch_id                     BIGINT       NOT NULL,
      bronze_load_ts                      DATETIME2(0) NOT NULL,
      CONSTRAINT PK_bronze_property PRIMARY KEY CLUSTERED ({PRIMARY_KEY}, bronze_load_ts)
    );"
  )
  dbExecute(con, sql)
}

# Initial Setup ####
# data <- raw_data |>
#   purrr::pluck("data")
#
# tracked_cols <- get_tracked_cols(data, EXCLUDED_FROM_HASH)
#
# hashed <- add_row_hash(data, PRIMARY_KEY, tracked_cols) |>
#   mutate(
#     bronze_load_ts = as.POSIXct(task_start, tz = "UTC"),
#     bronze_batch_id = BATCH_ID
#   )

# DBI::dbAppendTable(con, TARGET_TABLE, hashed)

etl_error <- NULL

# Hash and Classify Data ####
tryCatch(
  {
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
  },
  error = function(e) {
    log_etl_error(
      api_name = API_NAME,
      script_name = SCRIPT_NAME,
      table_name = TABLE_NAME,
      step = "hash_classify",
      condition = e
    )
    etl_error <<- e
  }
)

# Check for Existing Data not in current API pull ####
if (is.null(etl_error)) {
  tryCatch(
    {
      missing <- find_missing_from_pull(
        hashed,
        con,
        SCHEMA_NAME,
        TABLE_NAME,
        PRIMARY_KEY,
        status_col = "property_status_pobc"
      )
    },
    error = function(e) {
      log_etl_error(
        api_name = API_NAME,
        script_name = SCRIPT_NAME,
        table_name = TABLE_NAME,
        step = "find_missing",
        condition = e
      )
      etl_error <<- e
    }
  )
}

if (is.null(etl_error)) {
  tryCatch(
    {
      log_row <- apply_hash_gate(
        con,
        classified_data,
        TARGET_TABLE,
        API_NAME,
        CBRE_TABLE_NAME,
        BATCH_ID,
        task_start
      )

      audit_row <- log_row |>
        mutate(
          Missing = nrow(missing),
          Duration = round(
            (interval(task_start, Sys.time()) / dseconds()),
            digits = 2
          )
        )

      DBI::dbAppendTable(con, AUDIT_TABLE, audit_row)

      cat(
        "ETL complete — Audit Row Written:",
        audit_row$New,
        " new, ",
        audit_row$Changed,
        " changed, and ",
        audit_row$Unchanged,
        " unchanged.",
        "\n"
      )
    },
    error = function(e) {
      log_etl_error(
        api_name = API_NAME,
        script_name = SCRIPT_NAME,
        table_name = TABLE_NAME,
        step = "apply_hash_gate",
        condition = e
      )
      etl_error <<- e
    }
  )
}

task_end <- Sys.time()
task_duration <- interval(task_start, task_end) / dseconds()


if (is.null(etl_error)) {
  log_daily_etl_run(
    api_name = API_NAME,
    script_name = SCRIPT_NAME,
    table_name = TABLE_NAME,
    duration = task_duration,
    status = "SUCCESS",
    n_inserted = audit_row$New,
    n_updated = audit_row$Changed,
    n_deleted = nrow(missing),
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
