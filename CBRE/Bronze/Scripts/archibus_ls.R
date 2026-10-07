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
TABLE_NAME <- "archibus_ls"
CBRE_TABLE_NAME <- "archibus_ls"
PRIMARY_KEY <- "ls_ls_id"
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
  "ls_ls_id", # natural key itself — not a "value" to hash
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
      ls_ac_id                        VARCHAR(100)  NULL,
      ls_activity_log_id              VARCHAR(100)  NULL,
      ls_amendment                    VARCHAR(50)   NULL,
      ls_amount_base_rent             VARCHAR(50)   NULL,
      ls_amount_operating             VARCHAR(50)   NULL,
      ls_amount_other                 VARCHAR(50)   NULL,
      ls_amount_pct_rent              VARCHAR(50)   NULL,
      ls_amount_security              VARCHAR(50)   NULL,
      ls_amount_taxes                 VARCHAR(50)   NULL,
      ls_amount_tot_rent_exp          VARCHAR(50)   NULL,
      ls_amount_tot_rent_inc          VARCHAR(50)   NULL,
      ls_appropriated_hectares        VARCHAR(50)   NULL,
      ls_appropriated_parking_stalls  VARCHAR(50)   NULL,
      ls_appropriated_sqm             VARCHAR(50)   NULL,
      ls_area_bl_total                VARCHAR(50)   NULL,
      ls_area_common                  VARCHAR(50)   NULL,
      ls_area_negotiated              VARCHAR(50)   NULL,
      ls_area_pr_total                VARCHAR(50)   NULL,
      ls_area_rentable                VARCHAR(50)   NULL,
      ls_area_usable                  VARCHAR(50)   NULL,
      ls_automatic_renewal            VARCHAR(50)   NULL,
      ls_below_cap_threshold          VARCHAR(50)   NULL,
      ls_bl_id                        VARCHAR(50)   NULL,
      ls_bl_tot_rentable_area         VARCHAR(50)   NULL,
      ls_blended_area                 VARCHAR(50)   NULL,
      ls_class_wiz_step1_done         VARCHAR(50)   NULL,
      ls_class_wiz_step2_done         VARCHAR(50)   NULL,
      ls_class_wiz_step3_done         VARCHAR(50)   NULL,
      ls_commence_clause              VARCHAR(50)   NULL,
      ls_comments                     VARCHAR(2000) NULL,
      ls_cost_index                   VARCHAR(50)   NULL,
      ls_date_base_tax_end            VARCHAR(30)   NULL,
      ls_date_base_tax_start          VARCHAR(30)   NULL,
      ls_date_commencement            VARCHAR(30)   NULL,
      ls_date_cost_anal_end           VARCHAR(30)   NULL,
      ls_date_cost_anal_start         VARCHAR(30)   NULL,
      ls_date_costs_last_calcd        VARCHAR(30)   NULL,
      ls_date_end                     VARCHAR(30)   NULL,
      ls_date_end_fasb                VARCHAR(30)   NULL,
      ls_date_inception               VARCHAR(30)   NULL,
      ls_date_move                    VARCHAR(30)   NULL,
      ls_date_oprt_cost_end           VARCHAR(30)   NULL,
      ls_date_oprt_cost_start         VARCHAR(30)   NULL,
      ls_date_signed                  VARCHAR(30)   NULL,
      ls_date_start                   VARCHAR(30)   NULL,
      ls_date_terminated              VARCHAR(30)   NULL,
      ls_description                  VARCHAR(1000) NULL,
      ls_doc                          VARCHAR(100)  NULL,
      ls_eq_id                        VARCHAR(100)  NULL,
      ls_fasb_ls_class                VARCHAR(50)   NULL,
      ls_fasb_ls_legacy               VARCHAR(50)   NULL,
      ls_fasb_ls_type                 VARCHAR(50)   NULL,
      ls_fasb_review_status           VARCHAR(50)   NULL,
      ls_fasb_workflow_status         VARCHAR(50)   NULL,
      ls_floors                       VARCHAR(500)  NULL,
      ls_has_options                  VARCHAR(50)   NULL,
      ls_hpattern                     VARCHAR(100)  NULL,
      ls_hpattern_acad                VARCHAR(100)  NULL,
      ls_hvac_light_sys               VARCHAR(50)   NULL,
      ls_iasb_retro_method            VARCHAR(50)   NULL,
      ls_image_doc1                   VARCHAR(100)  NULL,
      ls_image_doc2                   VARCHAR(100)  NULL,
      ls_image_doc3                   VARCHAR(100)  NULL,
      ls_increm_borrow_rate           VARCHAR(50)   NULL,
      ls_initial_direct_cost          VARCHAR(50)   NULL,
      ls_initial_lease_liability      VARCHAR(50)   NULL,
      ls_is_auto_transfer             VARCHAR(50)   NULL,
      ls_is_early_termination         VARCHAR(50)   NULL,
      ls_is_facility_specialized      VARCHAR(50)   NULL,
      ls_is_gross_rent                VARCHAR(50)   NULL,
      ls_is_index_included            VARCHAR(50)   NULL,
      ls_is_landlord_allowance        VARCHAR(50)   NULL,
      ls_is_lease_contracted          VARCHAR(50)   NULL,
      ls_is_lease_expanded            VARCHAR(50)   NULL,
      ls_is_lease_near_eol            VARCHAR(50)   NULL,
      ls_is_npv_exceed_90_fmv         VARCHAR(50)   NULL,
      ls_is_option_to_buy             VARCHAR(50)   NULL,
      ls_is_parking_designated        VARCHAR(50)   NULL,
      ls_is_purchase_option           VARCHAR(50)   NULL,
      ls_is_renewal_changed           VARCHAR(50)   NULL,
      ls_is_rent_below_market         VARCHAR(50)   NULL,
      ls_is_residual_unknown          VARCHAR(50)   NULL,
      ls_is_short_term_lease          VARCHAR(50)   NULL,
      ls_is_term_exceed_life          VARCHAR(50)   NULL,
      ls_land_description             VARCHAR(2000) NULL,
      ls_landlord_tenant              VARCHAR(50)   NULL,
      ls_ld_contact                   VARCHAR(300)  NULL,
      ls_ld_name                      VARCHAR(100)  NULL,
      ls_lease_deprec_per             VARCHAR(50)   NULL,
      ls_lease_implicit_rate          VARCHAR(50)   NULL,
      ls_lease_payments               VARCHAR(50)   NULL,
      ls_lease_residual_value         VARCHAR(50)   NULL,
      ls_lease_sublease               VARCHAR(50)   NULL,
      ls_lease_term                   VARCHAR(50)   NULL,
      ls_lease_term_per               VARCHAR(50)   NULL,
      ls_lease_term_remain            VARCHAR(50)   NULL,
      ls_lease_term_remain_per        VARCHAR(50)   NULL,
      ls_lease_type                   VARCHAR(50)   NULL,
      ls_ll_corp_sign                 VARCHAR(50)   NULL,
      ls_ll_indi_sign                 VARCHAR(50)   NULL,
      ls_ls_id                        VARCHAR(100)  NOT NULL,
      ls_ls_parent_id                 VARCHAR(100)  NULL,
      ls_multi_tenant                 VARCHAR(50)   NULL,
      ls_non_standard                 VARCHAR(50)   NULL,
      ls_num_sched_add                VARCHAR(50)   NULL,
      ls_op_cost_le_rate              VARCHAR(50)   NULL,
      ls_op_cost_li_rate              VARCHAR(50)   NULL,
      ls_op_cost_type                 VARCHAR(50)   NULL,
      ls_option1                      VARCHAR(50)   NULL,
      ls_option2                      VARCHAR(50)   NULL,
      ls_owned                        VARCHAR(50)   NULL,
      ls_park_days_notice             VARCHAR(50)   NULL,
      ls_park_max_random              VARCHAR(50)   NULL,
      ls_payments_scheduled           VARCHAR(50)   NULL,
      ls_pct_appropriated             VARCHAR(50)   NULL,
      ls_pct_liability_over_fmv       VARCHAR(50)   NULL,
      ls_pct_term_over_life           VARCHAR(50)   NULL,
      ls_pr_id                        VARCHAR(50)   NULL,
      ls_project_id                   VARCHAR(100)  NULL,
      ls_qty_occupancy                VARCHAR(50)   NULL,
      ls_qty_suite_occupancy          VARCHAR(50)   NULL,
      ls_reconciliation               VARCHAR(50)   NULL,
      ls_reduction_pct                VARCHAR(50)   NULL,
      ls_reduction_space              VARCHAR(50)   NULL,
      ls_remaining_life_bldg          VARCHAR(50)   NULL,
      ls_remit_to                     VARCHAR(200)  NULL,
      ls_rentable_land_acres          VARCHAR(50)   NULL,
      ls_reorg_ls_id                  VARCHAR(100)  NULL,
      ls_reorg_status                 VARCHAR(50)   NULL,
      ls_reorg_tn                     VARCHAR(50)   NULL,
      ls_reorg_type                   VARCHAR(50)   NULL,
      ls_reports                      VARCHAR(500)  NULL,
      ls_saving_loss                  VARCHAR(50)   NULL,
      ls_share_prpnt                  VARCHAR(50)   NULL,
      ls_signed                       VARCHAR(50)   NULL,
      ls_space_use                    VARCHAR(50)   NULL,
      ls_standard_docs                VARCHAR(50)   NULL,
      ls_status                       VARCHAR(50)   NULL,
      ls_straight_line_exp            VARCHAR(50)   NULL,
      ls_tax_bill_num                 VARCHAR(300)  NULL,
      ls_tax_prop_share               VARCHAR(50)   NULL,
      ls_tax_rate_per_sf              VARCHAR(50)   NULL,
      ls_tax_type                     VARCHAR(50)   NULL,
      ls_template_name                VARCHAR(100)  NULL,
      ls_terms                        VARCHAR(50)   NULL,
      ls_tn_contact                   VARCHAR(100)  NULL,
      ls_tn_name                      VARCHAR(100)  NULL,
      ls_translink                    VARCHAR(50)   NULL,
      ls_unique_share                 VARCHAR(50)   NULL,
      ls_use_as_template              VARCHAR(50)   NULL,
      ls_value_bldg                   VARCHAR(50)   NULL,
      ls_vat_exclude                  VARCHAR(50)   NULL,
      ls_version                      VARCHAR(50)   NULL,
      md5_hash                        CHAR(32)      NULL,
      edp_last_updated_timestamp      VARCHAR(100)  NULL,
      source_system                   VARCHAR(50)   NULL,
      source_account_name             VARCHAR(50)   NULL,
      edp_update_ts                   VARCHAR(30)   NULL,
      row_hash                        CHAR(32)      NOT NULL,
      bronze_batch_id                 BIGINT        NOT NULL,
      bronze_load_ts                  DATETIME2(0)  NOT NULL,
      CONSTRAINT PK_bronze_ls PRIMARY KEY CLUSTERED ({PRIMARY_KEY}, bronze_load_ts)
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
#
# str(hashed, max.level = 2, vec.len = 0, list.len = Inf)
# max_char_lengths(hashed)
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
        status_col = "ls_status"
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
