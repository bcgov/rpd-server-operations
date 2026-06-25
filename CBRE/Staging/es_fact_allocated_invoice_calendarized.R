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
SCHEMA_NAME <- "CbreStaging"
TABLE_NAME <- "es_fact_allocated_invoice_calendarized"
CBRE_TABLE_NAME <- "es_fact_allocated_invoice_calendarized_vw"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
# etl_window <- get_etl_window()
API_NAME <- "CBRE"
SCRIPT_NAME <- "es_fact_allocated_invoice_calendarized"

# Connect to SQL database
con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = SQL_SERVER,
  database = DB_NAME,
  Trusted_Connection = "Yes"
)

# Query API
raw_data <- call_cbre_api(
  CBRE_TABLE_NAME,
  start_time = etl_window$start_time,
  end_time = etl_window$end_time
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

clean_data <- raw_data |>
  purrr::pluck("data")

###################################################################
CBRE_TABLE_NAME <- "es_fact_allocated_invoice_calendarized_vw"

chunk_1 <- call_cbre_api(
  CBRE_TABLE_NAME,
  start_time = "2010-04-01T00:00:00Z",
  end_time = "2024-06-01T00:00:00Z"
)

raw_data <- chunk_1$data

es_fact_allocated_invoice_calendarized_vw <- raw_data

clean_data <- es_fact_allocated_invoice_calendarized_vw |>
  select(
    reporting_group_skey,
    property_skey,
    vendor_skey,
    invoice_skey,
    service_account_skey,
    period_start_date,
    period_end_date,
    service_type,
    utility_type_group,
    service_type_secondary,
    estimated_usage,
    actual_usage,
    actual_usage_uom,
    estimated_cost,
    service_cost_optimized_amount,
    factor_ch4,
    factor_co2,
    factor_n2o,
    co2e_rate,
    previous_year_co2e_rate,
    gj_factor,
    m_conversion,
    l_conversion,
    m2_conversion,
    kg_conversion,
    uncertainty_factor,
    scope_desc,
    edp_create_ts,
    edp_update_ts
  ) |>
  mutate(
    across(
      c(
        period_start_date,
        period_end_date,
        edp_create_ts,
        edp_update_ts
      ),
      ~ as.POSIXct(.x, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC")
    )
  ) |>
  mutate(
    across(
      c(
        actual_usage,
        estimated_usage,
        estimated_cost,
        service_cost_optimized_amount,
        factor_ch4,
        factor_co2,
        factor_n2o,
        co2e_rate,
        previous_year_co2e_rate,
        gj_factor,
        m_conversion,
        l_conversion,
        m2_conversion,
        kg_conversion,
        uncertainty_factor
      ),
      as.double
    )
  ) |>
  mutate(
    across(
      c(
        reporting_group_skey,
        property_skey,
        vendor_skey,
        invoice_skey,
        service_account_skey
      ),
      as.character
    )
  ) |>
  mutate(RefreshDate = Sys.time(), .before = everything())

test <- clean_data |>
  group_by(
    reporting_group_skey,
    property_skey,
    invoice_skey
  ) |>
  mutate(count = n()) |>
  filter(count >= 2)

# dbRemoveTable(con, TARGET_TABLE)
if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- paste0(
    "CREATE TABLE ",
    SCHEMA_NAME, ".", TABLE_NAME, "
    (
      RefreshDate                    DATETIME2(3)    NOT NULL,
      reporting_group_skey           NVARCHAR(10)    NOT NULL,
      property_skey                  NVARCHAR(10)    NOT NULL,
      vendor_skey                    NVARCHAR(10)    NULL,
      invoice_skey                   NVARCHAR(15)    NOT NULL,
      service_account_skey           NVARCHAR(10)    NULL,
      period_start_date              DATETIME2(3)    NULL,
      period_end_date                DATETIME2(3)    NULL,
      service_type                   NVARCHAR(30)    NULL,
      utility_type_group             NVARCHAR(25)    NULL,
      service_type_secondary         NVARCHAR(50)    NULL,
      estimated_usage                FLOAT           NULL,
      actual_usage                   FLOAT           NULL,
      actual_usage_uom               NVARCHAR(15)    NULL,
      estimated_cost                 FLOAT           NULL,
      service_cost_optimized_amount  FLOAT           NULL,
      factor_ch4                     FLOAT           NULL,
      factor_co2                     FLOAT           NULL,
      factor_n2o                     FLOAT           NULL,
      co2e_rate                      FLOAT           NULL,
      previous_year_co2e_rate        FLOAT           NULL,
      gj_factor                      FLOAT           NULL,
      m_conversion                   FLOAT           NULL,
      l_conversion                   FLOAT           NULL,
      m2_conversion                  FLOAT           NULL,
      kg_conversion                  FLOAT           NULL,
      uncertainty_factor             FLOAT           NULL,
      scope_desc                     NVARCHAR(25)    NULL,
      edp_create_ts                  DATETIME2(3)    NULL,
      edp_update_ts                  DATETIME2(3)    NULL
    );"
  )

  dbExecute(con, sql)
}

# Database Transaction ####
etl_error <- NULL

dbBegin(con)

tryCatch(
  {
    if (dbExistsTable(con, TEMP_TABLE)) {
      dbRemoveTable(con, TEMP_TABLE)
    }

    dbExecute(
      con,
      paste0(
        "CREATE TABLE ", TEMP_TABLE, "
        (
          RefreshDate                    DATETIME2(3)    NOT NULL,
          reporting_group_skey           NVARCHAR(10)    NOT NULL,
          property_skey                  NVARCHAR(10)    NOT NULL,
          vendor_skey                    NVARCHAR(10)    NULL,
          invoice_skey                   NVARCHAR(15)    NOT NULL,
          service_account_skey           NVARCHAR(10)    NULL,
          period_start_date              DATETIME2(3)    NULL,
          period_end_date                DATETIME2(3)    NULL,
          service_type                   NVARCHAR(30)    NULL,
          utility_type_group             NVARCHAR(25)    NULL,
          service_type_secondary         NVARCHAR(50)    NULL,
          estimated_usage                FLOAT           NULL,
          actual_usage                   FLOAT           NULL,
          actual_usage_uom               NVARCHAR(15)    NULL,
          estimated_cost                 FLOAT           NULL,
          service_cost_optimized_amount  FLOAT           NULL,
          factor_ch4                     FLOAT           NULL,
          factor_co2                     FLOAT           NULL,
          factor_n2o                     FLOAT           NULL,
          co2e_rate                      FLOAT           NULL,
          previous_year_co2e_rate        FLOAT           NULL,
          gj_factor                      FLOAT           NULL,
          m_conversion                   FLOAT           NULL,
          l_conversion                   FLOAT           NULL,
          m2_conversion                  FLOAT           NULL,
          kg_conversion                  FLOAT           NULL,
          uncertainty_factor             FLOAT           NULL,
          scope_desc                     NVARCHAR(25)    NULL,
          edp_create_ts                  DATETIME2(3)    NULL,
          edp_update_ts                  DATETIME2(3)    NULL
        );"
      )
    )

    dbWriteTable(
      con,
      name = TEMP_TABLE,
      value = clean_data,
      append = TRUE,
      overwrite = FALSE
    )

    # -- Guard: catch duplicate composite keys in source data before touching target --
    dup_count <- dbGetQuery(
      con,
      paste0(
        "SELECT COUNT(*) AS n
         FROM (
           SELECT reporting_group_skey, property_skey, invoice_skey
           FROM ", TEMP_TABLE, "
           GROUP BY reporting_group_skey, property_skey, invoice_skey
           HAVING COUNT(*) > 1
         ) dupes;"
      )
    )$n

    if (dup_count > 0) {
      stop(paste0(
        "Duplicate composite key values detected in source data (",
        dup_count,
        " keys affected). Rolling back."
      ))
    }

    # -- Update matched rows --
    n_updated <- dbExecute(
      con,
      paste0(
        "UPDATE tgt
         SET
           tgt.RefreshDate                    = src.RefreshDate,
           tgt.vendor_skey                    = src.vendor_skey,
           tgt.service_account_skey           = src.service_account_skey,
           tgt.period_start_date              = src.period_start_date,
           tgt.period_end_date                = src.period_end_date,
           tgt.service_type                   = src.service_type,
           tgt.utility_type_group             = src.utility_type_group,
           tgt.service_type_secondary         = src.service_type_secondary,
           tgt.estimated_usage                = src.estimated_usage,
           tgt.actual_usage                   = src.actual_usage,
           tgt.actual_usage_uom               = src.actual_usage_uom,
           tgt.estimated_cost                 = src.estimated_cost,
           tgt.service_cost_optimized_amount  = src.service_cost_optimized_amount,
           tgt.factor_ch4                     = src.factor_ch4,
           tgt.factor_co2                     = src.factor_co2,
           tgt.factor_n2o                     = src.factor_n2o,
           tgt.co2e_rate                      = src.co2e_rate,
           tgt.previous_year_co2e_rate        = src.previous_year_co2e_rate,
           tgt.gj_factor                      = src.gj_factor,
           tgt.m_conversion                   = src.m_conversion,
           tgt.l_conversion                   = src.l_conversion,
           tgt.m2_conversion                  = src.m2_conversion,
           tgt.kg_conversion                  = src.kg_conversion,
           tgt.uncertainty_factor             = src.uncertainty_factor,
           tgt.scope_desc                     = src.scope_desc,
           tgt.edp_create_ts                  = src.edp_create_ts,
           tgt.edp_update_ts                  = src.edp_update_ts
         FROM ", SCHEMA_NAME, ".", TABLE_NAME, " tgt
         INNER JOIN ", TEMP_TABLE, " src
           ON  tgt.reporting_group_skey = src.reporting_group_skey
           AND tgt.property_skey        = src.property_skey
           AND tgt.invoice_skey         = src.invoice_skey;"
      )
    )

    # -- Insert new rows --
    n_inserted <- dbExecute(
      con,
      paste0(
        "INSERT INTO ", SCHEMA_NAME, ".", TABLE_NAME, "
        (
          RefreshDate,
          reporting_group_skey,
          property_skey,
          vendor_skey,
          invoice_skey,
          service_account_skey,
          period_start_date,
          period_end_date,
          service_type,
          utility_type_group,
          service_type_secondary,
          estimated_usage,
          actual_usage,
          actual_usage_uom,
          estimated_cost,
          service_cost_optimized_amount,
          factor_ch4,
          factor_co2,
          factor_n2o,
          co2e_rate,
          previous_year_co2e_rate,
          gj_factor,
          m_conversion,
          l_conversion,
          m2_conversion,
          kg_conversion,
          uncertainty_factor,
          scope_desc,
          edp_create_ts,
          edp_update_ts
        )
        SELECT
          src.RefreshDate,
          src.reporting_group_skey,
          src.property_skey,
          src.vendor_skey,
          src.invoice_skey,
          src.service_account_skey,
          src.period_start_date,
          src.period_end_date,
          src.service_type,
          src.utility_type_group,
          src.service_type_secondary,
          src.estimated_usage,
          src.actual_usage,
          src.actual_usage_uom,
          src.estimated_cost,
          src.service_cost_optimized_amount,
          src.factor_ch4,
          src.factor_co2,
          src.factor_n2o,
          src.co2e_rate,
          src.previous_year_co2e_rate,
          src.gj_factor,
          src.m_conversion,
          src.l_conversion,
          src.m2_conversion,
          src.kg_conversion,
          src.uncertainty_factor,
          src.scope_desc,
          src.edp_create_ts,
          src.edp_update_ts
        FROM ", TEMP_TABLE, " src
        LEFT JOIN ", SCHEMA_NAME, ".", TABLE_NAME, " tgt
          ON  tgt.reporting_group_skey = src.reporting_group_skey
          AND tgt.property_skey        = src.property_skey
          AND tgt.invoice_skey         = src.invoice_skey
        WHERE tgt.reporting_group_skey IS NULL;"
      )
    )

    dbCommit(con)

    n_updated <<- n_updated
    n_inserted <<- n_inserted

    cat("ETL complete — updated:", n_updated, "| inserted:", n_inserted, "\n")
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
    n_updated = n_updated,
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
