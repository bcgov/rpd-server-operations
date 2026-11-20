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
TABLE_NAME <- "ArchibusDepartment"
SOURCE_SCHEMA <- "InfBronze"
SOURCE_TABLE <- "archibus_dp"
SOURCE_PRIMARY_KEY <- c("dp_dv_id", "dp_dp_id")
TARGET_PRIMARY_KEY <- c("DivisionId", "DepartmentId")
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
# query <- dbSendQuery(
#   con,
#   glue::glue("SELECT * FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}")
# )
# data <- dbFetch(query, n = -1)
# dbClearResult(query)

pk_cols <- paste(SOURCE_PRIMARY_KEY, collapse = ", ")
query <- dbSendQuery(
  con,
  glue::glue(
    "
  SELECT *
    FROM (
      SELECT
      {pk_cols},
      dp_address_ref,
      dp_area_chargable,
      dp_area_comn,
      dp_area_comn_nocup,
      dp_area_comn_ocup,
      dp_area_nocup,
      dp_area_ocup,
      dp_area_rm,
      dp_collector,
      dp_contact_id,
      dp_customer_category,
      dp_customer_class,
      dp_customer_ref,
      dp_customer_segment,
      dp_dv_id,
      dp_gl_code,
      dp_hpattern_acad,
      dp_mvpt_category,
      dp_name,
      dp_option1,
      dp_option2,
      dp_pam,
      dp_reconciled,
      dp_recovery_fee,
      dp_sales_rep,
      dp_status,
      dp_tax_code,
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
    DepartmentId = dp_dp_id,
    DepartmentName = dp_name,
    DivisionId = dp_dv_id,
    CustomerCategory = dp_customer_category,
    CustomerSegment = dp_customer_segment,
    Status = dp_status,
    dp_address_ref = dp_address_ref,
    dp_area_chargable = as.double(dp_area_chargable),
    dp_area_comn = as.double(dp_area_comn),
    dp_area_comn_nocup = as.double(dp_area_comn_nocup),
    dp_area_comn_ocup = as.double(dp_area_comn_ocup),
    dp_area_nocup = as.double(dp_area_nocup),
    dp_area_ocup = as.double(dp_area_ocup),
    dp_area_rm = as.double(dp_area_rm),
    dp_collector = dp_collector,
    dp_contact_id = dp_contact_id,
    dp_customer_ref = dp_customer_ref,
    dp_customer_class = dp_customer_class,
    dp_gl_code = dp_gl_code,
    dp_hpattern_acad = dp_hpattern_acad,
    dp_mvpt_category = dp_mvpt_category,
    dp_option1 = dp_option1,
    dp_option2 = dp_option2,
    dp_pam = dp_pam,
    dp_reconciled = dp_reconciled,
    dp_recovery_fee = dp_recovery_fee,
    dp_sales_rep = dp_sales_rep,
    dp_tax_code = dp_tax_code,
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
      DepartmentId             VARCHAR(100)  NOT NULL,
      DepartmentName           VARCHAR(250)  NULL,
      DivisionId               VARCHAR(20)   NOT NULL,
      CustomerCategory         VARCHAR(100)  NULL,
      CustomerSegment          VARCHAR(100)  NULL,
      Status                   VARCHAR(20)   NULL,
      dp_address_ref           VARCHAR(20)   NULL,
      dp_area_chargable        DECIMAL(18,2) NULL,
      dp_area_comn             DECIMAL(18,2) NULL,
      dp_area_comn_nocup       DECIMAL(18,2) NULL,
      dp_area_comn_ocup        DECIMAL(18,2) NULL,
      dp_area_nocup            DECIMAL(18,2) NULL,
      dp_area_ocup             DECIMAL(18,2) NULL,
      dp_area_rm               DECIMAL(18,2) NULL,
      dp_collector             VARCHAR(50)   NULL,
      dp_contact_id            VARCHAR(90)   NULL,
      dp_customer_class        VARCHAR(90)   NULL,
      dp_customer_ref          VARCHAR(20)   NULL,
      dp_gl_code               VARCHAR(90)   NULL,
      dp_hpattern_acad         VARCHAR(90)   NULL,
      dp_mvpt_category         VARCHAR(90)   NULL,
      dp_option1               VARCHAR(50)   NULL,
      dp_option2               VARCHAR(50)   NULL,
      dp_pam                   VARCHAR(10)   NULL,
      dp_reconciled            VARCHAR(10)   NULL,
      dp_recovery_fee          VARCHAR(10)   NULL,
      dp_sales_rep             VARCHAR(90)   NULL,
      dp_tax_code              VARCHAR(50)   NULL,
      BronzeBatchId            BIGINT        NOT NULL,
      BronzeLoadTs             DATETIME2(0)  NOT NULL,
      CONSTRAINT PK_silver_department PRIMARY KEY CLUSTERED ({paste(TARGET_PRIMARY_KEY, collapse = ', ')})
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
