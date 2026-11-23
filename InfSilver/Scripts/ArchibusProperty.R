#' @title Archibus Property
#'
#' @description
#' Loads the current property inventory from InfBronze.archibus_property into
#' the Silver layer. The script selects the latest version of each property
#' record, applies business-friendly naming and data-type conversions, and
#' publishes a current-state property dataset for reporting and analytical
#' consumption.
#'
#' @database BuildingIntelligence
#' @schema InfSilver
#' @table ArchibusProperty
#'
#' @source InfBronze.archibus_property
#'
#' @load_type Full refresh from latest Bronze records
#' @grain One row per property
#'
#' @filters Only the most recent Bronze record version is retained for each
#'   property identifier
#' @filters Records are excluded if no current version exists in Bronze
#'
#' @consumers TODO
#'
#' @column PropertyId Property identifier
#' @column PropertyName Property name
#' @column SiteId Associated site identifier
#' @column Tenure Property ownership status
#' @column Status Property lifecycle status
#' @column Occupancy Current property occupancy status
#' @column PrimaryUse Primary property use classification
#' @column StrategicClass Strategic portfolio classification
#' @column BuildingAreaRentable Rentable building area associated with the property
#' @column LandArea Land area associated with the property
#' @column MarketValue Current market value of the property
#' @column BronzeBatchId Source Bronze batch identifier
#' @column BronzeLoadTs Timestamp of the Bronze record version selected
#'
#' @notes
#' Data is sourced from InfBronze.archibus_property.
#'
#' This table provides the current-state property inventory used for portfolio
#' management, real-property reporting, valuation analysis, occupancy
#' reporting, and strategic planning activities.
#'
#' Property ownership status and property lifecycle status are represented
#' separately to distinguish portfolio tenure (for example Owned, Leased, or
#' Managed) from operational lifecycle state (for example Active or Retired).
#'
#' Historical property versions are retained in the Bronze layer. Only the
#' most recent version of each property record is published to Silver.
#'
#' Technical source-system attribute names are transformed into
#' business-oriented field names and analytical data types where appropriate.
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
TABLE_NAME <- "ArchibusProperty"
SOURCE_SCHEMA <- "InfBronze"
SOURCE_TABLE <- "archibus_property"
SOURCE_PRIMARY_KEY <- c("property_pr_id")
TARGET_PRIMARY_KEY <- c("PropertyId")
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
query <- dbSendQuery(
  con,
  glue::glue("SELECT * FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}")
)
data <- dbFetch(query, n = -1)
dbClearResult(query)

pk_cols <- paste(SOURCE_PRIMARY_KEY, collapse = ", ")
query <- dbSendQuery(
  con,
  glue::glue(
    "
  SELECT *
    FROM (
      SELECT
      {pk_cols},
      property_address1,
      property_area_bl_gross_int,
      property_area_bl_rentable,
      property_area_bl_usable,
      property_area_land_acres,
      property_area_lease_meas,
      property_area_lease_neg,
      property_city_id,
      property_comments,
      property_condition,
      property_ctry_id,
      property_date_book_val,
      property_date_costs_end,
      property_date_costs_start,
      property_date_end_pobc,
      property_date_market_val,
      property_date_purchase,
      property_date_sold,
      property_date_start_pobc,
      property_lat,
      property_lon,
      property_name,
      property_occ_status,
      property_pricing_method,
      property_primary_use,
      property_regn_id,
      property_serv_provider,
      property_site_id,
      property_state_id,
      property_status,
      property_status_pobc,
      property_strategic_class,
      property_use1,
      property_value_book,
      property_value_extras,
      property_value_land,
      property_value_market,
      property_zip,
      property_zoning,
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
    PropertyId = property_pr_id,
    PropertyName = property_name,
    SiteId = property_site_id,
    Address = property_address1,
    City = property_city_id,
    Province = property_state_id,
    Country = property_ctry_id,
    PostalCode = property_zip,
    Latitude = as.double(property_lat),
    Longitude = as.double(property_lon),
    Tenure = property_status,
    Status = property_status_pobc,
    Occupancy = property_occ_status,
    StrategicClass = property_strategic_class,
    PropertyUse = property_use1,
    PrimaryUse = property_primary_use,
    PricingMethod = property_pricing_method,
    RegionId = property_regn_id,
    Comments = property_comments,
    BuildingAreaGrossInternal = as.double(property_area_bl_gross_int),
    BuildingAreaRentable = as.double(property_area_bl_rentable),
    BuildingAreaUsable = as.double(property_area_bl_usable),
    LandArea = as.double(property_area_land_acres),
    LeaseAreaMeas = as.double(property_area_lease_meas),
    LeaseAreaNeg = as.double(property_area_lease_neg),
    Condition = as.character(property_condition),
    DateCostsStart = as.POSIXct(property_date_costs_start, tz = "UTC"),
    DateCostsEnd = as.POSIXct(property_date_costs_end, tz = "UTC"),
    DatePoBCStart = as.POSIXct(property_date_start_pobc, tz = "UTC"),
    DatePoBCEnd = as.POSIXct(property_date_end_pobc, tz = "UTC"),
    DatePurchase = as.POSIXct(property_date_purchase, tz = "UTC"),
    BookValueDate = as.POSIXct(property_date_book_val, tz = "UTC"),
    BookValue = as.double(property_value_book),
    DateMarketValue = as.POSIXct(property_date_market_val, tz = "UTC"),
    MarketValue = as.double(property_value_market),
    PropertyValueExtras = as.double(property_value_extras),
    PropertyValueLand = as.double(property_value_land),
    ServiceProvider = property_serv_provider,
    PropertyZoning = property_zoning,
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
      PropertyId                 VARCHAR(20)   NOT NULL,
      PropertyName               VARCHAR(100)  NULL,
      SiteId                     VARCHAR(20)   NULL,
      Address                    VARCHAR(100)  NULL,
      City                       VARCHAR(100)  NULL,
      Province                   VARCHAR(10)   NULL,
      Country                    VARCHAR(10)   NULL,
      PostalCode                 VARCHAR(20)   NULL,
      Latitude                   DECIMAL(12,8) NULL,
      Longitude                  DECIMAL(12,8) NULL,
      Tenure                     VARCHAR(20)   NULL,
      Status                     VARCHAR(20)   NULL,
      Occupancy                  VARCHAR(20)   NULL,
      StrategicClass             VARCHAR(50)   NULL,
      PropertyUse                VARCHAR(50)   NULL,
      PrimaryUse                 VARCHAR(50)   NULL,
      PricingMethod              VARCHAR(50)   NULL,
      RegionId                   VARCHAR(50)   NULL,
      Comments                   VARCHAR(500)  NULL,
      BuildingAreaGrossInternal  DECIMAL(18,2) NULL,
      BuildingAreaRentable       DECIMAL(18,2) NULL,
      BuildingAreaUsable         DECIMAL(18,2) NULL,
      LandArea                   DECIMAL(18,2) NULL,
      LeaseAreaMeas              DECIMAL(18,2) NULL,
      LeaseAreaNeg               DECIMAL(18,2) NULL,
      Condition                  VARCHAR(20)   NULL,
      DateCostsStart             DATETIME2     NULL,
      DateCostsEnd               DATETIME2     NULL,
      DatePoBCStart              DATETIME2     NULL,
      DatePoBCEnd                DATETIME2     NULL,
      DatePurchase               DATETIME2     NULL,
      BookValueDate              DATETIME2     NULL,
      DateMarketValue            DATETIME2     NULL,
      BookValue                  DECIMAL(18,2) NULL,
      MarketValue                DECIMAL(18,2) NULL,
      PropertyValueExtras        DECIMAL(18,2) NULL,
      PropertyValueLand          DECIMAL(18,2) NULL,
      ServiceProvider            VARCHAR(50)   NULL,
      PropertyZoning             VARCHAR(50)   NULL,
      BronzeBatchId              BIGINT        NOT NULL,
      BronzeLoadTs               DATETIME2(0)  NOT NULL,
      CONSTRAINT PK_silver_property PRIMARY KEY CLUSTERED ({paste(TARGET_PRIMARY_KEY, collapse = ', ')})
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
