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
TABLE_NAME <- "ArchibusBuilding"
SOURCE_SCHEMA <- "InfBronze"
SOURCE_TABLE <- "archibus_bl"
SOURCE_PRIMARY_KEY <- c("bl_bl_id_key")
TARGET_PRIMARY_KEY <- c("BuildingId")
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
# query <- dbSendQuery(con, "SELECT * FROM InfBronze.archibus_bl")
# BuildingData <- dbFetch(query, n = -1)
# dbClearResult(query)
# archibus_bl
pk_cols <- paste(SOURCE_PRIMARY_KEY, collapse = ", ")
query <- dbSendQuery(
  con,
  glue::glue(
    "
  SELECT *
    FROM (
      SELECT
      {pk_cols},
      bl_address1,
      bl_area_bl_comn_nocup,
      bl_area_bl_comn_serv,
      bl_area_ext_wall,
      bl_area_gross_int,
      bl_area_ls_negotiated,
      bl_area_nocup,
      bl_area_nocup_comn,
      bl_area_nocup_dp,
      bl_area_ocup,
      bl_area_ocup_comn,
      bl_area_ocup_dp,
      bl_area_remain,
      bl_area_rentable,
      bl_area_rm,
      bl_area_rm_comn,
      bl_area_rm_dp,
      bl_area_serv,
      bl_area_usable,
      bl_area_vert_pen,
      bl_city_id,
      bl_comments,
      bl_condition,
      bl_construction_type,
      bl_ctry_id,
      bl_date_bl,
      bl_date_book_val,
      bl_date_costs_start,
      bl_date_costs_end,
      bl_date_end_pobc,
      bl_date_start_pobc,
      bl_date_market_val,
      bl_facility_type,
      bl_lat,
      bl_lon,
      bl_mam_predom_use,
      bl_name,
      bl_occ_status,
      bl_option1,
      bl_option2,
      bl_pr_id,
      bl_pricing_method,
      bl_primary_use,
      bl_qty_life_expect,
      bl_serv_provider,
      bl_site_id,
      bl_state_id,
      bl_status,
      bl_status_pobc,
      bl_strategic_class,
      bl_url,
      bl_value_book,
      bl_value_market,
      bl_zip,
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
    BuildingId = bl_bl_id_key,
    PropertyId = bl_pr_id,
    SiteId = bl_site_id,
    Address = bl_address1,
    City = bl_city_id,
    Province = bl_state_id,
    PostalCode = bl_zip,
    Name = bl_name,
    Tenure = bl_status,
    PoBCStatus = bl_status_pobc,
    FacilityType = bl_facility_type,
    PrimaryUse = bl_primary_use,
    StrategicClass = bl_strategic_class,
    OccupancyStatus = bl_occ_status,
    PricingMethod = bl_pricing_method,
    AreaCommonUnoccupied = as.double(bl_area_bl_comn_nocup),
    AreaCommonService = as.double(bl_area_bl_comn_serv),
    AreaExteriorWall = as.double(bl_area_ext_wall),
    AreaGrossInternal = as.double(bl_area_gross_int),
    AreaLeaseNegotiated = as.double(bl_area_ls_negotiated),
    AreaUnoccupied = as.double(bl_area_nocup),
    AreaUnoccupiedCommon = as.double(bl_area_nocup_comn),
    AreaUnoccupied_dp = as.double(bl_area_nocup_dp),
    AreaOccupied = as.double(bl_area_ocup),
    AreaOccupiedCommon = as.double(bl_area_ocup_comn),
    AreaRemain = as.double(bl_area_remain),
    AreaRentable = as.double(bl_area_rentable),
    AreaRoom = as.double(bl_area_rm),
    AreaRoomCommon = as.double(bl_area_rm_comn),
    AreaRoom_dp = as.double(bl_area_rm_dp),
    AreaService = as.double(bl_area_serv),
    AreaUsable = as.double(bl_area_usable),
    AreaVerticalPen = as.double(bl_area_vert_pen),
    Comments = bl_comments,
    Condition = bl_condition,
    ConstructionType = bl_construction_type,
    DateBuilding = as.POSIXct(bl_date_bl, tz = "UTC"),
    DateBookValue = as.POSIXct(bl_date_book_val, tz = "UTC"),
    BookValue = as.double(bl_value_book),
    DateCostsStart = as.POSIXct(bl_date_costs_start, tz = "UTC"),
    DateCostsEnd = as.POSIXct(bl_date_costs_end, tz = "UTC"),
    DateEndPoBC = as.POSIXct(bl_date_end_pobc, tz = "UTC"),
    DateStartPoBC = as.POSIXct(bl_date_start_pobc, tz = "UTC"),
    DateMarketValue = as.POSIXct(bl_date_market_val, tz = "UTC"),
    MarketValue = as.double(bl_value_market),
    lat = as.double(bl_lat),
    lon = as.double(bl_lon),
    Option1 = bl_option1,
    ExpectedLife = bl_qty_life_expect,
    MamPredomUse = bl_mam_predom_use,
    ServiceProvider = bl_serv_provider,
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
      BuildingId               VARCHAR(20)   NOT NULL,
      PropertyId               VARCHAR(20)   NULL,
      SiteId                   VARCHAR(20)   NULL,
      Address                  VARCHAR(100)  NULL,
      City                     VARCHAR(100)  NULL,
      Province                 VARCHAR(10)   NULL,
      PostalCode               VARCHAR(20)   NULL,
      Name                     VARCHAR(100)  NULL,
      Tenure                   VARCHAR(20)   NULL,
      PoBCStatus               VARCHAR(20)   NULL,
      FacilityType             VARCHAR(50)   NULL,
      PrimaryUse               VARCHAR(50)   NULL,
      StrategicClass           VARCHAR(50)   NULL,
      OccupancyStatus          VARCHAR(20)   NULL,
      PricingMethod            VARCHAR(50)   NULL,
      AreaCommonUnoccupied     DECIMAL(18,2) NULL,
      AreaCommonService        DECIMAL(18,2) NULL,
      AreaExteriorWall         DECIMAL(18,2) NULL,
      AreaGrossInternal        DECIMAL(18,2) NULL,
      AreaLeaseNegotiated      DECIMAL(18,2) NULL,
      AreaUnoccupied           DECIMAL(18,2) NULL,
      AreaUnoccupiedCommon     DECIMAL(18,2) NULL,
      AreaUnoccupied_dp        DECIMAL(18,2) NULL,
      AreaOccupied             DECIMAL(18,2) NULL,
      AreaOccupiedCommon       DECIMAL(18,2) NULL,
      AreaRemain               DECIMAL(18,2) NULL,
      AreaRentable             DECIMAL(18,2) NULL,
      AreaRoom                 DECIMAL(18,2) NULL,
      AreaRoomCommon           DECIMAL(18,2) NULL,
      AreaRoom_dp              DECIMAL(18,2) NULL,
      AreaService              DECIMAL(18,2) NULL,
      AreaUsable               DECIMAL(18,2) NULL,
      AreaVerticalPen          DECIMAL(18,2) NULL,
      Comments                 VARCHAR(100)  NULL,
      Condition                VARCHAR(20)   NULL,
      ConstructionType         VARCHAR(20)   NULL,
      DateBuilding             DATETIME2     NULL,
      DateBookValue            DATETIME2     NULL,
      BookValue                DECIMAL(18,2) NULL,
      DateCostsStart           DATETIME2     NULL,
      DateCostsEnd             DATETIME2     NULL,
      DateEndPoBC              DATETIME2     NULL,
      DateStartPoBC            DATETIME2     NULL,
      DateMarketValue          DATETIME2     NULL,
      MarketValue              DECIMAL(12,2) NULL,
      lat                      DECIMAL(12,8) NULL,
      lon                      DECIMAL(12,8) NULL,
      Option1                  VARCHAR(20)   NULL,
      ExpectedLife             VARCHAR(20)   NULL,
      MamPredomUse             VARCHAR(20)   NULL,
      ServiceProvider          VARCHAR(50)   NULL,
      BronzeBatchId            BIGINT        NOT NULL,
      BronzeLoadTs             DATETIME2(0)  NOT NULL,
      CONSTRAINT PK_silver_building PRIMARY KEY CLUSTERED ({paste(TARGET_PRIMARY_KEY, collapse = ', ')})
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
