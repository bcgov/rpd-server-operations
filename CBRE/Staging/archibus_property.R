# Load libraries
library(base64enc, quietly = TRUE, warn.conflicts = FALSE)
library(dplyr, quietly = TRUE, warn.conflicts = FALSE)
library(here, quietly = TRUE, warn.conflicts = FALSE)
library(httr2, quietly = TRUE, warn.conflicts = FALSE)
library(jsonlite, quietly = TRUE, warn.conflicts = FALSE)
library(lubridate, quietly = TRUE, warn.conflicts = FALSE)
library(purrr, quietly = TRUE, warn.conflicts = FALSE)
library(tibble, quietly = TRUE, warn.conflicts = FALSE)
library(tidyr, quietly = TRUE, warn.conflicts = FALSE)

library(odbc, quietly = TRUE, warn.conflicts = FALSE)
library(DBI, quietly = TRUE, warn.conflicts = FALSE)

# Load helper functions
source(here::here("./utilities/R/cbre_api_function.R"))
source(here::here("./utilities/R/event_logger.R"))
ETL_STATUS <- "DEV"

SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "CbreStaging"
TABLE_NAME <- "Property"
CBRE_TABLE_NAME <- "archibus_property"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
API_NAME <- "CBRE"
SCRIPT_NAME <- "Property"


# Connect to SQL database
con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = SQL_SERVER,
  database = DB_NAME,
  Trusted_Connection = "Yes"
)

raw_data <- extract_cbre_data(CBRE_TABLE_NAME)

clean_data <- raw_data |>
  select_if(~ !all(is.na(.))) |>
  select_if(~ !all(. == 0)) |>
  select_if(~ !all(. == '-1')) |>
  select_if(~ !all(. == "N/A")) |>
  select_if(~ !all(. == "-")) |>
  mutate(
    across(
      c(
        property_strategic_class,
        property_primary_use,
        property_pricing_method
      ),
      stringr::str_to_title
    )
  ) |>
  mutate(
    across(
      c(
        property_area_land_acres,
        property_value_book,
        property_value_market,
        property_area_bl_gross_int,
        property_area_bl_rentable,
        property_area_bl_usable,
        property_area_land_acres,
        property_area_lease_meas,
        property_area_lease_neg,
        property_lat,
        property_lon,
      ),
      as.double
    )
  ) |>
  mutate(
    across(
      c(
        edp_update_ts,
        property_date_book_val,
        property_date_market_val,
        property_date_start_pobc,
        property_date_end_pobc
      ),
      as.POSIXct
    )
  ) |>
  mutate(edp_update_ts = as.POSIXct(edp_update_ts)) |>
  mutate(RefreshDate = as.POSIXct(Sys.time())) |>
  select(
    RefreshDate,
    edp_update_ts,
    PobcStatus = property_status_pobc,
    PropertyId = property_pr_id,
    SiteId = property_site_id,
    linkAddress = property_address1,
    linkCity = property_city_id,
    Name = property_name,
    Tenure = property_status,
    PrimaryUse = property_primary_use,
    StrategicClassification = property_strategic_class,
    PropertyType = property_property_type,
    PobcStartDate = property_date_start_pobc,
    PobcEndDate = property_date_end_pobc,
    PricingMethod = property_pricing_method,
    BookValueDate = property_date_book_val,
    BookValue = property_value_book,
    MarketValueDate = property_date_market_val,
    MarketValue = property_value_market,
    TotalRentableLand = property_area_land_acres,
    OccupancyStatus = property_occ_status,
    lat = property_lat,
    lon = property_lon,
    property_area_bl_gross_int,
    property_area_bl_rentable,
    property_area_bl_usable,
    property_area_land_acres,
    property_area_lease_meas,
    property_area_lease_neg
  )

# Database Transaction ####
# dbRemoveTable(con, Id(schema = SCHEMA_NAME, table = TABLE_NAME))
if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- paste0(
    "CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    TABLE_NAME,
    " (
        RefreshDate                  DATETIME2(3)   NOT NULL,
        edp_update_ts                DATETIME2(3)   NOT NULL,
        PobcStatus                   NVARCHAR(30)   NULL,
        PropertyId                   NVARCHAR(20)   NOT NULL,
        SiteId                       NVARCHAR(20)   NULL,
        linkAddress                  NVARCHAR(150)  NULL,
        linkCity                     NVARCHAR(50)   NULL,
        Name                         NVARCHAR(150)  NULL,
        Tenure                       NVARCHAR(30)   NULL,
        PrimaryUse                   NVARCHAR(50)   NULL,
        StrategicClassification      NVARCHAR(50)   NULL,
        PropertyType                 NVARCHAR(50)   NULL,
        PobcStartDate                DATETIME2(3)   NULL,
        PobcEndDate                  DATETIME2(3)   NULL,
        PricingMethod                NVARCHAR(50)   NULL,
        BookValueDate                DATETIME2(3)   NULL,
        BookValue                    DECIMAL(18,2)  NULL,
        MarketValueDate              DATETIME2(3)   NULL,
        MarketValue                  DECIMAL(18,2)  NULL,
        TotalRentableLand            DECIMAL(18,2)  NULL,
        OccupancyStatus              NVARCHAR(30)   NULL,
        lat                          DECIMAL(9,6)   NULL,
        lon                          DECIMAL(9,6)   NULL,
        property_area_bl_gross_int   DECIMAL(18,2)  NULL,
        property_area_bl_rentable    DECIMAL(18,2)  NULL,
        property_area_bl_usable      DECIMAL(18,2)  NULL,
        property_area_lease_meas     DECIMAL(18,2)  NULL,
        property_area_lease_neg      DECIMAL(18,2)  NULL
      );"
  )
  dbExecute(con, sql)
}

etl_start_time <- Sys.time()

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
    CREATE TABLE  ",
        SCHEMA_NAME,
        ".",
        TEMP_TABLE,
        " (
        RefreshDate                  DATETIME2(3)   NOT NULL,
        edp_update_ts                DATETIME2(3)   NOT NULL,
        PobcStatus                   NVARCHAR(30)   NULL,
        PropertyId                   NVARCHAR(20)   NOT NULL,
        SiteId                       NVARCHAR(20)   NULL,
        linkAddress                  NVARCHAR(150)  NULL,
        linkCity                     NVARCHAR(50)   NULL,
        Name                         NVARCHAR(150)  NULL,
        Tenure                       NVARCHAR(30)   NULL,
        PrimaryUse                   NVARCHAR(50)   NULL,
        StrategicClassification      NVARCHAR(50)   NULL,
        PropertyType                 NVARCHAR(50)   NULL,
        PobcStartDate                DATETIME2(3)   NULL,
        PobcEndDate                  DATETIME2(3)   NULL,
        PricingMethod                NVARCHAR(50)   NULL,
        BookValueDate                DATETIME2(3)   NULL,
        BookValue                    DECIMAL(18,2)  NULL,
        MarketValueDate              DATETIME2(3)   NULL,
        MarketValue                  DECIMAL(18,2)  NULL,
        TotalRentableLand            DECIMAL(18,2)  NULL,
        OccupancyStatus              NVARCHAR(30)   NULL,
        lat                          DECIMAL(9,6)   NULL,
        lon                          DECIMAL(9,6)   NULL,
        property_area_bl_gross_int   DECIMAL(18,2)  NULL,
        property_area_bl_rentable    DECIMAL(18,2)  NULL,
        property_area_bl_usable      DECIMAL(18,2)  NULL,
        property_area_lease_meas     DECIMAL(18,2)  NULL,
        property_area_lease_neg      DECIMAL(18,2)  NULL
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
        PropertyId,
        SiteId,
        linkAddress,
        linkCity,
        Name,
        Tenure,
        PrimaryUse,
        StrategicClassification,
        PropertyType,
        PobcStartDate,
        PobcEndDate,
        PricingMethod,
        BookValueDate,
        BookValue,
        MarketValueDate,
        MarketValue,
        TotalRentableLand,
        OccupancyStatus,
        lat,
        lon,
        property_area_bl_gross_int,
        property_area_bl_rentable,
        property_area_bl_usable,
        property_area_lease_meas,
        property_area_lease_neg
      )
       SELECT * FROM ",
        TEMP_TABLE,
        ";"
      )
    )

    # Complete the transaction
    dbCommit(con)
    #     n_deleted <<- n_deleted
    n_inserted <<- n_inserted
    #     n_updated <<- n_updated
    #     # Rollback transaction on failure
  },
  error = function(e) {
    dbRollback(con)
    etl_error <<- e
    # stop(e)
  }
)

if (is.null(etl_error)) {
  log_daily_etl_run(
    api_name = API_NAME,
    script_name = SCRIPT_NAME,
    table_name = TABLE_NAME,
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
