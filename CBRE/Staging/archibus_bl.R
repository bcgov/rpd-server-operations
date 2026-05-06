ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "CbreStaging"
TABLE_NAME <- "Building"
CBRE_TABLE_NAME <- "archibus_bl"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
API_NAME <- "CBRE"
SCRIPT_NAME <- "Building"

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
    bl_strategic_class = stringr::str_to_title(bl_strategic_class),
    bl_primary_use = stringr::str_to_title(bl_primary_use)
  ) |>
  mutate(
    across(
      c(
        bl_value_book,
        bl_value_market,
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
        bl_area_vert_pen
      ),
      as.double
    )
  ) |>
  mutate(bl_option1 = as.logical(bl_option1)) |>
  mutate(
    across(
      c(
        edp_update_ts,
        bl_date_book_val,
        bl_date_costs_start,
        bl_date_start_pobc,
        bl_date_end_pobc,
        bl_date_market_val
      ),
      as.POSIXct
    )
  ) |>
  mutate(RefreshDate = as.POSIXct(Sys.time())) |>
  select(
    RefreshDate,
    edp_update_ts,
    PobcStatus = bl_status_pobc,
    BuildingId = bl_bl_id_key,
    PropertyId = bl_pr_id,
    SiteId = bl_site_id,
    linkAddress = bl_address1,
    linkCity = bl_city_id,
    Name = bl_name,
    Tenure = bl_status,
    PrimaryUse = bl_primary_use,
    StrategicClassification = bl_strategic_class,
    FacilityType = bl_facility_type,
    BuildingDate = bl_date_bl,
    PricingMethod = bl_pricing_method,
    BookValueDate = bl_date_book_val,
    BookValue = bl_value_book,
    CostsStartDate = bl_date_costs_start,
    PobcStartDate = bl_date_start_pobc,
    PobcEndDate = bl_date_end_pobc,
    MarketValueDate = bl_date_market_val,
    MarketValue = bl_value_market,
    OccupancyStatus = bl_occ_status,
    Option1 = bl_option1,
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


# Database Transaction ####
# dbRemoveTable(con, Id(schema = "CbreStaging", table = TABLE_NAME))
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
        bl_lat                  NVARCHAR(30)  NULL,
        bl_lon                  NVARCHAR(30)  NULL
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
      bl_lat                  NVARCHAR(30)  NULL,
      bl_lon                  NVARCHAR(30)  NULL
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
