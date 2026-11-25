#' @title Archibus Lease
#'
#' @description
#' Loads the current lease inventory from InfBronze.archibus_ls into the
#' Silver layer. The script selects the latest version of each lease record,
#' applies business-friendly naming and data-type conversions, and publishes a
#' current-state lease dataset for reporting and analytical consumption.
#'
#' @database BuildingIntelligence
#' @schema InfSilver
#' @table ArchibusLease
#'
#' @source InfBronze.archibus_ls
#'
#' @load_type Full refresh from latest Bronze records
#' @grain One row per lease
#'
#' @filters Only the most recent Bronze record version is retained for each
#'   lease identifier
#' @filters Records are excluded if no current version exists in Bronze
#'
#' @consumers TODO
#'
#' @column LeaseId Lease identifier
#' @column LeaseParentId Parent lease identifier
#' @column LeaseStatus Current lease status
#' @column LeaseBuildingId Associated building identifier
#' @column LeasePropertyId Associated property identifier
#' @column AmountBaseRent Base rent amount
#' @column AmountOperating Operating cost amount
#' @column AmountTotalRent Total rent amount including recoverable costs
#' @column AreaRentable Rentable lease area
#' @column AreaUsable Usable lease area
#' @column DateStart Lease start date
#' @column DateEnd Lease end date
#' @column LeaseType Lease classification
#' @column LeaseTenantName Tenant name
#' @column ProjectId Associated project identifier
#' @column BronzeBatchId Source Bronze batch identifier
#' @column BronzeLoadTs Timestamp of the Bronze record version selected
#'
#' @notes
#' Data is sourced from InfBronze.archibus_ls.
#'
#' This table provides the current-state lease inventory used for lease
#' administration, contract reporting, occupancy analysis, property
#' management, and financial reporting activities.
#'
#' Historical lease versions are retained in the Bronze layer. Only the most
#' recent version of each lease record is published to Silver.
#'
#' Technical source-system attribute names are transformed into
#' business-oriented field names and analytical data types where appropriate.
#'
#' Lease financial metrics, property relationships, and date attributes are
#' normalized for downstream reporting and analytical consumption.
#'
#' @owner David Rattray
#' @last_reviewed 2026-11-25

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
TABLE_NAME <- "ArchibusLease"
SOURCE_SCHEMA <- "InfBronze"
SOURCE_TABLE <- "archibus_ls"
SOURCE_PRIMARY_KEY <- c("ls_ls_id")
TARGET_PRIMARY_KEY <- c("LeaseId")
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
pk_cols <- paste(SOURCE_PRIMARY_KEY, collapse = ", ")
query <- dbSendQuery(
  con,
  glue::glue(
    "
  SELECT *
    FROM (
      SELECT
      {pk_cols},
      ls_ls_parent_id,
      ls_status,
      ls_bl_id,
      ls_pr_id,
      ls_amendment,
      ls_amount_base_rent,
      ls_amount_operating,
      ls_amount_other,
      ls_amount_tot_rent_inc,
      ls_appropriated_hectares,
      ls_appropriated_parking_stalls,
      ls_appropriated_sqm,
      ls_area_bl_total,
      ls_area_common,
      ls_area_negotiated,
      ls_area_pr_total,
      ls_area_rentable,
      ls_area_usable,
      ls_bl_tot_rentable_area,
      ls_blended_area,
      ls_commence_clause,
      ls_comments,
      ls_date_base_tax_end,
      ls_date_base_tax_start,
      ls_date_commencement,
      ls_date_cost_anal_end,
      ls_date_cost_anal_start,
      ls_date_costs_last_calcd,
      ls_date_end,
      ls_date_move,
      ls_date_oprt_cost_end,
      ls_date_oprt_cost_start,
      ls_date_signed,
      ls_date_start,
      ls_date_terminated,
      ls_description,
      ls_fasb_ls_type,
      ls_floors,
      ls_hvac_light_sys,
      ls_is_short_term_lease,
      ls_land_description,
      ls_ld_contact,
      ls_ld_name,
      ls_lease_sublease,
      ls_ll_corp_sign,
      ls_ll_indi_sign,
      ls_multi_tenant,
      ls_non_standard,
      ls_num_sched_add,
      ls_op_cost_type,
      ls_option1,
      ls_park_days_notice,
      ls_park_max_random,
      ls_project_id,
      ls_reconciliation,
      ls_reduction_space,
      ls_remit_to,
      ls_rentable_land_acres,
      ls_reorg_ls_id,
      ls_reorg_status,
      ls_reorg_type,
      ls_reports,
      ls_saving_loss,
      ls_share_prpnt,
      ls_standard_docs,
      ls_tax_bill_num,
      ls_tax_prop_share,
      ls_tax_rate_per_sf,
      ls_tax_type,
      ls_template_name,
      ls_tn_name,
      ls_translink,
      ls_unique_share,
      ls_version,
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
    LeaseId = ls_ls_id,
    LeaseParentId = ls_ls_parent_id,
    LeaseStatus = ls_status,
    LeaseBuildingId = ls_bl_id,
    LeasePropertyId = ls_pr_id,
    LeaseAmendment = ls_amendment,
    AmountBaseRent = as.double(ls_amount_base_rent),
    AmountOperating = as.double(ls_amount_operating),
    AmountOther = as.double(ls_amount_other),
    AmountTotalRent = as.double(ls_amount_tot_rent_inc),
    AppropriatedHectares = as.double(ls_appropriated_hectares),
    AppropriatedParkingStalls = as.double(ls_appropriated_parking_stalls),
    AppropriatedSqM = as.double(ls_appropriated_sqm),
    AreaBuildingTotal = as.double(ls_area_bl_total),
    AreaCommon = as.double(ls_area_common),
    AreaNegotiated = as.double(ls_area_negotiated),
    AreaPropertyTotal = as.double(ls_area_pr_total),
    AreaRentable = as.double(ls_area_rentable),
    AreaUsable = as.double(ls_area_usable),
    BuildingTotalRentableArea = as.double(ls_bl_tot_rentable_area),
    BlendedArea = ls_blended_area,
    CommenceClause = ls_commence_clause,
    Comments = ls_comments,
    DateBaseTaxEnd = as.POSIXct(ls_date_base_tax_end, tz = "UTC"),
    DateBaseTaxStart = as.POSIXct(ls_date_base_tax_start, tz = "UTC"),
    DateCommencement = as.POSIXct(ls_date_commencement, tz = "UTC"),
    DateCostAnalysisEnd = as.POSIXct(ls_date_cost_anal_end, tz = "UTC"),
    DateCostAnalysisStart = as.POSIXct(ls_date_cost_anal_start, tz = "UTC"),
    DateLastCostsCalculated = as.POSIXct(ls_date_costs_last_calcd, tz = "UTC"),
    DateEnd = as.POSIXct(ls_date_end, tz = "UTC"),
    DateMove = as.POSIXct(ls_date_move, tz = "UTC"),
    DateOperationCostEnd = as.POSIXct(ls_date_oprt_cost_end, tz = "UTC"),
    DateOperationCostStart = as.POSIXct(ls_date_oprt_cost_start, tz = "UTC"),
    DateSigned = as.POSIXct(ls_date_signed, tz = "UTC"),
    DateStart = as.POSIXct(ls_date_start, tz = "UTC"),
    DateTerminated = as.POSIXct(ls_date_terminated, tz = "UTC"),
    Description = ls_description,
    LeaseType = ls_fasb_ls_type,
    LeaseFloors = ls_floors,
    HvacLightSystem = ls_hvac_light_sys,
    ShortTermLease = as.logical(ls_is_short_term_lease),
    LandDescription = ls_land_description,
    LandlordContact = ls_ld_contact,
    LandlordName = ls_ld_name,
    Sublease = ls_lease_sublease,
    MultiTenant = as.logical(ls_multi_tenant),
    NonStandard = as.logical(ls_non_standard),
    OperationCostType = ls_op_cost_type,
    Option1 = ls_option1,
    ParkDaysNotice = ls_park_days_notice,
    ParkMaxRandom = ls_park_max_random,
    ProjectId = ls_project_id,
    Reconciliation = as.logical(ls_reconciliation),
    ReductionSpace = as.logical(ls_reduction_space),
    RemitTo = ls_remit_to,
    RentableLandHectares = as.double(ls_rentable_land_acres),
    SavingLoss = ls_saving_loss,
    SharePrpnt = ls_share_prpnt,
    StandardDocs = ls_standard_docs,
    TaxBillNumber = ls_tax_bill_num,
    TaxProportionShare = ls_tax_prop_share,
    TaxRatePer = ls_tax_rate_per_sf,
    TaxType = ls_tax_type,
    LeaseTenantName = ls_tn_name,
    Translink = ls_translink,
    UniqueShare = ls_unique_share,
    LeaseVersion = ls_version,
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
      LeaseId                    VARCHAR(20)   NOT NULL,
      LeaseParentId              VARCHAR(10)   NULL,
      LeaseStatus                VARCHAR(20)   NULL,
      LeaseBuildingId            VARCHAR(10)   NULL,
      LeasePropertyId            VARCHAR(10)   NULL,
      LeaseAmendment             VARCHAR(10)   NULL,
      AmountBaseRent             DECIMAL(18,2) NULL,
      AmountOperating            DECIMAL(18,2) NULL,
      AmountOther                DECIMAL(18,2) NULL,
      AmountTotalRent            DECIMAL(18,2) NULL,
      AppropriatedHectares       DECIMAL(18,2) NULL,
      AppropriatedParkingStalls  DECIMAL(18,2) NULL,
      AppropriatedSqM            DECIMAL(18,2) NULL,
      AreaBuildingTotal          DECIMAL(18,2) NULL,
      AreaCommon                 DECIMAL(18,2) NULL,
      AreaNegotiated             DECIMAL(18,2) NULL,
      AreaPropertyTotal          DECIMAL(18,2) NULL,
      AreaRentable               DECIMAL(18,2) NULL,
      AreaUsable                 DECIMAL(18,2) NULL,
      BuildingTotalRentableArea  DECIMAL(18,2) NULL,
      BlendedArea                VARCHAR(10)   NULL,
      CommenceClause             VARCHAR(10)   NULL,
      Comments                   VARCHAR(1000) NULL,
      DateBaseTaxEnd             DATETIME2     NULL,
      DateBaseTaxStart           DATETIME2     NULL,
      DateCommencement           DATETIME2     NULL,
      DateCostAnalysisEnd        DATETIME2     NULL,
      DateCostAnalysisStart      DATETIME2     NULL,
      DateLastCostsCalculated    DATETIME2     NULL,
      DateEnd                    DATETIME2     NULL,
      DateMove                   DATETIME2     NULL,
      DateOperationCostEnd       DATETIME2     NULL,
      DateOperationCostStart     DATETIME2     NULL,
      DateSigned                 DATETIME2     NULL,
      DateStart                  DATETIME2     NULL,
      DateTerminated             DATETIME2     NULL,
      Description                VARCHAR(1000) NULL,
      LeaseType                  VARCHAR(30)   NULL,
      LeaseFloors                VARCHAR(100)  NULL,
      HvacLightSystem            VARCHAR(10)   NULL,
      ShortTermLease             BIT           NULL,
      LandDescription            VARCHAR(1000) NULL,
      LandlordContact            VARCHAR(100)  NULL,
      LandlordName               VARCHAR(50)   NULL,
      Sublease                   VARCHAR(10)   NULL,
      MultiTenant                BIT           NULL,
      NonStandard                BIT           NULL,
      OperationCostType          VARCHAR(20)   NULL,
      Option1                    VARCHAR(20)   NULL,
      ParkDaysNotice             VARCHAR(10)   NULL,
      ParkMaxRandom              VARCHAR(10)   NULL,
      ProjectId                  VARCHAR(30)   NULL,
      Reconciliation             BIT           NULL,
      ReductionSpace             BIT           NULL,
      RemitTo                    VARCHAR(50)   NULL,
      RentableLandHectares       DECIMAL(18,2) NULL,
      SavingLoss                 VARCHAR(20)   NULL,
      SharePrpnt                 VARCHAR(20)   NULL,
      StandardDocs               VARCHAR(20)   NULL,
      TaxBillNumber              VARCHAR(100)  NULL,
      TaxProportionShare         VARCHAR(20)   NULL,
      TaxRatePer                 VARCHAR(20)   NULL,
      TaxType                    VARCHAR(20)   NULL,
      LeaseTenantName            VARCHAR(20)   NULL,
      Translink                  VARCHAR(10)   NULL,
      UniqueShare                VARCHAR(10)   NULL,
      LeaseVersion               VARCHAR(10)   NULL,
      BronzeBatchId              BIGINT        NOT NULL,
      BronzeLoadTs               DATETIME2(0)  NOT NULL,
      CONSTRAINT PK_silver_lease
        PRIMARY KEY CLUSTERED ({paste(TARGET_PRIMARY_KEY, collapse = ', ')})
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
