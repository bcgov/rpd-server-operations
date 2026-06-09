# For server logging
# Begin timer
task_start <- Sys.time()

# Load helper functions
source(here::here("utilities/R/utilities.R"))

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

# Setup necessary variables
ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "CbreStaging"
TABLE_NAME <- "fm_fact_workorder"
CBRE_TABLE_NAME <- "fm_fact_workorder_vw"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
API_NAME <- "CBRE"
SCRIPT_NAME <- "fm_fact_workorder"

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

if (is.null(raw_data$data) || nrow(raw_data$data) == 0) {
  cat(
    "No data returned from API for window",
    etl_window$start_time,
    "to",
    etl_window$end_time,
    "— nothing to load. Exiting gracefully.\n"
  )
  stop("No new data from API")
}

clean_data <- raw_data |>
  # purrr::pluck("data") |>
  # # comment out these after initial data analysis as risk of
  # # losing columns in small data loads
  # select_if(~ !all(is.na(.))) |>
  # select_if(~ !all(. == 0)) |>
  # select_if(~ !all(. == '-1')) |>
  # select_if(~ !all(. == "N/A")) |>
  # select_if(~ !all(. == "-")) |>
  mutate(RefreshDate = as.POSIXct(Sys.time())) |>
  mutate(
    across(
      c(
        parent_workorder_skey,
        maintenance_skey,
        worker_skey,
        property_skey,
        equipment_skey,
        vendor_skey,
        vendor_site_skey,
        problem_code_skey,
        caller_skey,
        campus_skey,
        workorder_skey,
        property_hierarchy_skey,
        local_currency_skey,
        workorder_type_skey,
        status_code_skey,
        maintenance_plan_skey,
        repair_code_skey,
        equipment_sub_category_skey,
        priority_code_skey,
        cbre_standardized_workorder_type_skey,
        cbre_standardized_cost_category_skey,
        cbre_standardized_workorder_category_skey
      ),
      as.character
    )
  ) |>
  mutate(
    across(
      c(
        edp_update_ts,
        estimated_completion_date_ts,
        actual_completion_date_ts,
        estimated_response_date_ts,
        actual_arrival_ts,
        actual_completed_ts,
        target_completed_ts,
        actual_start_ts,
        target_start_ts,
        due_date_ts,
        scheduled_finish_ts,
        scheduled_start_ts,
        workorder_creation_date_ts,
        workorder_dispatched_date_ts,
        estimated_response_date_ts,
        actual_response_date_ts,
        activity_change_date,
        activity_status_date,
        local_workorder_creation_date_ts,
        local_activity_status_date,
        local_activity_change_date,
        local_actual_response_date_ts,
        local_target_start_ts,
        local_workorder_dispatched_date_ts,
        local_scheduled_finish_ts,
        local_actual_completion_date_ts,
        local_target_completed_ts,
        local_estimated_response_date_ts,
        local_actual_arrival_ts,
        local_actual_completed_ts,
        local_estimated_completion_date_ts,
        local_scheduled_start_ts,
        local_due_date_ts,
        local_actual_start_ts,
        local_source_created_ts,
        source_created_ts,
        closed_date
      ),
      ~ as.POSIXct(.x, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC")
    )
  ) |>
  mutate(
    across(
      c(
        actual_cost,
        total_labor_logged_hours,
        bid_amount
      ),
      as.double
    )
  ) |>
  select(
    RefreshDate,
    workorder_skey,
    workorder_number,
    estimated_completion_date_ts,
    parent_workorder_skey,
    source_created_ts,
    source_created_by,
    status_code_desc,
    status_code,
    status_code_skey,
    actual_cost,
    employee_cost_center,
    edp_update_by,
    edp_update_ts,
    source_system_code,
    source_unique_id,
    source_record_hash,
    client_skey,
    source_client_name,
    activity_desc,
    activity_name,
    asset_sub_category_code_desc,
    asset_sub_category_code,
    asset_category_code_desc,
    asset_category_code,
    actual_arrival_ts,
    actual_completed_ts,
    target_completed_ts,
    actual_start_ts,
    target_start_ts,
    due_date_ts,
    scheduled_total_duration,
    scheduled_labor_hours_duration,
    scheduled_finish_ts,
    scheduled_start_ts,
    total_labor_logged_hours,
    completion_notes,
    created_by_user,
    generated_by_system,
    repair_code_desc,
    repair_code,
    repair_code_skey,
    repair_category_code,
    repair_category_code_desc,
    maintenance_skey,
    maintenance_plan_skey,
    worker_skey,
    property_skey,
    property_hierarchy_skey,
    equipment_skey,
    equipment_sub_category_skey,
    vendor_skey,
    vendor_site_skey,
    workorder_creation_date_ts,
    workorder_dispatched_date_ts,
    estimated_response_date_ts,
    actual_response_date_ts,
    actual_completion_date_ts,
    estimated_wrench_time,
    actual_wrench_time,
    workorder_desc,
    workorder_comments,
    priority_code,
    priority_code_desc,
    priority_code_skey,
    type_code,
    type_code_desc,
    workorder_type_skey,
    category_code,
    category_code_desc,
    sub_category_code,
    sub_category_code_desc,
    customer_order_number,
    assigned_name,
    reference_01,
    reference_02,
    reference_03,
    reference_04,
    reference_05,
    reference_06,
    reference_07,
    reference_08,
    time_zone,
    bid_amount,
    problem_code_skey,
    caller_skey,
    requested_by_type_id,
    created_on_behalf_of,
    parent_workorder_number,
    parent_workorder_relationship_name,
    region_skey,
    campus_skey,
    activity_change_date,
    activity_status_date,
    local_currency_skey,
    closed_date,
    local_workorder_creation_date_ts,
    local_activity_status_date,
    local_activity_change_date,
    local_actual_response_date_ts,
    local_target_start_ts,
    local_workorder_dispatched_date_ts,
    local_scheduled_finish_ts,
    local_actual_completion_date_ts,
    local_target_completed_ts,
    local_estimated_response_date_ts,
    local_actual_arrival_ts,
    local_actual_completed_ts,
    local_estimated_completion_date_ts,
    local_scheduled_start_ts,
    local_due_date_ts,
    local_actual_start_ts,
    local_source_created_ts,
    cbre_standardized_workorder_type_code,
    cbre_standardized_cost_category_code,
    cbre_standardized_workorder_type_skey,
    cbre_standardized_workorder_category_code,
    cbre_standardized_cost_category_skey,
    cbre_standardized_workorder_category_skey
  )

# dbRemoveTable(con, TARGET_TABLE)
if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- paste0(
    "CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    TABLE_NAME,
    " (
      RefreshDate                                DATETIME2(3)   NOT NULL,
      workorder_skey                             NVARCHAR(50)   NOT NULL,
      workorder_number                           NVARCHAR(50)   NOT NULL,
      estimated_completion_date_ts               DATETIME2(3)   NULL,
      parent_workorder_skey                      NVARCHAR(50)   NULL,
      source_created_ts                          DATETIME2(3)   NULL,
      source_created_by                          NVARCHAR(255)  NULL,
      status_code_desc                           NVARCHAR(255)  NULL,
      status_code                                NVARCHAR(50)   NULL,
      status_code_skey                           NVARCHAR(50)   NULL,
      actual_cost                                DECIMAL(18,2)  NULL,
      employee_cost_center                       NVARCHAR(100)  NULL,
      edp_update_ts                              DATETIME2(3)   NULL,
      source_system_code                         NVARCHAR(50)   NULL,
      source_unique_id                           NVARCHAR(50)   NULL,
      activity_desc                              NVARCHAR(MAX)  NULL,
      activity_name                              NVARCHAR(MAX)  NULL,
      asset_sub_category_code_desc               NVARCHAR(255)  NULL,
      asset_sub_category_code                    NVARCHAR(50)   NULL,
      asset_category_code_desc                   NVARCHAR(255)  NULL,
      asset_category_code                        NVARCHAR(50)   NULL,
      actual_arrival_ts                          DATETIME2(3)   NULL,
      actual_completed_ts                        DATETIME2(3)   NULL,
      target_completed_ts                        DATETIME2(3)   NULL,
      actual_start_ts                            DATETIME2(3)   NULL,
      target_start_ts                            DATETIME2(3)   NULL,
      due_date_ts                                DATETIME2(3)   NULL,
      scheduled_total_duration                   NVARCHAR(50)   NULL,
      scheduled_labor_hours_duration             NVARCHAR(50)   NULL,
      scheduled_finish_ts                        DATETIME2(3)   NULL,
      scheduled_start_ts                         DATETIME2(3)   NULL,
      total_labor_logged_hours                   DECIMAL(18,2)  NULL,
      completion_notes                           NVARCHAR(MAX)  NULL,
      created_by_user                            NVARCHAR(255)  NULL,
      generated_by_system                        NVARCHAR(100)  NULL,
      repair_code_desc                           NVARCHAR(255)  NULL,
      repair_code                                NVARCHAR(50)   NULL,
      repair_code_skey                           NVARCHAR(50)   NULL,
      repair_category_code                       NVARCHAR(50)   NULL,
      repair_category_code_desc                  NVARCHAR(255)  NULL,
      maintenance_skey                           NVARCHAR(50)   NULL,
      maintenance_plan_skey                      NVARCHAR(50)   NULL,
      worker_skey                                NVARCHAR(50)   NULL,
      property_skey                              NVARCHAR(50)   NULL,
      property_hierarchy_skey                    NVARCHAR(50)   NULL,
      equipment_skey                             NVARCHAR(50)   NULL,
      equipment_sub_category_skey                NVARCHAR(50)   NULL,
      vendor_skey                                NVARCHAR(50)   NULL,
      vendor_site_skey                           NVARCHAR(50)   NULL,
      workorder_creation_date_ts                 DATETIME2(3)   NULL,
      workorder_dispatched_date_ts               DATETIME2(3)   NULL,
      estimated_response_date_ts                 DATETIME2(3)   NULL,
      actual_response_date_ts                    DATETIME2(3)   NULL,
      actual_completion_date_ts                  DATETIME2(3)   NULL,
      estimated_wrench_time                      NVARCHAR(50)   NULL,
      actual_wrench_time                         NVARCHAR(50)   NULL,
      workorder_desc                             NVARCHAR(MAX)  NULL,
      workorder_comments                         NVARCHAR(MAX)  NULL,
      priority_code                              NVARCHAR(50)   NULL,
      priority_code_desc                         NVARCHAR(255)  NULL,
      priority_code_skey                         NVARCHAR(50)   NULL,
      type_code                                  NVARCHAR(50)   NULL,
      type_code_desc                             NVARCHAR(255)  NULL,
      workorder_type_skey                        NVARCHAR(50)   NULL,
      category_code                              NVARCHAR(50)   NULL,
      category_code_desc                         NVARCHAR(255)  NULL,
      sub_category_code                          NVARCHAR(50)   NULL,
      sub_category_code_desc                     NVARCHAR(255)  NULL,
      customer_order_number                      NVARCHAR(100)  NULL,
      assigned_name                              NVARCHAR(255)  NULL,
      reference_01                               NVARCHAR(255)  NULL,
      reference_02                               NVARCHAR(255)  NULL,
      reference_03                               NVARCHAR(255)  NULL,
      reference_04                               NVARCHAR(255)  NULL,
      reference_05                               NVARCHAR(255)  NULL,
      reference_06                               NVARCHAR(255)  NULL,
      reference_07                               NVARCHAR(255)  NULL,
      reference_08                               NVARCHAR(255)  NULL,
      time_zone                                  NVARCHAR(50)   NULL,
      bid_amount                                 DECIMAL(18,2)  NULL,
      problem_code_skey                          NVARCHAR(50)   NULL,
      caller_skey                                NVARCHAR(50)   NULL,
      requested_by_type_id                       INT            NULL,
      created_on_behalf_of                       NVARCHAR(255)  NULL,
      parent_workorder_number                    NVARCHAR(50)   NULL,
      parent_workorder_relationship_name         NVARCHAR(255)  NULL,
      region_skey                                NVARCHAR(50)   NULL,
      campus_skey                                NVARCHAR(50)   NULL,
      activity_change_date                       DATETIME2(3)   NULL,
      activity_status_date                       DATETIME2(3)   NULL,
      local_currency_skey                        NVARCHAR(50)   NULL,
      closed_date                                DATETIME2(3)   NULL,
      local_workorder_creation_date_ts           DATETIME2(3)   NULL,
      local_activity_status_date                 DATETIME2(3)   NULL,
      local_activity_change_date                 DATETIME2(3)   NULL,
      local_actual_response_date_ts              DATETIME2(3)   NULL,
      local_target_start_ts                      DATETIME2(3)   NULL,
      local_workorder_dispatched_date_ts         DATETIME2(3)   NULL,
      local_scheduled_finish_ts                  DATETIME2(3)   NULL,
      local_actual_completion_date_ts            DATETIME2(3)   NULL,
      local_target_completed_ts                  DATETIME2(3)   NULL,
      local_estimated_response_date_ts           DATETIME2(3)   NULL,
      local_actual_arrival_ts                    DATETIME2(3)   NULL,
      local_actual_completed_ts                  DATETIME2(3)   NULL,
      local_estimated_completion_date_ts         DATETIME2(3)   NULL,
      local_scheduled_start_ts                   DATETIME2(3)   NULL,
      local_due_date_ts                          DATETIME2(3)   NULL,
      local_actual_start_ts                      DATETIME2(3)   NULL,
      local_source_created_ts                    DATETIME2(3)   NULL,
      cbre_standardized_workorder_type_code      NVARCHAR(100)  NULL,
      cbre_standardized_cost_category_code       NVARCHAR(100)  NULL,
      cbre_standardized_workorder_type_skey      NVARCHAR(50)   NULL,
      cbre_standardized_workorder_category_code  NVARCHAR(100)  NULL,
      cbre_standardized_cost_category_skey       NVARCHAR(50)   NULL,
      cbre_standardized_workorder_category_skey  NVARCHAR(50)   NULL
    );"
  )

  dbExecute(con, sql)
}

clean_data <- read.csv(here("input/CbreStaging/fm_workorder.csv")) |>
  mutate(
    across(
      c(
        RefreshDate
      ),
      ~ as.POSIXct(.x, format = "%Y-%m-%d %H:%M:%OS", tz = "America/Vancouver")
    )
  ) |>
  mutate(
    across(
      c(
        edp_update_ts,
        estimated_completion_date_ts,
        actual_completion_date_ts,
        estimated_response_date_ts,
        actual_arrival_ts,
        actual_completed_ts,
        target_completed_ts,
        actual_start_ts,
        target_start_ts,
        due_date_ts,
        scheduled_finish_ts,
        scheduled_start_ts,
        workorder_creation_date_ts,
        workorder_dispatched_date_ts,
        estimated_response_date_ts,
        actual_response_date_ts,
        activity_change_date,
        activity_status_date,
        local_workorder_creation_date_ts,
        local_activity_status_date,
        local_activity_change_date,
        local_actual_response_date_ts,
        local_target_start_ts,
        local_workorder_dispatched_date_ts,
        local_scheduled_finish_ts,
        local_actual_completion_date_ts,
        local_target_completed_ts,
        local_estimated_response_date_ts,
        local_actual_arrival_ts,
        local_actual_completed_ts,
        local_estimated_completion_date_ts,
        local_scheduled_start_ts,
        local_due_date_ts,
        local_actual_start_ts,
        local_source_created_ts,
        source_created_ts,
        closed_date
      ),
      ~ as.POSIXct(.x, format = "%Y-%m-%d %H:%M:%OS")
    )
  ) |>
  select(
    -c(
      edp_update_by,
      source_record_hash,
      client_skey,
      source_client_name
    )
  )

clean_data_1 <- clean_data[1:250000, ]
clean_data_2 <- clean_data[250001:500000, ]
clean_data_3 <- clean_data[500001:750000, ]
clean_data_4 <- clean_data[750001:1000000, ]
clean_data_5 <- clean_data[1000001:1250000, ]
clean_data_6 <- clean_data[1250001:1500000, ]
clean_data_7 <- clean_data[1500001:1750000, ]
clean_data_8 <- clean_data[1750001:2000000, ]
clean_data_9 <- clean_data[2000001:2213706, ]

data <- clean_data_7 # ran out of db space at this point.

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
        "CREATE TABLE ",
        TEMP_TABLE,
        " (
          RefreshDate                                DATETIME2(3)   NOT NULL,
          workorder_skey                             NVARCHAR(50)   NOT NULL,
          workorder_number                           NVARCHAR(50)   NOT NULL,
          estimated_completion_date_ts               DATETIME2(3)   NULL,
          parent_workorder_skey                      NVARCHAR(50)   NULL,
          source_created_ts                          DATETIME2(3)   NULL,
          source_created_by                          NVARCHAR(255)  NULL,
          status_code_desc                           NVARCHAR(255)  NULL,
          status_code                                NVARCHAR(50)   NULL,
          status_code_skey                           NVARCHAR(50)   NULL,
          actual_cost                                DECIMAL(18,2)  NULL,
          employee_cost_center                       NVARCHAR(100)  NULL,
          edp_update_ts                              DATETIME2(3)   NULL,
          source_system_code                         NVARCHAR(50)   NULL,
          source_unique_id                           NVARCHAR(50)   NULL,
          activity_desc                              NVARCHAR(MAX)  NULL,
          activity_name                              NVARCHAR(MAX)  NULL,
          asset_sub_category_code_desc               NVARCHAR(255)  NULL,
          asset_sub_category_code                    NVARCHAR(50)   NULL,
          asset_category_code_desc                   NVARCHAR(255)  NULL,
          asset_category_code                        NVARCHAR(50)   NULL,
          actual_arrival_ts                          DATETIME2(3)   NULL,
          actual_completed_ts                        DATETIME2(3)   NULL,
          target_completed_ts                        DATETIME2(3)   NULL,
          actual_start_ts                            DATETIME2(3)   NULL,
          target_start_ts                            DATETIME2(3)   NULL,
          due_date_ts                                DATETIME2(3)   NULL,
          scheduled_total_duration                   NVARCHAR(50)   NULL,
          scheduled_labor_hours_duration             NVARCHAR(50)   NULL,
          scheduled_finish_ts                        DATETIME2(3)   NULL,
          scheduled_start_ts                         DATETIME2(3)   NULL,
          total_labor_logged_hours                   DECIMAL(18,2)  NULL,
          completion_notes                           NVARCHAR(MAX)  NULL,
          created_by_user                            NVARCHAR(255)  NULL,
          generated_by_system                        NVARCHAR(100)  NULL,
          repair_code_desc                           NVARCHAR(255)  NULL,
          repair_code                                NVARCHAR(50)   NULL,
          repair_code_skey                           NVARCHAR(50)   NULL,
          repair_category_code                       NVARCHAR(50)   NULL,
          repair_category_code_desc                  NVARCHAR(255)  NULL,
          maintenance_skey                           NVARCHAR(50)   NULL,
          maintenance_plan_skey                      NVARCHAR(50)   NULL,
          worker_skey                                NVARCHAR(50)   NULL,
          property_skey                              NVARCHAR(50)   NULL,
          property_hierarchy_skey                    NVARCHAR(50)   NULL,
          equipment_skey                             NVARCHAR(50)   NULL,
          equipment_sub_category_skey                NVARCHAR(50)   NULL,
          vendor_skey                                NVARCHAR(50)   NULL,
          vendor_site_skey                           NVARCHAR(50)   NULL,
          workorder_creation_date_ts                 DATETIME2(3)   NULL,
          workorder_dispatched_date_ts               DATETIME2(3)   NULL,
          estimated_response_date_ts                 DATETIME2(3)   NULL,
          actual_response_date_ts                    DATETIME2(3)   NULL,
          actual_completion_date_ts                  DATETIME2(3)   NULL,
          estimated_wrench_time                      NVARCHAR(50)   NULL,
          actual_wrench_time                         NVARCHAR(50)   NULL,
          workorder_desc                             NVARCHAR(MAX)  NULL,
          workorder_comments                         NVARCHAR(MAX)  NULL,
          priority_code                              NVARCHAR(50)   NULL,
          priority_code_desc                         NVARCHAR(255)  NULL,
          priority_code_skey                         NVARCHAR(50)   NULL,
          type_code                                  NVARCHAR(50)   NULL,
          type_code_desc                             NVARCHAR(255)  NULL,
          workorder_type_skey                        NVARCHAR(50)   NULL,
          category_code                              NVARCHAR(50)   NULL,
          category_code_desc                         NVARCHAR(255)  NULL,
          sub_category_code                          NVARCHAR(50)   NULL,
          sub_category_code_desc                     NVARCHAR(255)  NULL,
          customer_order_number                      NVARCHAR(100)  NULL,
          assigned_name                              NVARCHAR(255)  NULL,
          reference_01                               NVARCHAR(255)  NULL,
          reference_02                               NVARCHAR(255)  NULL,
          reference_03                               NVARCHAR(255)  NULL,
          reference_04                               NVARCHAR(255)  NULL,
          reference_05                               NVARCHAR(255)  NULL,
          reference_06                               NVARCHAR(255)  NULL,
          reference_07                               NVARCHAR(255)  NULL,
          reference_08                               NVARCHAR(255)  NULL,
          time_zone                                  NVARCHAR(50)   NULL,
          bid_amount                                 DECIMAL(18,2)  NULL,
          problem_code_skey                          NVARCHAR(50)   NULL,
          caller_skey                                NVARCHAR(50)   NULL,
          requested_by_type_id                       INT            NULL,
          created_on_behalf_of                       NVARCHAR(255)  NULL,
          parent_workorder_number                    NVARCHAR(50)   NULL,
          parent_workorder_relationship_name         NVARCHAR(255)  NULL,
          region_skey                                NVARCHAR(50)   NULL,
          campus_skey                                NVARCHAR(50)   NULL,
          activity_change_date                       DATETIME2(3)   NULL,
          activity_status_date                       DATETIME2(3)   NULL,
          local_currency_skey                        NVARCHAR(50)   NULL,
          closed_date                                DATETIME2(3)   NULL,
          local_workorder_creation_date_ts           DATETIME2(3)   NULL,
          local_activity_status_date                 DATETIME2(3)   NULL,
          local_activity_change_date                 DATETIME2(3)   NULL,
          local_actual_response_date_ts              DATETIME2(3)   NULL,
          local_target_start_ts                      DATETIME2(3)   NULL,
          local_workorder_dispatched_date_ts         DATETIME2(3)   NULL,
          local_scheduled_finish_ts                  DATETIME2(3)   NULL,
          local_actual_completion_date_ts            DATETIME2(3)   NULL,
          local_target_completed_ts                  DATETIME2(3)   NULL,
          local_estimated_response_date_ts           DATETIME2(3)   NULL,
          local_actual_arrival_ts                    DATETIME2(3)   NULL,
          local_actual_completed_ts                  DATETIME2(3)   NULL,
          local_estimated_completion_date_ts         DATETIME2(3)   NULL,
          local_scheduled_start_ts                   DATETIME2(3)   NULL,
          local_due_date_ts                          DATETIME2(3)   NULL,
          local_actual_start_ts                      DATETIME2(3)   NULL,
          local_source_created_ts                    DATETIME2(3)   NULL,
          cbre_standardized_workorder_type_code      NVARCHAR(100)  NULL,
          cbre_standardized_cost_category_code       NVARCHAR(100)  NULL,
          cbre_standardized_workorder_type_skey      NVARCHAR(50)   NULL,
          cbre_standardized_workorder_category_code  NVARCHAR(100)  NULL,
          cbre_standardized_cost_category_skey       NVARCHAR(50)   NULL,
          cbre_standardized_workorder_category_skey  NVARCHAR(50)   NULL
        );"
      )
    )

    dbWriteTable(
      con,
      name = TEMP_TABLE,
      value = data,
      append = TRUE,
      overwrite = FALSE
    )

    # -- Guard: catch duplicate keys in source data before touching target --
    dup_count <- dbGetQuery(
      con,
      paste0(
        "SELECT COUNT(*) AS n
         FROM (
           SELECT workorder_skey
           FROM ",
        TEMP_TABLE,
        "
           GROUP BY workorder_skey
           HAVING COUNT(*) > 1
         ) dupes;"
      )
    )$n

    if (dup_count > 0) {
      stop(paste0(
        "Duplicate workorder_skey values detected in source data (",
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
           tgt.RefreshDate                                = src.RefreshDate,
           tgt.workorder_number                           = src.workorder_number,
           tgt.estimated_completion_date_ts               = src.estimated_completion_date_ts,
           tgt.parent_workorder_skey                      = src.parent_workorder_skey,
           tgt.source_created_ts                          = src.source_created_ts,
           tgt.source_created_by                          = src.source_created_by,
           tgt.status_code_desc                           = src.status_code_desc,
           tgt.status_code                                = src.status_code,
           tgt.status_code_skey                           = src.status_code_skey,
           tgt.actual_cost                                = src.actual_cost,
           tgt.employee_cost_center                       = src.employee_cost_center,
           tgt.edp_update_ts                              = src.edp_update_ts,
           tgt.source_system_code                         = src.source_system_code,
           tgt.source_unique_id                           = src.source_unique_id,
           tgt.activity_desc                              = src.activity_desc,
           tgt.activity_name                              = src.activity_name,
           tgt.asset_sub_category_code_desc               = src.asset_sub_category_code_desc,
           tgt.asset_sub_category_code                    = src.asset_sub_category_code,
           tgt.asset_category_code_desc                   = src.asset_category_code_desc,
           tgt.asset_category_code                        = src.asset_category_code,
           tgt.actual_arrival_ts                          = src.actual_arrival_ts,
           tgt.actual_completed_ts                        = src.actual_completed_ts,
           tgt.target_completed_ts                        = src.target_completed_ts,
           tgt.actual_start_ts                            = src.actual_start_ts,
           tgt.target_start_ts                            = src.target_start_ts,
           tgt.due_date_ts                                = src.due_date_ts,
           tgt.scheduled_total_duration                   = src.scheduled_total_duration,
           tgt.scheduled_labor_hours_duration             = src.scheduled_labor_hours_duration,
           tgt.scheduled_finish_ts                        = src.scheduled_finish_ts,
           tgt.scheduled_start_ts                         = src.scheduled_start_ts,
           tgt.total_labor_logged_hours                   = src.total_labor_logged_hours,
           tgt.completion_notes                           = src.completion_notes,
           tgt.created_by_user                            = src.created_by_user,
           tgt.generated_by_system                        = src.generated_by_system,
           tgt.repair_code_desc                           = src.repair_code_desc,
           tgt.repair_code                                = src.repair_code,
           tgt.repair_code_skey                           = src.repair_code_skey,
           tgt.repair_category_code                       = src.repair_category_code,
           tgt.repair_category_code_desc                  = src.repair_category_code_desc,
           tgt.maintenance_skey                           = src.maintenance_skey,
           tgt.maintenance_plan_skey                      = src.maintenance_plan_skey,
           tgt.worker_skey                                = src.worker_skey,
           tgt.property_skey                              = src.property_skey,
           tgt.property_hierarchy_skey                    = src.property_hierarchy_skey,
           tgt.equipment_skey                             = src.equipment_skey,
           tgt.equipment_sub_category_skey                = src.equipment_sub_category_skey,
           tgt.vendor_skey                                = src.vendor_skey,
           tgt.vendor_site_skey                           = src.vendor_site_skey,
           tgt.workorder_creation_date_ts                 = src.workorder_creation_date_ts,
           tgt.workorder_dispatched_date_ts               = src.workorder_dispatched_date_ts,
           tgt.estimated_response_date_ts                 = src.estimated_response_date_ts,
           tgt.actual_response_date_ts                    = src.actual_response_date_ts,
           tgt.actual_completion_date_ts                  = src.actual_completion_date_ts,
           tgt.estimated_wrench_time                      = src.estimated_wrench_time,
           tgt.actual_wrench_time                         = src.actual_wrench_time,
           tgt.workorder_desc                             = src.workorder_desc,
           tgt.workorder_comments                         = src.workorder_comments,
           tgt.priority_code                              = src.priority_code,
           tgt.priority_code_desc                         = src.priority_code_desc,
           tgt.priority_code_skey                         = src.priority_code_skey,
           tgt.type_code                                  = src.type_code,
           tgt.type_code_desc                             = src.type_code_desc,
           tgt.workorder_type_skey                        = src.workorder_type_skey,
           tgt.category_code                              = src.category_code,
           tgt.category_code_desc                         = src.category_code_desc,
           tgt.sub_category_code                          = src.sub_category_code,
           tgt.sub_category_code_desc                     = src.sub_category_code_desc,
           tgt.customer_order_number                      = src.customer_order_number,
           tgt.assigned_name                              = src.assigned_name,
           tgt.reference_01                               = src.reference_01,
           tgt.reference_02                               = src.reference_02,
           tgt.reference_03                               = src.reference_03,
           tgt.reference_04                               = src.reference_04,
           tgt.reference_05                               = src.reference_05,
           tgt.reference_06                               = src.reference_06,
           tgt.reference_07                               = src.reference_07,
           tgt.reference_08                               = src.reference_08,
           tgt.time_zone                                  = src.time_zone,
           tgt.bid_amount                                 = src.bid_amount,
           tgt.problem_code_skey                          = src.problem_code_skey,
           tgt.caller_skey                                = src.caller_skey,
           tgt.requested_by_type_id                       = src.requested_by_type_id,
           tgt.created_on_behalf_of                       = src.created_on_behalf_of,
           tgt.parent_workorder_number                    = src.parent_workorder_number,
           tgt.parent_workorder_relationship_name         = src.parent_workorder_relationship_name,
           tgt.region_skey                                = src.region_skey,
           tgt.campus_skey                                = src.campus_skey,
           tgt.activity_change_date                       = src.activity_change_date,
           tgt.activity_status_date                       = src.activity_status_date,
           tgt.local_currency_skey                        = src.local_currency_skey,
           tgt.closed_date                                = src.closed_date,
           tgt.local_workorder_creation_date_ts           = src.local_workorder_creation_date_ts,
           tgt.local_activity_status_date                 = src.local_activity_status_date,
           tgt.local_activity_change_date                 = src.local_activity_change_date,
           tgt.local_actual_response_date_ts              = src.local_actual_response_date_ts,
           tgt.local_target_start_ts                      = src.local_target_start_ts,
           tgt.local_workorder_dispatched_date_ts         = src.local_workorder_dispatched_date_ts,
           tgt.local_scheduled_finish_ts                  = src.local_scheduled_finish_ts,
           tgt.local_actual_completion_date_ts            = src.local_actual_completion_date_ts,
           tgt.local_target_completed_ts                  = src.local_target_completed_ts,
           tgt.local_estimated_response_date_ts           = src.local_estimated_response_date_ts,
           tgt.local_actual_arrival_ts                    = src.local_actual_arrival_ts,
           tgt.local_actual_completed_ts                  = src.local_actual_completed_ts,
           tgt.local_estimated_completion_date_ts         = src.local_estimated_completion_date_ts,
           tgt.local_scheduled_start_ts                   = src.local_scheduled_start_ts,
           tgt.local_due_date_ts                          = src.local_due_date_ts,
           tgt.local_actual_start_ts                      = src.local_actual_start_ts,
           tgt.local_source_created_ts                    = src.local_source_created_ts,
           tgt.cbre_standardized_workorder_type_code      = src.cbre_standardized_workorder_type_code,
           tgt.cbre_standardized_cost_category_code       = src.cbre_standardized_cost_category_code,
           tgt.cbre_standardized_workorder_type_skey      = src.cbre_standardized_workorder_type_skey,
           tgt.cbre_standardized_workorder_category_code  = src.cbre_standardized_workorder_category_code,
           tgt.cbre_standardized_cost_category_skey       = src.cbre_standardized_cost_category_skey,
           tgt.cbre_standardized_workorder_category_skey  = src.cbre_standardized_workorder_category_skey
         FROM ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
         INNER JOIN ",
        TEMP_TABLE,
        " src
           ON tgt.workorder_skey = src.workorder_skey;"
      )
    )

    # -- Insert new rows --
    n_inserted <- dbExecute(
      con,
      paste0(
        "INSERT INTO ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " (
          RefreshDate,
          workorder_skey,
          workorder_number,
          estimated_completion_date_ts,
          parent_workorder_skey,
          source_created_ts,
          source_created_by,
          status_code_desc,
          status_code,
          status_code_skey,
          actual_cost,
          employee_cost_center,
          edp_update_ts,
          source_system_code,
          source_unique_id,
          activity_desc,
          activity_name,
          asset_sub_category_code_desc,
          asset_sub_category_code,
          asset_category_code_desc,
          asset_category_code,
          actual_arrival_ts,
          actual_completed_ts,
          target_completed_ts,
          actual_start_ts,
          target_start_ts,
          due_date_ts,
          scheduled_total_duration,
          scheduled_labor_hours_duration,
          scheduled_finish_ts,
          scheduled_start_ts,
          total_labor_logged_hours,
          completion_notes,
          created_by_user,
          generated_by_system,
          repair_code_desc,
          repair_code,
          repair_code_skey,
          repair_category_code,
          repair_category_code_desc,
          maintenance_skey,
          maintenance_plan_skey,
          worker_skey,
          property_skey,
          property_hierarchy_skey,
          equipment_skey,
          equipment_sub_category_skey,
          vendor_skey,
          vendor_site_skey,
          workorder_creation_date_ts,
          workorder_dispatched_date_ts,
          estimated_response_date_ts,
          actual_response_date_ts,
          actual_completion_date_ts,
          estimated_wrench_time,
          actual_wrench_time,
          workorder_desc,
          workorder_comments,
          priority_code,
          priority_code_desc,
          priority_code_skey,
          type_code,
          type_code_desc,
          workorder_type_skey,
          category_code,
          category_code_desc,
          sub_category_code,
          sub_category_code_desc,
          customer_order_number,
          assigned_name,
          reference_01,
          reference_02,
          reference_03,
          reference_04,
          reference_05,
          reference_06,
          reference_07,
          reference_08,
          time_zone,
          bid_amount,
          problem_code_skey,
          caller_skey,
          requested_by_type_id,
          created_on_behalf_of,
          parent_workorder_number,
          parent_workorder_relationship_name,
          region_skey,
          campus_skey,
          activity_change_date,
          activity_status_date,
          local_currency_skey,
          closed_date,
          local_workorder_creation_date_ts,
          local_activity_status_date,
          local_activity_change_date,
          local_actual_response_date_ts,
          local_target_start_ts,
          local_workorder_dispatched_date_ts,
          local_scheduled_finish_ts,
          local_actual_completion_date_ts,
          local_target_completed_ts,
          local_estimated_response_date_ts,
          local_actual_arrival_ts,
          local_actual_completed_ts,
          local_estimated_completion_date_ts,
          local_scheduled_start_ts,
          local_due_date_ts,
          local_actual_start_ts,
          local_source_created_ts,
          cbre_standardized_workorder_type_code,
          cbre_standardized_cost_category_code,
          cbre_standardized_workorder_type_skey,
          cbre_standardized_workorder_category_code,
          cbre_standardized_cost_category_skey,
          cbre_standardized_workorder_category_skey
        )
        SELECT
          src.RefreshDate,
          src.workorder_skey,
          src.workorder_number,
          src.estimated_completion_date_ts,
          src.parent_workorder_skey,
          src.source_created_ts,
          src.source_created_by,
          src.status_code_desc,
          src.status_code,
          src.status_code_skey,
          src.actual_cost,
          src.employee_cost_center,
          src.edp_update_ts,
          src.source_system_code,
          src.source_unique_id,
          src.activity_desc,
          src.activity_name,
          src.asset_sub_category_code_desc,
          src.asset_sub_category_code,
          src.asset_category_code_desc,
          src.asset_category_code,
          src.actual_arrival_ts,
          src.actual_completed_ts,
          src.target_completed_ts,
          src.actual_start_ts,
          src.target_start_ts,
          src.due_date_ts,
          src.scheduled_total_duration,
          src.scheduled_labor_hours_duration,
          src.scheduled_finish_ts,
          src.scheduled_start_ts,
          src.total_labor_logged_hours,
          src.completion_notes,
          src.created_by_user,
          src.generated_by_system,
          src.repair_code_desc,
          src.repair_code,
          src.repair_code_skey,
          src.repair_category_code,
          src.repair_category_code_desc,
          src.maintenance_skey,
          src.maintenance_plan_skey,
          src.worker_skey,
          src.property_skey,
          src.property_hierarchy_skey,
          src.equipment_skey,
          src.equipment_sub_category_skey,
          src.vendor_skey,
          src.vendor_site_skey,
          src.workorder_creation_date_ts,
          src.workorder_dispatched_date_ts,
          src.estimated_response_date_ts,
          src.actual_response_date_ts,
          src.actual_completion_date_ts,
          src.estimated_wrench_time,
          src.actual_wrench_time,
          src.workorder_desc,
          src.workorder_comments,
          src.priority_code,
          src.priority_code_desc,
          src.priority_code_skey,
          src.type_code,
          src.type_code_desc,
          src.workorder_type_skey,
          src.category_code,
          src.category_code_desc,
          src.sub_category_code,
          src.sub_category_code_desc,
          src.customer_order_number,
          src.assigned_name,
          src.reference_01,
          src.reference_02,
          src.reference_03,
          src.reference_04,
          src.reference_05,
          src.reference_06,
          src.reference_07,
          src.reference_08,
          src.time_zone,
          src.bid_amount,
          src.problem_code_skey,
          src.caller_skey,
          src.requested_by_type_id,
          src.created_on_behalf_of,
          src.parent_workorder_number,
          src.parent_workorder_relationship_name,
          src.region_skey,
          src.campus_skey,
          src.activity_change_date,
          src.activity_status_date,
          src.local_currency_skey,
          src.closed_date,
          src.local_workorder_creation_date_ts,
          src.local_activity_status_date,
          src.local_activity_change_date,
          src.local_actual_response_date_ts,
          src.local_target_start_ts,
          src.local_workorder_dispatched_date_ts,
          src.local_scheduled_finish_ts,
          src.local_actual_completion_date_ts,
          src.local_target_completed_ts,
          src.local_estimated_response_date_ts,
          src.local_actual_arrival_ts,
          src.local_actual_completed_ts,
          src.local_estimated_completion_date_ts,
          src.local_scheduled_start_ts,
          src.local_due_date_ts,
          src.local_actual_start_ts,
          src.local_source_created_ts,
          src.cbre_standardized_workorder_type_code,
          src.cbre_standardized_cost_category_code,
          src.cbre_standardized_workorder_type_skey,
          src.cbre_standardized_workorder_category_code,
          src.cbre_standardized_cost_category_skey,
          src.cbre_standardized_workorder_category_skey
        FROM ",
        TEMP_TABLE,
        " src
        LEFT JOIN ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
          ON tgt.workorder_skey = src.workorder_skey
        WHERE tgt.workorder_skey IS NULL;"
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
