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
SCHEMA_NAME <- "RealProperty"
TABLE_NAME <- "PortfolioManagement"
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
SCRIPT_NAME <- "PortfolioManagement"
API_NAME <- "None"

# Connect to SQL database
con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = SQL_SERVER,
  database = DB_NAME,
  Trusted_Connection = "Yes"
)

# Query SQL Datasets ####
query <- dbSendQuery(con, "SELECT * FROM CbreStaging.archibus_bl")
BuildingData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.archibus_property")
PropertyData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.archibus_ls")
LeasingData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.archibus_dv")
DivisionData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.archibus_dp")
DepartmentData <- dbFetch(query, n = -1)
dbClearResult(query)

InitialData <- BuildingData |>
  left_join(LeasingData, by = join_by(BuildingId == ls_bl_id)) |>
  left_join(
    DepartmentData,
    by = join_by(ls_tn_name == dp_dp_id),
    relationship = "many-to-many"
  ) |> # 1214611 and 254536 are duplicated in Department
  left_join(DivisionData, by = join_by(dp_dv_id == dv_dv_id)) |>
  select(
    linkAddress,
    dv_name,
    dp_name,
    linkCity,
    BuildingId,
    ls_ls_id,
    ls_ls_parent_id,
    ls_option1,
    ls_lease_sublease,
    Tenure,
    #ContractType
    ls_status,
    #AgreementStatus,
    ls_version,
    ls_date_start,
    ls_date_end,
    ls_date_move,
    ls_date_terminated,
    FacilityType
  ) |>
  # Select lease status' of interest
  filter(
    ls_status %in% c("Terminated", "Retired", "Active", "Holdover")
  ) |>
  # Remove weird TBD BuildingIds
  filter(
    BuildingId != "TBD"
  ) |>
  # Remove C agreements
  filter(
    ls_lease_sublease != "C"
  ) |>
  mutate(
    LeaseLineage = case_when(
      !is.na(ls_option1) ~ ls_option1,
      .default = paste0(BuildingId, "_", row_number())
    ),
    .after = ls_option1
  ) |>
  group_by(LeaseLineage) |>
  filter(ls_version == max(ls_version)) |>
  ungroup() |>
  mutate(
    LeaseGroup = case_when(
      # When the row is the parent lease
      Tenure == "LEASED" &
        startsWith(ls_ls_id, "L") &
        is.na(ls_ls_parent_id) ~ stringr::str_extract(
        ls_ls_id,
        "(L\\d+)",
        group = 1
      ),
      # When the row is the parent parking lease
      Tenure == "LEASED" &
        startsWith(ls_ls_id, "P") &
        is.na(ls_ls_parent_id) ~ stringr::str_extract(
        ls_ls_id,
        "(P\\d+)",
        group = 1
      ),
      # When the row is a child agreement of a lease
      Tenure == "LEASED" &
        startsWith(ls_ls_parent_id, "L") &
        startsWith(ls_ls_id, "A") ~ stringr::str_extract(
        ls_ls_parent_id,
        "(L\\d+)",
        group = 1
      ),
      # When the row is a child agreement of a parking lease
      Tenure == "LEASED" &
        startsWith(ls_ls_parent_id, "P") &
        startsWith(ls_ls_id, "A") ~ stringr::str_extract(
        ls_ls_parent_id,
        "(P\\d+)",
        group = 1
      ),
      # When the row is an owned property but is a parent lease
      Tenure == "OWNED" &
        is.na(ls_ls_parent_id) &
        startsWith(ls_ls_id, "L") ~ stringr::str_extract(
        ls_ls_id,
        "(L\\d+)",
        group = 1
      ),
      # When the row is an owned property but is a parent parking lease
      Tenure == "OWNED" &
        is.na(ls_ls_parent_id) &
        startsWith(ls_ls_id, "P") ~ stringr::str_extract(
        ls_ls_id,
        "(P\\d+)",
        group = 1
      ),
      # When the row is an owned property but is a child agreement
      Tenure == "OWNED" &
        startsWith(ls_ls_parent_id, "L") ~ stringr::str_extract(
        ls_ls_parent_id,
        "(L\\d+)",
        group = 1
      ),
      # When the row is an owned property but is a child parking agreement
      Tenure == "OWNED" &
        startsWith(ls_ls_parent_id, "P") ~ stringr::str_extract(
        ls_ls_parent_id,
        "(P\\d+)",
        group = 1
      ),
      # When the row is an owned property and is not part of a lease
      Tenure == "OWNED" &
        is.na(ls_ls_parent_id) &
        is.na(ls_option1) ~ BuildingId,
      # When the row is an managed property and is not part of a lease
      Tenure == "MANAGED" &
        is.na(ls_ls_parent_id) &
        is.na(ls_option1) ~ BuildingId,
      .default = NA_character_
    ),
    .after = ls_option1
  )

Agreements <- InitialData |>
  filter(ls_lease_sublease == "A") |>
  rename(
    ContractCode = ls_ls_id,
    AgreementStatus = ls_status,
    LeaseVersion = ls_version,
    AgreementDateStart = ls_date_start,
    AgreementDateEnd = ls_date_end,
    AgreementDateTerminated = ls_date_terminated,
  ) |>
  mutate(
    Lease = NA_character_,
    LeaseStatus = NA_character_,
    LeaseDateStart = as.POSIXct(NA),
    LeaseDateEnd = as.POSIXct(NA),
    LeaseDateTerminated = as.POSIXct(NA)
  )

Leases <- InitialData |>
  filter(ls_lease_sublease %in% c("L", "P")) |>
  rename(
    Lease = ls_ls_id,
    LeaseStatus = ls_status,
    LeaseVersion = ls_version,
    LeaseDateStart = ls_date_start,
    LeaseDateEnd = ls_date_end,
    LeaseDateTerminated = ls_date_terminated
  ) |>
  mutate(
    ContractCode = NA_character_,
    AgreementStatus = NA_character_,
    AgreementDateStart = as.POSIXct(NA),
    AgreementDateEnd = as.POSIXct(NA),
    AgreementDateTerminated = as.POSIXct(NA),
  )

Output <- Agreements |>
  union(Leases) |>
  group_by(LeaseGroup) |>
  tidyr::fill(
    Lease,
    LeaseStatus,
    LeaseVersion,
    LeaseDateStart,
    LeaseDateEnd,
    LeaseDateTerminated,
    .direction = "updown"
  ) |>
  ungroup() |>
  filter(ls_lease_sublease == "A") |>
  select(
    Address = linkAddress,
    DivisionName = dv_name,
    DepartmentName = dp_name,
    City = linkCity,
    BuildingId,
    Lease,
    ContractCode,
    Tenure,
    AgreementStatus,
    LeaseStatus,
    LeaseVersion,
    AgreementDateStart,
    AgreementDateEnd,
    AgreementDateTerminated,
    LeaseDateStart,
    LeaseDateEnd,
    LeaseDateTerminated,
    FacilityType
  ) |>
  arrange(
    Address,
    BuildingId,
    ContractCode
  )


mutate(
  Region = case_when(
    City %in%
      c(
        "Victoria",
        "VICTORIA",
        "View Royal",
        "SAANICH",
        "Saanich",
        "Esquimalt"
      ) ~ "Victoria",
    City %in% c("Prince George", "PRINCE GEORGE") ~ "Prince George",
    City %in% c("Kamloops", "KAMLOOPS") ~ "Kamloops",
    City %in% c("Kelowna", "KELOWNA") ~ "Kelowna",
    City %in% c("Surrey", "SURREY") ~ "Surrey",
    City %in% c("Vancouver", "VANCOUVER") ~ "Vancouver",
    City %in% c("West Kelowna", "WEST KELOWNA") ~ "West Kelowna",
    .default = "None"
  )
)
