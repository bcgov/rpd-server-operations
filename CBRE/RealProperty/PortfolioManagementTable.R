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

Output <- BuildingData |>
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
  filter(
    ls_status %in% c("Terminated", "Retired", "Active", "Holdover")
  )

test <- Output |> filter(!is.na(ls_option1))
# have to sort out the Retired rows that are followed by an active row.
#|>
mutate(
  LeaseLineage =
)
mutate(
  LeaseGroup = case_when(
    !is.na(ls_ls_parent_id) &
      startsWith(ls_ls_id, "A") ~ stringr::str_extract(
      ls_ls_parent_id,
      "(L\\d+)",
      group = 1
    ),
    startsWith(ls_ls_id, "A") &
      startsWith(ls_ls_parent_id, "L") ~ stringr::str_extract(
      ls_ls_id,
      "-(L\\d+)",
      group = 1
    ),
    startsWith(ls_ls_id, "A") &
      startsWith(ls_ls_parent_id, "P") ~ stringr::str_extract(
      ls_ls_id,
      "-(P\\d+)",
      group = 1
    ),
    startsWith(ls_ls_id, "A") & is.na(ls_ls_parent_id) ~ stringr::str_extract(
      ls_ls_id,
      "-(B\\d+)",
      group = 1
    ),
    startsWith(ls_ls_id, "L") ~ stringr::str_extract(
      ls_ls_id,
      "(L\\d+)",
      group = 1
    ),
    startsWith(ls_ls_id, "P") ~ stringr::str_extract(
      ls_ls_id,
      "(P\\d+)",
      group = 1
    ),
    !(is.na(ls_option1)) ~ ls_option1,
    .default = "Missing"
  ),
  .after = ls_option1
)
# B0092138 B0078239

select(
  linkAddress,
  DivisionName = dv_dv_name,
  DepartmentName = dp_dp_name,
  linkCity,
  BuildingId,
  Lease = ls_ls_id,
  ls_ls_parent_id,
  Tenure,
  #ContractType
  LeaseStatus = ls_status,
  #AgreementStatus,
  LeaseVersion = ls_ls_version,
  AgreementDateStart = ls_date_start,
  AgreementDateEnd = ls_date_end,
  LeaseDateEnd = FacilityType
) |>
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
