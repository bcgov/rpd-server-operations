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
TABLE_NAME <- "PORT_ProjectCosts"
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
SCRIPT_NAME <- "PORT_ProjectCosts"
API_NAME <- "None"

# Connect to SQL database
con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = SQL_SERVER,
  database = DB_NAME,
  Trusted_Connection = "Yes"
)

# Query SQL Datasets
query <- dbSendQuery(
  con,
  "WITH DimProject AS (
   SELECT
   csf_wsiregion AS Region,
   project_number as KahuaProjectNumber,
   project_name,
   project_skey
   FROM CbreStaging.pjm_dim_project
   )
   SELECT
   fact.project_skey,
   fact.property_skey,
   dim.KahuaProjectNumber,
   dim.Region,
   dim.project_name
   FROM CbreStaging.pjm_fact_project fact
   LEFT JOIN DimProject dim
   ON dim.project_skey = fact.project_skey"
)
PjmProjectData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(
  con,
  "SELECT
    project_skey,
    project_activity_skey,
    record_type,
    paid
  FROM CbreStaging.pjm_fact_project_activity"
)
PjmFactProjectActivityData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(
  con,
  "SELECT
    Identifier,
    Name,
    Address,
    City,
    BuildingRentableArea,
    PropertyArea
  FROM RealProperty.FacilityDetail"
)
FacilityDetail <- dbFetch(query, n = -1)
dbClearResult(query)

ProjectCosts <- PjmProjectData |>
  left_join(PjmFactProjectActivityData, by = join_by(project_skey)) |>
  filter(record_type == "Summary") |>
  mutate(
    Identifier = stringr::str_extract(project_name, "B{1}\\d{7}|N{1}\\d{7}") # regex extraction is fragile
  ) |>
  group_by(project_skey, KahuaProjectNumber, Identifier, project_name) |>
  summarise(
    TotalPaid = max(paid, na.rm = TRUE),
    Region = first(Region, na_rm = TRUE),
    .groups = "drop_last"
  ) |>
  group_by(KahuaProjectNumber, Identifier) |>
  summarise(
    TotalPaid = max(TotalPaid, na.rm = TRUE),
    Region = first(Region, na_rm = TRUE),
    .groups = "drop_last"
  ) |> # clearing out duplicate project numbers
  filter(
    !is.na(Identifier) & !Identifier %in% c("9999100", "9999200", "9999300")
  ) |>
  group_by(Identifier) |>
  summarise(
    Region = first(Region, na_rm = TRUE),
    ProjectCount = n(),
    TotalPaid = sum(TotalPaid, na.rm = TRUE)
  ) |>
  ungroup() |>
  left_join(FacilityDetail, by = join_by(Identifier)) |>
  filter(!is.na(Address)) |>
  mutate(TotalPaid = round(TotalPaid, digits = 2)) |>
  mutate(
    CostPerSqM = case_when(
      startsWith(Identifier, "B") & BuildingRentableArea > 0 ~ round(
        TotalPaid /
          BuildingRentableArea,
        digits = 2
      ),
      .default = NA
    ),
    CostPerHA = case_when(
      startsWith(Identifier, "N") & PropertyArea > 0 ~ round(
        TotalPaid /
          PropertyArea,
        digits = 2
      ),
      .default = NA
    )
  ) |>
  relocate(
    Identifier,
    Name,
    Address,
    City,
    Region,
    ProjectCount,
    TotalPaid,
    BuildingRentableArea,
    PropertyArea,
    .before = everything()
  ) |>
  mutate(RefreshDate = Sys.time(), .before = everything())

# Create target table if it doesn't exist ####
# dbRemoveTable(con, TARGET_TABLE)
if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- paste0(
    "CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    TABLE_NAME,
    " (
      RefreshDate           DATETIME2(3)    NOT NULL,
      Identifier            NVARCHAR(50)    NOT NULL,
      Name                  NVARCHAR(255)   NULL,
      Address               NVARCHAR(255)   NULL,
      City                  NVARCHAR(100)   NULL,
      Region                NVARCHAR(100)   NULL,
      ProjectCount          INT             NULL,
      TotalPaid             DECIMAL(18,2)   NULL,
      BuildingRentableArea  DECIMAL(18,2)   NULL,
      PropertyArea          DECIMAL(18,4)   NULL,
      CostPerSqM            DECIMAL(18,2)   NULL,
      CostPerHA             DECIMAL(18,2)   NULL
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

    # Create temp table to hold new data
    dbExecute(
      con,
      sql <- paste0(
        "CREATE TABLE ",
        SCHEMA_NAME,
        ".",
        TEMP_TABLE,
        " (
        RefreshDate           DATETIME2(3)    NOT NULL,
        Identifier            NVARCHAR(50)    NOT NULL,
        Name                  NVARCHAR(255)   NULL,
        Address               NVARCHAR(255)   NULL,
        City                  NVARCHAR(100)   NULL,
        Region                NVARCHAR(100)   NULL,
        ProjectCount          INT             NULL,
        TotalPaid             DECIMAL(18,2)   NULL,
        BuildingRentableArea  DECIMAL(18,2)   NULL,
        PropertyArea          DECIMAL(18,4)   NULL,
        CostPerSqM            DECIMAL(18,2)   NULL,
        CostPerHA             DECIMAL(18,2)   NULL
      );"
      )
    )

    dbWriteTable(
      con,
      name = TEMP_TABLE,
      value = ProjectCosts,
      append = TRUE,
      overwrite = FALSE
    )

    dbExecute(
      con,
      paste0("DELETE FROM ", SCHEMA_NAME, ".", TABLE_NAME, ";")
    )

    n_inserted <- dbExecute(
      con,
      paste0(
        "INSERT INTO ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " (
      RefreshDate,
      Identifier,
      Name,
      Address,
      City,
      Region,
      ProjectCount,
      TotalPaid,
      BuildingRentableArea,
      PropertyArea,
      CostPerSqM,
      CostPerHA
    )
    SELECT
      RefreshDate,
      Identifier,
      Name,
      Address,
      City,
      Region,
      ProjectCount,
      TotalPaid,
      BuildingRentableArea,
      PropertyArea,
      CostPerSqM,
      CostPerHA
    FROM ",
        TEMP_TABLE,
        ";"
      )
    )

    dbCommit(con)
    n_inserted <<- n_inserted
  },
  error = function(e) {
    dbRollback(con)
    stop(e)
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
