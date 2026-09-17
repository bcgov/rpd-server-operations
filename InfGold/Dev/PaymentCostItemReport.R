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
TABLE_NAME <- "PaymentCostItemReport"
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
SCRIPT_NAME <- "PaymentCostItemReport"
API_NAME <- "None"

# Connect to SQL database
con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = SQL_SERVER,
  database = DB_NAME,
  Trusted_Connection = "Yes"
)
# K1005164 - 9183324 -

query <- dbSendQuery(
  con,
  "SELECT *
  FROM CbreStaging.pjm_dim_project"
)
DimProjectData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(
  con,
  "SELECT *
  FROM CbreStaging.pjm_fact_project"
)
FactProjectData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(
  con,
  "SELECT *
  FROM CbreStaging.pjm_dim_invoice"
)
DimInvoiceData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(
  con,
  "SELECT *
  FROM CbreStaging.pjm_fact_invoice"
)
FactInvoiceData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(
  con,
  "SELECT *
  FROM CbreStaging.pjm_dim_project_activity"
)
DimProjectActivityData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(
  con,
  "SELECT *
  FROM CbreStaging.pjm_fact_project_activity"
)
FactProjectActivityData <- dbFetch(query, n = -1)
dbClearResult(query)
