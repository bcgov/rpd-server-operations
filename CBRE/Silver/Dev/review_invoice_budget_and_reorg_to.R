library(dplyr)
library(here)
library(openxlsx2)
library(DBI)
library(odbc)

invoice <- read.csv(here::here("input/EdpTables/Invoice.csv"))

budget <- openxlsx2::read_xlsx(here::here("input/EdpTables/Budget.xlsx"))

reorg_to <- openxlsx2::read_xlsx(here::here("input/EdpTables/Reorg_to.xlsx"))

ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "CbreSilver"
TABLE_NAME <- "archibus_reorg_to"
CBRE_TABLE_NAME <- "archibus_reorg_to"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
API_NAME <- "CBRE"
SCRIPT_NAME <- if (ETL_STATUS == "PROD") {
  paste0("PROD-", TABLE_NAME)
} else {
  paste0("TEST-", TABLE_NAME)
}

# Connect to SQL database
con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = SQL_SERVER,
  database = DB_NAME,
  Trusted_Connection = "Yes"
)


dbWriteTable(conn = con, name = TARGET_TABLE, value = reorg_to)
