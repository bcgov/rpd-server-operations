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
library(stringr, quietly = TRUE, warn.conflicts = FALSE)
library(openxlsx2, quietly = TRUE, warn.conflicts = FALSE)
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
SCHEMA_NAME <- "RealProperty"
TABLE_NAME <- "PORT_WorkOrders"
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
SCRIPT_NAME <- "PORT_WorkOrders"
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
query <- dbSendQuery(con, "SELECT * FROM CbreStaging.fm_fact_workorder")
WorkOrderData <- dbFetch(query, n = -1)
dbClearResult(query)
