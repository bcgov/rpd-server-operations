library(odbc, quietly = TRUE, warn.conflicts = FALSE)
library(DBI, quietly = TRUE, warn.conflicts = FALSE)

sql_server <- "dynamo.idir.bcgov\\CA_PRD"
db_name <- "BuildingIntelligence"
schema_name <- "Jira"

# Connect to SQL database
con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = sql_server,
  database = db_name,
  Trusted_Connection = "Yes"
)

dbExecute(con, "CREATE SCHEMA JIRA")
