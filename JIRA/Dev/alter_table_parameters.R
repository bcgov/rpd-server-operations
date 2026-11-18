library(odbc, quietly = TRUE, warn.conflicts = FALSE)
library(DBI, quietly = TRUE, warn.conflicts = FALSE)

etl_status <- "PROD"
sql_server <- if (etl_status == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
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

sql <- glue::glue(
  "ALTER TABLE {schema_name}.PSO
   ALTER COLUMN KahuaNumber VARCHAR(250) NULL;"
)

dbExecute(con, sql)
