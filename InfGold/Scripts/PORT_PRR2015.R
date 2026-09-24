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
TABLE_NAME <- "PORT_PRR2015"
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
SCRIPT_NAME <- "PORT_PRR2015"
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
query <- dbSendQuery(con, "SELECT * FROM RealProperty.PRR2015")
PRR2015 <- dbFetch(query, n = -1)
dbClearResult(query)

fiscFile <- list.files(
  here::here("input"),
  pattern = "PRR2015"
) |>
  sort(decreasing = TRUE)

PRR2015Extract <- read_xlsx(
  here(
    "input/",
    # fiscFile[1]
    "PRR2015_20260429.xlsx"
  ),
  start_row = 3
) |>
  rename_with(~ gsub(" ", "", .x)) |>
  pivot_longer(
    cols = matches("^([0-9]{4})Year"),
    names_to = "raw_name",
    values_to = "value"
  ) |>
  mutate(
    FiscalYear = str_extract(raw_name, "^([0-9]{4})"),
    metric = str_extract(raw_name, "(?<=^[0-9]{4}Year)(.*)"),
    .keep = "unused"
  ) |>
  pivot_wider(names_from = metric, values_from = value) |>
  relocate(`Variance%`, .after = everything()) |>
  mutate(across(
    RentableArea:`Variance%`,
    ~ as.double(gsub(
      "[^0-9.-]",
      "",
      .x
    ))
  )) |>
  rename(Identifier = PrimaryLocation) |>
  filter(FiscalYear == min(FiscalYear))

dbWriteTable(
  con,
  name = Id(SCHEMA_NAME, TABLE_NAME),
  value = PRR2015,
  append = FALSE,
  overwrite = TRUE
)

test <- PRR2015 |>
  mutate(
    cost_check_total = rowSums(across(BaseRent:Parking), na.rm = TRUE),
    admin_check_total = rowSums(across(LLAdminFee:UtilitiesAdmin), na.rm = TRUE)
  ) |>
  relocate(
    TotalCost,
    cost_check_total,
    TotalAdmin,
    admin_check_total,
    .before = everything()
  )
