# Load helper functions
source(here::here("utilities/R/utilities.R"))

# Set options
options(scipen = 999)

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
TABLE_NAME <- "EmployeeRef"
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
SCRIPT_NAME <- "EmployeeRef"
API_NAME <- "None"

options(scipen = 999)
options(digits = 7)

# Connect to SQL database
con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = SQL_SERVER,
  database = DB_NAME,
  Trusted_Connection = "Yes"
)

# Query PSA API ####
# Load necessary packages
library(base64enc, quietly = TRUE, warn.conflicts = FALSE)
library(dplyr, quietly = TRUE, warn.conflicts = FALSE)
library(httr2, quietly = TRUE, warn.conflicts = FALSE)
library(jsonlite, quietly = TRUE, warn.conflicts = FALSE)
library(lubridate, quietly = TRUE, warn.conflicts = FALSE)
library(purrr, quietly = TRUE, warn.conflicts = FALSE)
library(tibble, quietly = TRUE, warn.conflicts = FALSE)
library(tidyr, quietly = TRUE, warn.conflicts = FALSE)

username <- "bccitz"

api_key <- keyring::key_get(
  service = "PSA_API",
  username = username
)

# Setup API parameters ####
# Headcount: "https://analytics-api.psa.gov.bc.ca/apiserver/api.rsc/Datamart_CITZ_Report_usp_SHR_010_ORG/"
# Establishment Report: "https://analytics-api.psa.gov.bc.ca/apiserver/api.rsc/Datamart_CITZ_Report_usp_SO_001_ORG/"
# Telework: "https://analytics-api.psa.gov.bc.ca/apiserver/api.rsc/Datamart_CITZ_Report_usp_Telework/"

query_url = "https://analytics-api.psa.gov.bc.ca/apiserver/api.rsc/Datamart_CITZ_Report_usp_SO_001_ORG/"
# query_url = "https://analytics-api.psa.gov.bc.ca/apiserver/api.rsc/Datamart_CITZ_Report_usp_SHR_010_ORG/"

req <- request(query_url) |>
  req_auth_basic(username, api_key) |>
  req_method("POST") |>
  req_headers(Accept = "application/json") |>
  # req_body_form(FilterDate = Sys.Date()) |>
  req_body_form(Organization = "Infrastructure") |>
  req_perform()

resp <- req |> resp_body_json()

Establishment <- resp |>
  purrr::pluck("value") |>
  tibble::enframe() |>
  tidyr::unnest_wider(value, names_sep = "_") |>
  select(-c(name)) |>
  rename_with(~ gsub("value_", "", .), everything()) |>
  mutate(across(where(is.character), ~ na_if(.x, ""))) |>
  filter(is.na(empty)) |>
  select(
    name,
    emplid,
    level1,
    level2,
    level3,
    level4,
    title,
    pos_classification,
    supervisor_name,
    direct,
    indirect
  )

# Query SQL Datasets ####
query <- dbSendQuery(
  con,
  "
  SELECT
      contact_skey,
      contact_id,
      job_title,
      department_name,
      company_name,
      first_name,
      last_name,
      email_id
  FROM CbreStaging.dim_contact
  WHERE email_id LIKE '%@gov.bc.ca'
  "
)
DimContactData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(
  con,
  "
  SELECT
    FirstName,
    LastName,
    EmailAddress,
    EmployeeId,
    Company,
    Department,
    Office,
    JobTitle
  FROM RealProperty.GlobalAddressLookup
  WHERE Company LIKE '%Infrastructure%'"
)

# Can we get IDIR from GAL?
query <- dbSendQuery(con, "SELECT * FROM RealProperty.GlobalAddressLookup")
GalData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.pjm_fact_project_role")
FactProjRoleData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.pjm_dim_project")
DimProjData <- dbFetch(query, n = -1)
dbClearResult(query)

# Assemble Report ####
EmployeeRef <- GalData |>
  left_join(Establishment, by = join_by(EmployeeId == emplid)) |>
  right_join(DimContactData, by = join_by(EmailAddress == email_id))
