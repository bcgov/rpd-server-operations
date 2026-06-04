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

options(scipen = 999)

# Connect to SQL database
con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = SQL_SERVER,
  database = DB_NAME,
  Trusted_Connection = "Yes"
)

# PCIReport <- openxlsx2::read_xlsx(here(
#   "input/KahuaPayable/2026-05-25-PCIReport.xlsx"
# ))

KahuaSource <- openxlsx2::read_xlsx(here(
  "input/KahuaPayable/2026-05-21-CompareKahua.xlsx"
))

query <- dbSendQuery(
  con,
  "SELECT project_skey, project_number, project_name FROM CbreStaging.pjm_dim_project"
)
PjmDimProjectData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(
  con,
  "SELECT
    invoice_skey,
    invoice_id,
    invoice_approval_status,
    payment_date,
    period_from,
    period_to,
    check_number,
    vendor_invoice_number
  FROM CbreStaging.pjm_dim_invoice"
)
PjmDimInvoiceData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(
  con,
  "SELECT * FROM CbreStaging.pjm_fact_invoice"
)
PjmFactInvoiceData <- dbFetch(query, n = -1)
dbClearResult(query)

LinkReport <- KahuaSource |>
  rename_with(~ gsub("\\s*/*\\n*", "", .)) |>
  left_join(
    PjmDimProjectData,
    by = join_by(KahuaProjectNumber == project_number)
  ) |>
  left_join(
    PjmDimInvoiceData,
    by = join_by(InvoiceNumber == vendor_invoice_number, LineID == invoice_id)
  )

test <- LinkReport |>
  filter(!is.na(invoice_skey))

subset <- LinkReport |>
  filter(InvoiceNumber == "804219") |>
  relocate(InvoiceNumber, .before = LineID)

invoice_set <- PjmDimInvoiceData |>
  filter(vendor_invoice_number == "804219") |>
  relocate(vendor_invoice_number, .before = invoice_id)
