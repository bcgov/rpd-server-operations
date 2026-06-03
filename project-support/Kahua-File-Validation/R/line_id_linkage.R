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

KahuaSource <- openxlsx2::read_xlsx(here(
  "input/KahuaPayable/2026-05-21-CompareKahua.xlsx"
))
PCIReport <- openxlsx2::read_xlsx(here(
  "input/KahuaPayable/2026-05-25-PCIReport.xlsx"
))

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.fact_invoice")
FactInvoiceData <- dbFetch(query, n = -1)
dbClearResult(query)

FactInvoiceData <- FactInvoiceData |>
  mutate(
    across(
      c(
        project_skey
      ),
      as.character
    )
  )

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.dim_invoice")
DimInvoiceData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.dim_project")
DimProjectData <- dbFetch(query, n = -1)
dbClearResult(query)

Report <- FactInvoiceData |>
  select(
    invoice_item_id,
    current_payment_due,
    project_skey,
    project_activity_skey,
    invoice_skey,
    invoice_item_skey,
    change_order_skey,
    change_order_item_skey,
    change_order_item_id,
    contract_skey,
    contract_line_skey,
    to_contact_skey,
    from_contact_skey,
    to_company_skey,
    from_company_skey
  ) |>
  left_join(DimInvoiceData, by = join_by(invoice_skey)) |>
  select(
    invoice_item_id,
    invoice_id,
    payment_date,
    current_payment_due,
    vendor_invoice_number,
    invoice_approval_status,
    project_skey,
    project_activity_skey,
    invoice_skey,
    invoice_item_skey,
    change_order_skey,
    change_order_item_skey,
    change_order_item_id,
    contract_skey,
    contract_line_skey,
    to_contact_skey,
    from_contact_skey,
    to_company_skey,
    from_company_skey
  ) |>
  left_join(DimProjectData, by = join_by(project_skey)) |>
  select(
    invoice_item_id,
    invoice_id,
    project_number,
    payment_date,
    current_payment_due,
    vendor_invoice_number,
    invoice_approval_status,
    csf_fundingsource,
    csf_fundingtype,
    csf_servicetype,
    standard_project_type,
    project_skey,
    project_activity_skey,
    invoice_skey,
    invoice_item_skey,
    change_order_skey,
    change_order_item_skey,
    change_order_item_id,
    contract_skey,
    contract_line_skey,
    to_contact_skey,
    from_contact_skey,
    to_company_skey,
    from_company_skey
  )
