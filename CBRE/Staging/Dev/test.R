ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"

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

# Load helper functions
source(here::here("utilities/R/cbre_api_function.R"))
source(here::here("utilities/R/event_logger.R"))

# Connect to SQL database
con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = SQL_SERVER,
  database = DB_NAME,
  Trusted_Connection = "Yes"
)

# Entities of interest
pjm_fact_project_comment_vw
pjm_fact_project_risk_vw
pjm_fact_project_service_vw
pjm_fact_project_vw
pjm_kahua_h2_3_fullsnapshot
pjm_report_milestone_vw
pjm_report_project_details_vw
pjm_report_project_role_vw


dim_project_activity_vw <- extract_cbre_data(
  "dim_project_activity_vw",
  max_pages = 2
)
dim_project_role_vw <- extract_cbre_data("dim_project_role_vw", max_pages = 2)
dim_project_vw <- extract_cbre_data("dim_project_vw", max_pages = 2)
es_fact_allocation_by_reporing_group_vw <- extract_cbre_data(
  "es_fact_allocation_by_reporing_group_vw",
  max_pages = 2
)
fact_budget_vw <- extract_cbre_data("fact_budget_vw", max_pages = 2)
fact_contract_vw <- extract_cbre_data("fact_contract_vw", max_pages = 2)
fact_invoice_vw <- extract_cbre_data("fact_invoice_vw", max_pages = 2)
fact_milestone_vw <- extract_cbre_data("fact_milestone_vw", max_pages = 2)
fact_project_activity_vw <- extract_cbre_data(
  "fact_project_activity_vw",
  max_pages = 2
)
fact_project_risk_vw <- extract_cbre_data("fact_project_risk_vw", max_pages = 2)
fact_project_role_vw <- extract_cbre_data("fact_project_role_vw", max_pages = 2)
fact_project_vw <- extract_cbre_data("fact_project_vw", max_pages = 2)
fin_dim_general_ledger_vw <- extract_cbre_data(
  "fin_dim_general_ledger_vw",
  max_pages = 2
)
fin_fact_general_ledger_actuals_vw <- extract_cbre_data(
  "fin_fact_general_ledger_actuals_vw",
  max_pages = 2
)
fm_report_worker_role_vw <- extract_cbre_data(
  "fm_report_worker_role_vw",
  max_pages = 2
)


fetch_edp_csv(c)


query <- dbSendQuery(con, "SELECT * FROM CbreStaging.RoomAllocation")
RoomAllocation <- dbFetch(query, n = -1)
dbClearResult(query)

test <- RoomAllocation |>
  filter(RefreshDate == as.POSIXct("2026-04-23 14:51:11.039", tz = "UTC"))

test2 <- RoomAllocation |>
  filter(rmpct_bl_id == "B0092319")

row1 <- test2[13, ]
row2 <- test2[35, ]

join_test <- row1 |>
  left_join(
    row2,
    by = join_by(rmpct_bl_id, rmpct_fl_id, rmpct_rm_id, rmpct_ls_id)
  )
