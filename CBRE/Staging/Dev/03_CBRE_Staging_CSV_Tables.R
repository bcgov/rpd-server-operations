# Load libraries
library(base64enc)
library(dplyr)
library(httr2)
library(tidyr)
library(lubridate)
library(tools)
library(odbc)
library(DBI)

source("E:/Projects/citz-rpd-utilities/cbre_api_function.R")
source("E:/Projects/citz-rpd-utilities/event_logger.R")
source("E:/Projects/citz-rpd-utilities/helper_functions.R")

# Connect to SQL database
con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = "windfarm.idir.bcgov\\CA_TST",
  database = "BuildingIntelligence",
  Trusted_Connection = "Yes"
)

# fact_invoice_vw ####

edp_tables <- list(
  "dim_project_activity_vw",
  "dim_project_role_vw",
  "fact_budget_vw",
  "fact_invoice_vw",
  "fact_project_activity_vw"
)
# ---- submit all exports ----
# jobs <- lapply(edp_tables, function(tbl) {
#   submit_edp_export(
#     edp_table = tbl
#   )
# })

# ---- single delay (or none if polling suffices) ----
Sys.sleep(1200) # optional cushion

# ---- retrieve all results ----
results <- lapply(jobs, function(job) {
  retrieve_edp_export(job$file_id)
})


pjm_fact_invoice_full <- results |>
  purrr::pluck(1) |>
  select_if(~ !all(is.na(.))) |>
  select_if(~ !all(. == 0)) |>
  select_if(~ !all(. == '-1')) |>
  select_if(~ !all(. == "N/A")) |>
  select_if(~ !all(. == "-"))


pjmFactInvoice <- pjm_fact_invoice_full |>
  select(
    source_created_ts,
    source_modified_ts,
    project_skey,
    invoice_skey,
    invoice_item_id,
    record_type,
    invoice_desc,
    invoice_status,
    work_completed_this_period,
    work_completed_to_date,
    work_retainage,
    work_retainage_percent,
    payables_billed_total,
    payables_remitted_total,
    payables_withheld_total,
    payables_remaining_total,
    total_retainage,
    total_earned_to_date,
    total_to_date,
    total_to_date_percent,
    balance_to_finish,
    balance_to_finish_with_retainage,
    previous_work_completed,
    previous_total_earned,
    scheduled_value,
    current_payment_due
  ) |>
  mutate(
    across(
      c(
        source_created_ts,
        source_modified_ts
      ),
      as.POSIXct
    )
  ) |>
  mutate(
    across(
      c(
        work_completed_this_period,
        work_completed_to_date,
        work_retainage,
        work_retainage_percent,
        payables_billed_total,
        payables_remitted_total,
        payables_withheld_total,
        payables_remaining_total,
        total_retainage,
        total_earned_to_date,
        total_to_date,
        total_to_date_percent,
        balance_to_finish,
        balance_to_finish_with_retainage,
        previous_work_completed,
        previous_total_earned,
        scheduled_value,
        current_payment_due
      ),
      as.double
    )
  ) |>
  arrange(invoice_desc, desc(source_modified_ts)) |>
  filter(record_type == "invoice_item") |>
  mutate(
    MonthCreatedName = lubridate::month(source_created_ts, label = TRUE),
    MonthCreatedNumber = as.integer(lubridate::month(source_created_ts)),
    YearCreatedNumber = as.integer(lubridate::year(source_created_ts)),
    FiscalYear = get_fiscal_year(source_created_ts),
    .after = source_modified_ts
  )

dbWriteTable(
  conn = con,
  name = DBI::Id(schema = "CbreStaging", table = "PjmFactInvoice"),
  pjmFactInvoice,
  overwrite = TRUE
)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.pjmFactInvoice")
pjmFactInvoice <- dbFetch(query, n = -1)
dbClearResult(query)
# dim_budget_vw
# dim_contract_vw
# dim_project_activity_vw
# dim_project_vw
# dim_project_role_vw
# dm_refresh_ts
