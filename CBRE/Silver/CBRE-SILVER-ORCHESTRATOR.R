# orchestrator_staging.R
# Sourced by Task Scheduler via Rscript.exe
# Runs all CBRE staging scripts, continues on error
source(here::here("renv/activate.R"))

# Load helper functions
source(here::here("utilities/utilities.R"))

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
orchestrator_start <- Sys.time()
ORCHESTRATOR_NAME <- "CBRE-SILVER-ORCHESTRATOR"

etl_window <- get_etl_window()

scripts <- c(
  # Drop and refresh
  "CBRE/Silver/Scripts/archibus_bl.R",
  "CBRE/Silver/Scripts/archibus_budget_asset.R",
  "CBRE/Silver/Scripts/archibus_budget_asset_ar.R",
  "CBRE/Silver/Scripts/archibus_company.R",
  "CBRE/Silver/Scripts/archibus_cost_tran_recur.R",
  "CBRE/Silver/Scripts/archibus_property.R",
  "CBRE/Silver/Scripts/archibus_rm.R",
  "CBRE/Silver/Scripts/archibus_rmpct.R",
  "CBRE/Silver/Scripts/archibus_ls.R",
  "CBRE/Silver/Scripts/archibus_dv.R",
  "CBRE/Silver/Scripts/archibus_dp.R",
  "CBRE/Silver/Scripts/pjm_report_project_role.R",
  # Uses etl_window
  "CBRE/Silver/Scripts/com_dim_property.R",
  "CBRE/Silver/Scripts/dim_budget.R",
  "CBRE/Silver/Scripts/dim_contact.R",
  "CBRE/Silver/Scripts/dim_project_activity.R",
  "CBRE/Silver/Scripts/dim_project_role.R",
  "CBRE/Silver/Scripts/dim_project.R",
  "CBRE/Silver/Scripts/dim_property.R",
  "CBRE/Silver/Scripts/fact_budget.R",
  "CBRE/Silver/Scripts/fact_project_activity.R",
  # skipping this section
  # "CBRE/Silver/Scripts/fact_project_role.R",
  # "CBRE/Silver/Scripts/fact_project.R",
  # "CBRE/Silver/Scripts/fm_benchmark_dim_asset.R",
  # "CBRE/Silver/Scripts/fm_benchmark_property_asset_link.R",
  # "CBRE/Silver/Scripts/fm_dim_property_extended_attribute.R",
  # only partially completed fm_fact_workorder
  "CBRE/Silver/Scripts/fm_fact_workorder.R",
  "CBRE/Silver/Scripts/kahua_cashflow.R",
  "CBRE/Silver/Scripts/kahua_milestones.R",
  "CBRE/Silver/Scripts/kahua_project_role.R",
  "CBRE/Silver/Scripts/pjm_dim_budget.R",
  "CBRE/Silver/Scripts/pjm_dim_contact.R",
  "CBRE/Silver/Scripts/pjm_dim_project_activity.R",
  "CBRE/Silver/Scripts/pjm_dim_project_role.R",
  "CBRE/Silver/Scripts/pjm_dim_project.R",
  "CBRE/Silver/Scripts/pjm_fact_budget.R",
  "CBRE/Silver/Scripts/pjm_fact_milestone.R",
  "CBRE/Silver/Scripts/pjm_fact_project_role.R",
  "CBRE/Silver/Scripts/pjm_fact_project_activity.R",
  "CBRE/Silver/Scripts/pjm_fact_project.R",
  # Claude Reviewed up to here
  "CBRE/Silver/Scripts/pjm_dim_invoice.R",
  "CBRE/Silver/Scripts/pjm_fact_invoice.R"
  #   fin_dim_general_ledger
  # fin_fact_general_ledger_actuals
  #   fin_dim_invoice_line
  #  fin_dim_invoice
  # fin_dim_cost_center_hierarchy
  # fin_fact_cost_distribution_detail
  # fact_invoice # update skey datatype and refresh to current
  # dim_invoice
)

# -- Per-script result tracking --
results <- vector("list", length(scripts))
names(results) <- scripts

for (script in scripts) {
  script_start <- Sys.time()
  script_path <- here::here(script)

  tryCatch(
    {
      source(script_path)
      results[[script]] <- list(
        status = "SUCCESS",
        duration = as.numeric(difftime(
          Sys.time(),
          script_start,
          units = "secs"
        ))
      )
    },
    no_data_condition = function(e) {
      results[[script]] <<- list(
        status = "NO_DATA",
        duration = as.numeric(difftime(
          Sys.time(),
          script_start,
          units = "secs"
        )),
        message = conditionMessage(e)
      )
    },
    error = function(e) {
      results[[script]] <<- list(
        status = "ERROR",
        duration = as.numeric(difftime(
          Sys.time(),
          script_start,
          units = "secs"
        )),
        message = conditionMessage(e)
      )
    }
  )
}

# -- Rollup --
orchestrator_duration <- as.numeric(
  difftime(Sys.time(), orchestrator_start, units = "secs")
)

n_success <- sum(sapply(results, \(r) r$status %in% c("SUCCESS", "NO_DATA")))
n_error <- sum(sapply(results, \(r) r$status == "ERROR"))
overall_status <- if (n_error == 0) "SUCCESS" else "PARTIAL_FAILURE"

failed_scripts <- names(Filter(\(r) r$status == "ERROR", results))
rollup_message <- if (n_error == 0) {
  paste0(
    n_success,
    " script(s) succeeded in ",
    round(orchestrator_duration, 1),
    "s"
  )
} else {
  paste0(
    n_success,
    " succeeded, ",
    n_error,
    " failed in ",
    round(orchestrator_duration, 1),
    "s. Failed: ",
    paste(failed_scripts, collapse = "; ")
  )
}

log_daily_etl_run(
  api_name = ORCHESTRATOR_NAME,
  script_name = ORCHESTRATOR_NAME,
  status = overall_status,
  message = substr(rollup_message, 1, 500)
)
