# orchestrator_staging.R
# Sourced by Task Scheduler via Rscript.exe
# Runs all CBRE staging scripts, continues on error
source(here::here("renv/activate.R"))
source(here::here("utilities/R/utilities.R"))

orchestrator_start <- Sys.time()
ORCHESTRATOR_NAME <- "cbre_staging"

etl_window <- get_etl_window()

scripts <- c(
  # Drop and refresh
  "CBRE/Staging/archibus_bl.R",
  "CBRE/Staging/archibus_property.R",
  "CBRE/Staging/archibus_rm.R",
  "CBRE/Staging/archibus_rmpct.R",
  "CBRE/Staging/archibus_ls.R",
  "CBRE/Staging/archibus_dv.R",
  "CBRE/Staging/archibus_dp.R",
  # Uses etl_window
  "CBRE/Staging/dim_budget.R",
  "CBRE/Staging/dim_contact.R",
  "CBRE/Staging/dim_project_activity.R",
  "CBRE/Staging/dim_project_role.R",
  "CBRE/Staging/dim_project.R",
  "CBRE/Staging/fact_budget.R",
  "CBRE/Staging/fact_project_activity.R",
  "CBRE/Staging/fact_project_role.R",
  "CBRE/Staging/fact_project.R",
  "CBRE/Staging/fm_fact_workorder.R",
  "CBRE/Staging/fm_benchmark_dim_asset.R",
  "CBRE/Staging/fm_benchmark_property_asset_link.R",
  "CBRE/Staging/pjm_dim_budget.R",
  "CBRE/Staging/pjm_dim_contact.R",
  "CBRE/Staging/pjm_dim_project_activity.R",
  "CBRE/Staging/pjm_dim_project_role.R",
  "CBRE/Staging/pjm_dim_project.R",
  "CBRE/Staging/pjm_fact_budget.R",
  "CBRE/Staging/pjm_fact_project_role.R",
  "CBRE/Staging/pjm_fact_project_activity.R",
  "CBRE/Staging/pjm_fact_project.R",
  # Claude Reviewed up to here
  "CBRE/Staging/pjm_dim_invoice.R",
  "CBRE/Staging/pjm_fact_invoice.R"
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

n_success <- sum(sapply(results, \(r) r$status == "SUCCESS"))
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
