# orchestrator_staging.R
# Sourced by Task Scheduler via Rscript.exe
# Runs all CBRE staging scripts, continues on error
source(here::here("renv/activate.R"))
source(here::here("utilities/R/event_logger.R"))
source(here::here("utilities/R/api_helpers.R"))
source(here::here("utilities/R/sql_helper_functions.R"))

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
  "CBRE/Staging/pjm_dim_budget.R",
  "CBRE/Staging/pjm_dim_contact.R",
  "CBRE/Staging/pjm_dim_project_activity.R",
  "CBRE/Staging/pjm_dim_project_role.R",
  "CBRE/Staging/pjm_dim_project.R",
  "CBRE/Staging/pjm_fact_budget.R",
  "CBRE/Staging/pjm_fact_project_role.R",
  "CBRE/Staging/pjm_fact_project_activity.R",
  "CBRE/Staging/pjm_fact_project.R"
  # Claude Reviewed up to here
  #   fin_dim_general_ledger
  # fin_fact_general_ledger_actuals
  #   fin_dim_invoice_line
  #  fin_dim_invoice
  # fin_dim_cost_center_hierarchy
  # fin_fact_cost_distribution_detail
  # fact_invoice # update skey datatype and refresh to current
  # dim_invoice
)

for (script in scripts) {
  script_path <- here::here(script)

  tryCatch(
    {
      source(script_path)

      log_daily_etl_run(
        api_name = ORCHESTRATOR_NAME,
        script_name = script,
        status = "SUCCESS"
      )
    },
    error = function(e) {
      log_daily_etl_run(
        api_name = ORCHESTRATOR_NAME,
        script_name = script,
        status = "ERROR",
        message = conditionMessage(e)
      )
    }
  )
}
