# orchestrator_staging.R
# Sourced by Task Scheduler via Rscript.exe
# Runs all CBRE staging scripts, continues on error
source(here::here("renv/activate.R"))
source(here::here("utilities/R/event_logger.R"))

ORCHESTRATOR_NAME <- "cbre_staging"

scripts <- c(
  "CBRE/Staging/archibus_bl.R",
  "CBRE/Staging/archibus_property.R",
  "CBRE/Staging/archibus_rm.R",
  "CBRE/Staging/archibus_rmpct.R",
  "CBRE/Staging/archibus_ls.R",
  "CBRE/Staging/archibus_dv.R",
  "CBRE/Staging/archibus_dp.R"
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
