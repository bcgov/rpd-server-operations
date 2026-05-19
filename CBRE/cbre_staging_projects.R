# orchestrator_staging.R
# Sourced by Task Scheduler via Rscript.exe
# Runs all CBRE staging scripts, continues on error
source(here::here("renv/activate.R"))
source(here::here("utilities/R/event_logger.R"))

ORCHESTRATOR_NAME <- "cbre_staging_projects"

scripts <- c(
  "CBRE/Staging/dim_project_activity.R",
  "CBRE/Staging/fact_project_activity.R"
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
