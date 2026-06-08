# orchestrator_staging.R
# Sourced by Task Scheduler via Rscript.exe
# Runs all CBRE staging scripts, continues on error
source(here::here("renv/activate.R"))
source(here::here("utilities/R/utilities.R"))

orchestrator_start <- Sys.time()
ORCHESTRATOR_NAME <- "jira_boards"

message <- "script_started"
log_daily_etl_run(
  api_name = ORCHESTRATOR_NAME,
  script_name = ORCHESTRATOR_NAME,
  status = message,
  message = substr(rollup_message, 1, 500)
)

scripts <- c(
  "JIRA/Boards/JIRA-TEST-CSR-Issues.R",
  "JIRA/Boards/JIRA-TEST-GPOPR-Issues.R",
  "JIRA/Boards/JIRA-TEST-PAR-Issues.R",
  "JIRA/Boards/JIRA-TEST-PSO-Issues.R",
  "JIRA/Boards/JIRA-TEST-RBAS-Issues.R",
  "JIRA/Boards/JIRA-TEST-SBP-Issues.R",
  "JIRA/Boards/JIRA-TEST-SBPSB-Issues.R"
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
