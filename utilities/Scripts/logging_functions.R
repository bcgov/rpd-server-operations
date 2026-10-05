#' Handles the logging of error, warning, and condition messages during ETL
#'
#' @param api_name A character vector naming the source API of incoming Bronze-bound records
#' @param script_name A character vector naming the ETL script
#' @param condition Whether the log being recorded is an error, warning, or condition
#' @param table_name The table name that the error is occurring for
#' @param step A character vector naming the step that the condition is occurring in
#' @param severity The severity level
#' @param etl_env The ETL Environment it is occurring in
#' @return invisible tibble of logging data
log_etl_error <- function(
  api_name,
  script_name,
  condition,
  table_name = NA_character_,
  step = NA_character_,
  severity = "ERROR",
  etl_env = Sys.getenv("ETL_ENV", unset = "UNKNOWN")
) {
  log_dir <- here::here("logs", "error")
  if (!dir.exists(log_dir)) {
    dir.create(log_dir, recursive = TRUE)
  }

  log_file <- file.path(log_dir, paste0("etl_error_log_", Sys.Date(), ".csv"))

  log_row <- tibble::tibble(
    run_timestamp = format(
      lubridate::with_tz(Sys.time(), tzone = "America/Vancouver"),
      "%Y-%m-%d %H:%M:%S"
    ),
    etl_env = etl_env,
    host = Sys.info()[["nodename"]],
    api_name = api_name,
    script_name = script_name,
    table_name = table_name,
    step = step,
    severity = severity,
    condition_class = paste(class(condition), collapse = "/"),
    message = conditionMessage(condition)
  )

  if (!file.exists(log_file)) {
    readr::write_csv(log_row, log_file)
  } else {
    readr::write_csv(log_row, log_file, append = TRUE)
  }

  invisible(log_row)
}

#' Handles the daily logging of ETL script execution
#'
#' @param api_name A character vector naming the source API of incoming Bronze-bound records
#' @param script_name A character vector naming the ETL script
#' @param table_name The table name that the log is for
#' @param status The outcome of the table load
#' @param duration The time taken to execute the script
#' @param n_inserted The number of rows inserted into the db
#' @param n_updated TThe number of rows updated in the db
#' @param n_deleted The number of rows deleted the db
#' @param message The message describing the outcome
#' @param etl_env The ETL Environment it is occurring in
#' @return invisible tibble of logging data
log_daily_etl_run <- function(
  api_name,
  script_name,
  table_name = NA_character_,
  status,
  duration = NA_integer_,
  n_inserted = NA_integer_,
  n_updated = NA_integer_,
  n_deleted = NA_integer_,
  message = NA_character_,
  etl_env = Sys.getenv("ETL_ENV", unset = "UNKNOWN")
) {
  # Resolve repo root & log directory
  log_dir <- here::here("logs")
  if (!dir.exists(log_dir)) {
    dir.create(log_dir, recursive = TRUE)
  }

  # Daily log file (overwritten each day)
  log_file <- file.path(
    log_dir,
    paste0("daily_etl_log_", Sys.Date(), ".csv")
  )

  # Construct log row
  log_row <- tibble::tibble(
    run_timestamp = format(
      lubridate::with_tz(Sys.time(), tzone = "America/Vancouver"),
      "%Y-%m-%d %H:%M:%S"
    ),
    etl_env = etl_env,
    host = Sys.info()[["nodename"]],
    api_name = api_name,
    script_name = script_name,
    table_name = table_name,
    duration = duration,
    status = status,
    n_inserted = n_inserted,
    n_updated = n_updated,
    n_deleted = n_deleted,
    message = message
  )

  # Write or append (single-writer assumption on Muon)
  if (!file.exists(log_file)) {
    readr::write_csv(log_row, log_file)
  } else {
    readr::write_csv(log_row, log_file, append = TRUE)
  }

  invisible(log_row)
}

#' Handles the daily logging of ETL script execution
#'
#' @param orchestrator_name A character vector naming the source API of incoming Bronze-bound records
#' @param status The outcome of the table load
#' @param duration The time taken to execute the script
#' @param n_success The number of scripts that succeeded
#' @param n_error TThe number of scripts with errors
#' @param n_no_data The number of scripts with no data
#' @param failed_scripts The collection of failed scripts
#' @param message The output message
#' @param etl_env The ETL Environment it is occurring in
#' @return invisible tibble of logging data
log_daily_etl_script <- function(
  orchestrator_name,
  status,
  duration,
  n_success,
  n_error,
  n_no_data = NA_integer_,
  failed_scripts = NA_character_,
  message = NA_character_,
  etl_env = Sys.getenv("ETL_ENV", unset = "UNKNOWN")
) {
  log_dir <- here::here("logs", "orchestrator")
  if (!dir.exists(log_dir)) {
    dir.create(log_dir, recursive = TRUE)
  }

  log_file <- file.path(
    log_dir,
    paste0("orchestrator_log_", Sys.Date(), ".csv")
  )

  log_row <- tibble::tibble(
    run_timestamp = format(
      lubridate::with_tz(Sys.time(), tzone = "America/Vancouver"),
      "%Y-%m-%d %H:%M:%S"
    ),
    etl_env = etl_env,
    host = Sys.info()[["nodename"]],
    orchestrator_name = orchestrator_name,
    status = status,
    duration = duration,
    n_success = n_success,
    n_error = n_error,
    n_no_data = n_no_data,
    failed_scripts = failed_scripts,
    message = message
  )

  if (!file.exists(log_file)) {
    readr::write_csv(log_row, log_file)
  } else {
    readr::write_csv(log_row, log_file, append = TRUE)
  }

  invisible(log_row)
}
