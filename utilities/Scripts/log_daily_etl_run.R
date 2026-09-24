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
