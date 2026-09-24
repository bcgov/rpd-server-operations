# event_logger.R (for daily recording of API queries)
# Columns: timestamp, api, subset, event_type, description
# Registry of (api, subset) pairs for auditing

# --- Configuration defaults (update here if needed) -------------------------
.default_log_dir <- "E:/Projects/server-logs"
.default_registry_file <- "registry.csv"

# --- Public API: event_logger -----------------------------------------------
event_logger <- function(
  api, # e.g., "Jira", "CBRE"
  subset, # e.g., "PAR", "SBP", "RBAS" or CBRE project name
  event_type, # e.g., "success" or "error"
  description, # free text; normalized to one line
  task_time = NA, # elapsed time on the task
  n_updated = NA, # where applicable, SQL rows updated
  n_inserted = NA, # where applicable, SQL rows updated
  subdirectory,
  log_dir = .default_log_dir,
  registry_file = .default_registry_file,
  tz = Sys.timezone()
) {
  # Ensure log directory exists
  dir.create(
    paste0(log_dir, "/", subdirectory),
    showWarnings = FALSE,
    recursive = TRUE
  )

  # Normalize fields
  ts_str <- format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z", tz = tz)
  event_type <- tolower(trimws(event_type)) # stable filter values
  api <- trimws(api) # open-ended labels
  subset <- trimws(subset) # open-ended labels
  description <- gsub("[\r\n]+", " ", description)

  # Auto-register labels (non-blocking audit trail)
  .register_label(
    api,
    subset,
    subdirectory = subdirectory,
    log_dir = log_dir,
    registry_file = registry_file
  )

  # Daily file name
  target_file <- file.path(
    # log_dir,
    paste0(log_dir, "/", subdirectory),
    # sprintf("server-log_%s.csv", as.Date(Sys.time(), tz = tz))
    paste0(subdirectory, "_", api, "_", subset, ".csv")
  )

  # Build one-row data frame
  entry <- data.frame(
    timestamp = ts_str,
    api = api,
    subset = subset,
    event_type = event_type,
    n_updated = n_updated,
    n_inserted = n_inserted,
    description = description,
    task_time = task_time,
    stringsAsFactors = FALSE
  )

  # Append row; write header if file doesn't exist yet
  utils::write.table(
    entry,
    file = target_file,
    sep = ",",
    row.names = FALSE,
    col.names = !file.exists(target_file),
    append = TRUE,
    quote = TRUE # quote strings so commas inside description don't break CSV
  )

  invisible(target_file)
}

# --- Helpers: registry & readers -------------------------------------------

# Append unseen (api, subset) pairs into a registry CSV for auditing.
.register_label <- function(
  api,
  subset,
  log_dir = .default_log_dir,
  subdirectory = subdirectory,
  registry_file = .default_registry_file
) {
  reg_path <- file.path(log_dir, registry_file)
  dir.create(dirname(reg_path), showWarnings = FALSE, recursive = TRUE)

  new_row <- data.frame(api = api, subset = subset, stringsAsFactors = FALSE)

  if (!file.exists(reg_path)) {
    utils::write.table(
      new_row,
      reg_path,
      sep = ",",
      row.names = FALSE,
      col.names = TRUE,
      append = FALSE,
      quote = TRUE
    )
    return(invisible(TRUE))
  }

  # Read current registry; append only if unseen combination
  existing <- tryCatch(
    utils::read.csv(reg_path, stringsAsFactors = FALSE),
    error = function(...) NULL
  )

  if (
    is.null(existing) ||
      !any(
        existing$api == api &
          existing$subset == subset &
          subdirectory == "Status"
      )
  ) {
    utils::write.table(
      new_row,
      reg_path,
      sep = ",",
      row.names = FALSE,
      col.names = FALSE,
      append = TRUE,
      quote = TRUE
    )
  }

  invisible(TRUE)
}

# New Logger ####
log_daily_etl_run <- function(
  api_name,
  script_name,
  table_name = NA_character_,
  status,
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
    run_timestamp = as.POSIXct(Sys.time()),
    etl_env = etl_env,
    host = Sys.info()[["nodename"]],
    api_name = api_name,
    script_name = script_name,
    table_name = table_name,
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
