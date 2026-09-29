#' Apply the hash gate: write qualifying rows to Bronze, log every row
#'
#' @param classified Output of classify_incoming()
#' @param con DBI connection
#' @param bronze_table Bronze table name
#' @param audit_table Audit log table name
#' @param source_table_name Label for this source, stored in the audit log
#' @param batch_id Identifier for this run (e.g. from your existing batch/run ID scheme)
#' @return invisible list with counts, for logging via log_daily_etl_run()
apply_hash_gate <- function(
  con,
  classified,
  primary_key,
  bronze_table,
  audit_table,
  source_system,
  source_table_name,
  batch_id,
  start_time
) {
  # --- Bronze: only NEW / CHANGED rows get a full new version written
  to_write <- classified |>
    filter(action %in% c("NEW", "CHANGED")) |>
    select(-c(action))

  if (nrow(to_write) > 0) {
    DBI::dbAppendTable(con, bronze_table, to_write)
    cat("ETL complete — written:", nrow(to_write), " rows.", "\n")
  }

  possible_actions <- c("NEW", "UNCHANGED", "CHANGED")

  counts <- count(classified, action) |>
    tidyr::pivot_wider(names_from = action, values_from = n, values_fill = 0) |>
    ensure_columns(possible_actions, value = 0) |>
    rename_with(.fn = stringr::str_to_title, .cols = everything()) |>
    relocate(New, Changed, Unchanged, .before = everything())

  log_row <- counts |>
    mutate(
      load_ts = as.POSIXct(start_time, tz = "UTC"),
      batch_id = batch_id,
      source_system = source_system,
      source_table = source_table_name,
      .before = everything()
    )

  DBI::dbAppendTable(con, audit_table, log_row)

  cat(
    "ETL complete — Audit Row Written:",
    log_row$New,
    " new, ",
    log_row$Changed,
    " changed, and ",
    log_row$Unchanged,
    " unchanged.",
    "\n"
  )

  # invisible(list(
  #   batch_id = batch_id,
  #   load_ts = load_ts,
  #   new = counts$NEW %||% 0,
  #   changed = counts$CHANGED %||% 0,
  #   unchanged = counts$UNCHANGED %||% 0,
  #   total = nrow(classified)
  # ))
}

# `%||%` <- function(x, y) if (is.null(x) || length(x) == 0) y else x
