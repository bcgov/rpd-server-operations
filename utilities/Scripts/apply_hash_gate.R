#' Apply the hash gate: write qualifying rows to Bronze, log every row
#'
#' @param classified Output of classify_incoming()
#' @param con DBI connection
#' @param bronze_table Bronze table name
#' @param source_table_name Label for this source, stored in the audit log
#' @param batch_id Identifier for this run (e.g. from your existing batch/run ID scheme)
#' @return invisible list with counts, for logging via log_daily_etl_run()
apply_hash_gate <- function(
  con,
  classified,
  primary_key,
  bronze_table,
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

  return(
    log_row
  )
}
