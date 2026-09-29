# =============================================================================
# scd2_hash_gate.R
#
# Bronze-layer hash-gate for daily API pulls that change infrequently.
# Purpose:
#   - Compute an in-house row hash per natural key over "business" columns
#     only (excludes source-side pull metadata that changes every pull).
#   - Compare against the most recent hash on file for that key.
#   - Only write a new Bronze row when the hash differs (NEW or CHANGED).
#   - Always write a lightweight audit-log row, even on UNCHANGED days,
#     so there's a daily proof-of-check without paying for a full copy.
#   - Purge Bronze down to the last N versions per key (not a time window),
#     since some entities may go >30 days with no real change.
#
# Assumes: natural key = bl_bl_id_key (confirmed unique).
# Fits into the existing upsert-pattern / log_daily_etl_run() conventions.
# =============================================================================

library(dplyr)
library(purrr)
library(digest)
library(DBI)

# -----------------------------------------------------------------------------
# 1. Column configuration
# -----------------------------------------------------------------------------
# Columns that are ABOUT the pull/feed itself, not the building record.
# These are excluded from the hash so a metadata-only tick doesn't trigger
# a false "CHANGED" and a new Bronze row.
EXCLUDED_FROM_HASH <- c(
  "bl_bl_id_key", # natural key itself — not a "value" to hash
  "md5_hash", # partner-supplied hash — not used, and would
  # change even when nothing we care about does
  "edp_last_updated_timestamp",
  "edp_update_ts",
  "source_system",
  "source_account_name",
  "bl_source_time_update", # source-side update timestamp; can tick
  "bl_source_date_update" # without any tracked field actually changing
)

#' Get the set of columns to include in the row hash for a given data frame
#'
#' @param df A data frame of incoming Bronze-bound records
#' @return character vector of column names to hash, in stable (sorted) order
get_tracked_cols <- function(df) {
  sort(setdiff(names(df), EXCLUDED_FROM_HASH))
}

# -----------------------------------------------------------------------------
# 2. Row hashing
# -----------------------------------------------------------------------------
#' Add a row_hash column computed over tracked_cols only
#'
#' Sorting tracked_cols before hashing means column reordering upstream
#' never changes the hash — only actual value changes do.
#'
#' @param df Data frame with a bl_bl_id_key column plus business columns
#' @param tracked_cols character vector of columns to include in the hash;
#'   defaults to get_tracked_cols(df) if not supplied
#' @return df with a new row_hash (CHAR(32) / MD5 hex) column appended
add_row_hash <- function(df, tracked_cols = get_tracked_cols(df)) {
  stopifnot("bl_bl_id_key" %in% names(df))

  df |>
    rowwise() |>
    mutate(
      row_hash = digest::digest(
        pick(all_of(tracked_cols)),
        algo = "md5"
      )
    ) |>
    ungroup()
}

# -----------------------------------------------------------------------------
# 3. Compare incoming pull against last-known Bronze state
# -----------------------------------------------------------------------------
#' Classify each incoming record as NEW / CHANGED / UNCHANGED
#'
#' @param incoming Hashed incoming data (must have bl_bl_id_key, row_hash)
#' @param con DBI connection
#' @param bronze_table Name of the Bronze table (schema-qualified if needed)
#' @return incoming with an `action` column added: "NEW" | "CHANGED" | "UNCHANGED"
classify_incoming <- function(incoming, con, bronze_table, primary_key) {
  stopifnot(all(c("bl_bl_id_key", "row_hash") %in% names(incoming)))

  # Most recent hash on file per key (Bronze may hold multiple versions)
  last_known <- DBI::dbGetQuery(
    con,
    glue::glue(
      "
    SELECT bl_bl_id_key, row_hash
    FROM (
      SELECT bl_bl_id_key, row_hash,
             ROW_NUMBER() OVER (
               PARTITION BY bl_bl_id_key ORDER BY bronze_load_ts DESC
             ) AS rn
      FROM {bronze_table}
    ) ranked
    WHERE rn = 1
  "
    )
  )

  incoming |>
    left_join(
      last_known |> rename(row_hash_prev = row_hash),
      by = "bl_bl_id_key"
    ) |>
    mutate(
      action = case_when(
        is.na(row_hash_prev) ~ "NEW",
        row_hash != row_hash_prev ~ "CHANGED",
        TRUE ~ "UNCHANGED"
      )
    ) |>
    select(-row_hash_prev)
}

# -----------------------------------------------------------------------------
# 4. Write step: Bronze gets NEW/CHANGED only; audit log gets everything
# -----------------------------------------------------------------------------
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
  classified,
  con,
  bronze_table,
  audit_table,
  source_table_name,
  batch_id
) {
  load_ts <- Sys.time()

  # --- Bronze: only NEW / CHANGED rows get a full new version written
  to_write <- classified |>
    filter(action %in% c("NEW", "CHANGED")) |>
    mutate(
      bronze_batch_id = batch_id,
      bronze_load_ts = load_ts
    )

  if (nrow(to_write) > 0) {
    DBI::dbAppendTable(con, bronze_table, to_write)
  }

  # --- Audit log: every row, every day, regardless of action
  audit_rows <- classified |>
    transmute(
      source_table = source_table_name,
      batch_id = batch_id,
      load_ts = load_ts,
      bl_bl_id_key = bl_bl_id_key,
      row_hash = row_hash,
      action = action
    )

  DBI::dbAppendTable(con, audit_table, audit_rows)

  counts <- count(classified, action) |>
    tidyr::pivot_wider(names_from = action, values_from = n, values_fill = 0)

  invisible(list(
    batch_id = batch_id,
    load_ts = load_ts,
    new = counts$NEW %||% 0,
    changed = counts$CHANGED %||% 0,
    unchanged = counts$UNCHANGED %||% 0,
    total = nrow(classified)
  ))
}

`%||%` <- function(x, y) if (is.null(x) || length(x) == 0) y else x

# -----------------------------------------------------------------------------
# 5. Retention: keep last N versions PER KEY, not a time window
# -----------------------------------------------------------------------------
#' Purge Bronze down to the most recent N versions per bl_bl_id_key
#'
#' Chosen over a time-window purge because a stable entity can go 30+ days
#' with zero changes — a time window would leave it with no historical
#' snapshot at all, which isn't the goal. Version-count retention ties
#' storage to actual change activity instead of the calendar.
#'
#' @param con DBI connection
#' @param bronze_table Bronze table name
#' @param n_versions Number of most recent versions to retain per key (default 10)
purge_bronze_versions <- function(con, bronze_table, n_versions = 10) {
  sql <- glue::glue(
    "
    ;WITH ranked AS (
      SELECT *,
             ROW_NUMBER() OVER (
               PARTITION BY bl_bl_id_key ORDER BY bronze_load_ts DESC
             ) AS rn
      FROM {bronze_table}
    )
    DELETE FROM ranked WHERE rn > {n_versions};
  "
  )
  DBI::dbExecute(con, sql)
}

# -----------------------------------------------------------------------------
# 6. Orchestration — one call per pipeline run
# -----------------------------------------------------------------------------
#' Run the full hash-gate step for one day's pull
#'
#' @param incoming Raw data frame from the API pull (must include bl_bl_id_key)
#' @param con DBI connection
#' @param bronze_table Bronze table name
#' @param audit_table Audit log table name
#' @param source_table_name Label used in the audit log
#' @param batch_id This run's batch/run identifier
#' @param n_versions Versions to retain per key after this run (default 10)
#' @return invisible list of run counts, suitable for log_daily_etl_run()
run_hash_gate <- function(
  incoming,
  con,
  bronze_table,
  audit_table,
  source_table_name,
  batch_id,
  n_versions = 10
) {
  hashed <- add_row_hash(incoming)
  classified <- classify_incoming(hashed, con, bronze_table)
  result <- apply_hash_gate(
    classified,
    con,
    bronze_table,
    audit_table,
    source_table_name,
    batch_id
  )
  purge_bronze_versions(con, bronze_table, n_versions)

  result
}
