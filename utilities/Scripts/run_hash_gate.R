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
