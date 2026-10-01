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
