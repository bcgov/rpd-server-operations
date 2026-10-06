#' Find keys present in the latest Bronze state but absent from today's pull
#'
#' @param incoming This run's pull (must have primary_key column)
#' @param con DBI connection
#' @param schema,table Bronze table location
#' @param primary_key Natural key column name
#' @param status_col Optional column to pull the key's last-known status
#'   alongside it (e.g. "bl_status"), for severity triage
#' @return tibble of missing keys, with their last-known status if requested
find_missing_from_pull <- function(
  incoming,
  con,
  schema,
  table,
  primary_key,
  status_col = NULL
) {
  pk_cols <- paste(primary_key, collapse = ", ")
  select_cols <- paste(c(primary_key, status_col), collapse = ", ")

  last_known <- DBI::dbGetQuery(
    con,
    glue::glue(
      "
    SELECT {select_cols}
    FROM (
      SELECT *,
             ROW_NUMBER() OVER (
               PARTITION BY {pk_cols} ORDER BY bronze_load_ts DESC
             ) AS rn
      FROM {schema}.{table}
    ) ranked
    WHERE rn = 1
  "
    )
  )

  last_known |>
    anti_join(incoming, by = primary_key)
}
