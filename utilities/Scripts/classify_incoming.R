#' Classify each incoming record as NEW / CHANGED / UNCHANGED
#'
#' @param incoming Hashed incoming data (must have the primary key column(s)
#'   and `row_hash`)
#' @param con DBI connection
#' @param schema Name of the schema for the Bronze table
#' @param table Name of the Bronze table (without the schema)
#' @param primary_key Character vector of one or more primary key column
#'   names, used to match incoming rows to the latest Bronze version (and as
#'   the partitioning columns when finding it)
#' @return `incoming` with an `action` column added:
#'   "NEW" | "CHANGED" | "UNCHANGED"
classify_incoming <- function(incoming, con, schema, table, primary_key) {
  stopifnot(all(c(primary_key, "row_hash") %in% names(incoming)))

  # Comma-separated key columns for SQL; works for one column or several
  pk_cols <- paste(primary_key, collapse = ", ")

  # Most recent hash on file per key (Bronze may hold multiple versions)
  last_known <- DBI::dbGetQuery(
    con,
    glue::glue(
      "
    SELECT {pk_cols}, row_hash
    FROM (
      SELECT {pk_cols}, row_hash,
             ROW_NUMBER() OVER (
               PARTITION BY {pk_cols} ORDER BY bronze_load_ts DESC
             ) AS rn
      FROM {schema}.{table}
    ) ranked
    WHERE rn = 1
  "
    )
  )

  incoming |>
    left_join(
      last_known |> rename(row_hash_prev = row_hash),
      by = primary_key
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
