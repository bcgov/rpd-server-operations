#' Classify each incoming record as NEW / CHANGED / UNCHANGED
#'
#' @param incoming Hashed incoming data (must have bl_bl_id_key, row_hash)
#' @param con DBI connection
#' @param bronze_table Name of the Bronze table (schema-qualified if needed)
#' @return incoming with an `action` column added: "NEW" | "CHANGED" | "UNCHANGED"
classify_incoming <- function(incoming, con, schema, table, primary_key) {
  stopifnot(all(c(primary_key, "row_hash") %in% names(incoming)))

  # Most recent hash on file per key (Bronze may hold multiple versions)
  last_known <- DBI::dbGetQuery(
    con,
    glue::glue(
      "
    SELECT {primary_key}, row_hash
    FROM (
      SELECT {primary_key}, row_hash,
             ROW_NUMBER() OVER (
               PARTITION BY {primary_key} ORDER BY bronze_load_ts DESC
             ) AS rn
      FROM {schema}.{table}
    ) ranked
    WHERE rn = 1
  "
    )
  )

  test <- incoming |>
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
