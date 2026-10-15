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
  status_col = NULL,
  load_ts,
  batch_id,
  key_sep = "|"
) {
  # Assign variables for testing.
  # incoming <- hashed
  # schema <- SCHEMA_NAME
  # table <- TABLE_NAME
  # primary_key <- PRIMARY_KEY
  # status_col = NULL
  # load_ts <- task_start
  # batch_id <- BATCH_ID
  # key_sep = "|"
  pk_cols <- paste(primary_key, collapse = ", ")
  select_cols <- paste(c(primary_key, status_col), collapse = ", ")
  source_key_value <- paste(primary_key, collapse = key_sep)

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

  # normalise the optional status column to a fixed name
  if (!is.null(status_col)) {
    last_known <- dplyr::rename(
      last_known,
      last_status = dplyr::all_of(status_col)
    )
  } else {
    cols_needed <- "last_status"
    last_known <- ensure_columns(last_known, cols_needed)
  }

  missing <- last_known |>
    anti_join(incoming, by = primary_key) |>
    tidyr::unite(
      "source_key",
      dplyr::all_of(primary_key),
      sep = key_sep,
      remove = TRUE
    ) |>
    dplyr::mutate(
      source_key_values = paste(primary_key, collapse = key_sep),
      .before = last_status
    ) |>
    dplyr::mutate(
      source_system = schema,
      source_table = table,
      load_ts = as.POSIXct(load_ts, tz = "UTC"),
      batch_id = batch_id
    )

  if (nrow(missing) > 0) {
    current_tombstones <- DBI::dbGetQuery(
      con,
      glue::glue(
        "
    SELECT
    tombstone_id,
    source_key_values
    FROM ServerLogs.Tombstones
    WHERE source_key_values = '{source_key_value}'
  "
      )
    )

    new_tombstones <- missing |>
      anti_join(current_tombstones)

    if (nrow(new_tombstones) > 0) {
      DBI::dbAppendTable(
        con,
        DBI::Id(schema = "ServerLogs", table = "Tombstones"),
        new_tombstones
      )
      write.csv(
        new_tombstones,
        here::here(paste0(
          "logs/missing/",
          Sys.Date(),
          "-",
          table,
          "-missing.csv"
        )),
        row.names = FALSE
      )
    }
  }

  # write an else statement to do something if nrow(missing) == 0 or if nrow(new_tombstones) == 0
}
