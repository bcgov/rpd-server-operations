#' Get the set of columns to include in the row hash for a given data frame
#'
#' @param df A data frame of incoming Bronze-bound records
#' @param excluded A character vector of excluded column names
#' @return character vector of column names to hash, in stable (sorted) order
get_tracked_cols <- function(df, excluded) {
  sort(setdiff(names(df), excluded))
}


#' Add a row_hash column computed over tracked_columns only
#'
#' Hashes the values in `tracked_columns` for each row. The primary key
#' column(s) should not be part of `tracked_columns`, so the hash only
#' reflects changes to the non-key business columns.
#'
#' @param df Data frame containing the primary key column(s) plus the
#'   business columns to be hashed
#' @param primary_key Character vector of one or more column names that make
#'   up the primary key (e.g. `"dv_dv_id"` or
#'   `c("rm_bl_id", "rm_fl_id", "rm_rm_id")`). Checked for presence in `df`
#' @param tracked_columns Character vector of columns to include in the hash
#' @return `df` with a new `row_hash` (CHAR(32) / MD5 hex) column appended

add_row_hash <- function(df, primary_key, tracked_columns) {
  stopifnot(all(primary_key %in% names(df)))
  df |>
    rowwise() |>
    mutate(
      row_hash = digest::digest(
        pick(all_of(tracked_columns)),
        algo = "md5"
      )
    ) |>
    ungroup()
}
