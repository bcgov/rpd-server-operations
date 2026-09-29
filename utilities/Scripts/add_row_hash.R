#' Get the set of columns to include in the row hash for a given data frame
#'
#' @param df A data frame of incoming Bronze-bound records
#' @param excluded A character vector of excluded column names
#' @return character vector of column names to hash, in stable (sorted) order
get_tracked_cols <- function(df, excluded) {
  sort(setdiff(names(df), excluded))
}


#' Add a row_hash column computed over tracked_cols only
#'
#' Sorting tracked_cols before hashing means column reordering upstream
#' never changes the hash — only actual value changes do.
#'
#' @param df Data frame with a bl_bl_id_key column plus business columns
#' @param tracked_cols character vector of columns to include in the hash;
#' @param primary_key records the primary key column name for reusability across datasets
#' @return df with a new row_hash (CHAR(32) / MD5 hex) column appended
add_row_hash <- function(df, primary_key, tracked_cols) {
  stopifnot(primary_key %in% names(df))
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
