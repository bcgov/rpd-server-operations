ensure_columns <- function(df, cols, value = NA_character_) {
  missing_cols <- setdiff(cols, names(df))
  df[missing_cols] <- value
  df
}
