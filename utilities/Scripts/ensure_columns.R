ensure_columns <- function(df, cols) {
  missing_cols <- setdiff(cols, names(df))
  df[missing_cols] <- NA_character_
  df
}
