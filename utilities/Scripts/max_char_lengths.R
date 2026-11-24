max_char_lengths <- function(df) {
  char_cols <- vapply(df, is.character, logical(1))

  vapply(
    df[, char_cols, drop = FALSE],
    function(x) max(nchar(x, keepNA = FALSE)),
    integer(1)
  )
}

basic_str <- function(df) {
  str(df, vec.len = 0, max.level = 1)
}
