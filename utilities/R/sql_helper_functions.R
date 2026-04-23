max_char_lengths <- function(df) {
  char_cols <- vapply(df, is.character, logical(1))

  vapply(
    df[, char_cols, drop = FALSE],
    function(x) max(nchar(x, keepNA = FALSE)),
    integer(1)
  )
}
