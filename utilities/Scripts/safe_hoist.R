#' Safe Hoist - Extract values from list columns with NA handling
#'
#' A safe wrapper around tidyr::hoist() that handles both list columns and NA values
#' without throwing errors. When the column is a list, it extracts the specified
#' values using hoist(). When the column contains NA values, it creates new columns
#' filled with NA instead of erroring.
#'
#' @param .data A data frame
#' @param .col Column name containing lists or NA values (supports tidy selection)
#' @param ... Arguments passed to tidyr::hoist(), typically in the form
#'   new_col = "field_name" or new_col = list("nested", "path")
#'
#' @return A data frame with new columns extracted from the list column, or
#'   NA-filled columns when the source column contains non-list values
#'
#' @details This function is particularly useful when working with API responses
#' where some rows may have nested list data while others contain NA values.
#' It prevents the common error that occurs when hoist() encounters non-list values.
#'
#' @examples
#' \dontrun{
#' # Basic usage with list column
#' df %>% safe_hoist(nested_col, name = "display_name", id = "user_id")
#'
#' # Handles mixed list/NA column gracefully
#' df %>% safe_hoist(api_response,
#'                   user_name = list("user", "name"),
#'                   user_active = list("user", "active"))
#' }
#'
#' @seealso \code{\link[tidyr]{hoist}} for the underlying function
#' @export
#'
safe_hoist <- function(.data, .col, ...) {
  .col <- tidyselect::vars_pull(names(.data), {{ .col }})
  if (is.list(.data[[.col]])) {
    tidyr::hoist(.data, .col, ...)
  } else {
    dot_args <- list(...)
    dot_args <- dot_args[setdiff(names(dot_args), names(formals(hoist)))]
    mutate(.data, !!!replace(dot_args, TRUE, NA))
  }
}
