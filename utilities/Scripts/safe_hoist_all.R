#' Safe Hoist All - Extract and concatenate values from deeply nested list columns
#'
#' Extracts specified fields from all elements at a given level in nested list
#' structures and concatenates them into a single string. Handles variable list
#' lengths, NA values, and complex nested structures commonly found in API responses.
#' When the source column is not a list (e.g. a vector of NAs returned by an API
#' call with no data), all output columns are gracefully populated with NA.
#'
#' @param .data A data frame.
#' @param .col Column name containing nested lists (supports tidy selection).
#' @param ... Named arguments of the form \code{new_col_name = path}, where the
#'   argument name becomes the output column name and the value is a character
#'   vector or list specifying the path to navigate through the nested structure.
#'   Multiple arguments can be supplied to extract several fields in one call;
#'   output columns are inserted in order after \code{.col}. Numeric values in
#'   the path navigate to a list index (1-based); character values navigate to a
#'   named element. The final element in the path is extracted from all objects
#'   at that level and concatenated.
#' @param sep Character string used to separate multiple extracted values.
#'   Default: \code{";"}.
#' @param .remove Logical; if \code{TRUE}, removes the original nested column
#'   from the result. Default: \code{FALSE}.
#'
#' @return A data frame with one new column per \code{...} argument, each
#'   containing the concatenated extracted values for that path, inserted after
#'   \code{.col}.
#'
#' @details
#' This function is designed for complex nested API responses where you need to
#' extract the same field from multiple nested objects — for example, pulling
#' \code{displayName} from every participant in a list of participant objects.
#'
#' Unlike \code{\link{safe_hoist}}, which uses \code{tidyr::hoist()} internally
#' and targets a single element at a known index, \code{safe_hoist_all} fans out
#' across all elements at the terminal level of the path and concatenates the
#' results. Integer indexing mid-path is supported for intermediate navigation,
#' but the final path step should always be a named field.
#'
#' @examples
#' \dontrun{
#' # Extract displayName from all participants, naming the output column
#' issues |> safe_hoist_all(RequestParticipants, Participants = "displayName")
#'
#' # Extract multiple fields in one call
#' issues |>
#'   safe_hoist_all(
#'     RequestParticipants,
#'     Participants     = "displayName",
#'     ParticipantEmail = "emailAddress"
#'   )
#'
#' # Navigate a deeper path before fanning out, with a custom separator
#' data |>
#'   safe_hoist_all(
#'     nested_col,
#'     Names = c("users", "profile", "name"),
#'     sep = " | "
#'   )
#'
#' # Remove the source column after extraction
#' data |> safe_hoist_all(responses, Answers = c("answers", "text"), .remove = TRUE)
#' }
#'
#' @seealso \code{\link{safe_hoist}} for single-element extraction from list columns.
#' @export
safe_hoist_all <- function(.data, .col, ..., sep = ";", .remove = FALSE) {
  .col_name <- unname(tidyselect::vars_pull(names(.data), {{ .col }}))
  col_data <- .data[[.col_name]]

  # Capture named ... args — name = output column, value = path vector
  dot_args <- list(...)
  if (
    length(dot_args) == 0 ||
      is.null(names(dot_args)) ||
      any(names(dot_args) == "")
  ) {
    rlang::abort(
      "All `...` arguments must be named. Use `new_col_name = path` syntax."
    )
  }

  extract_nested_values <- function(x, path) {
    if (is.null(x) || (length(x) == 1 && is.na(x))) {
      return(NA_character_)
    }
    if (!is.list(x)) {
      return(NA_character_)
    }

    current <- x
    path <- as.list(path) # normalise — accepts c() or list() from caller

    for (i in seq_along(path)) {
      step <- path[[i]]
      if (is.null(current) || !is.list(current)) {
        return(NA_character_)
      }

      if (i == length(path)) {
        if (is.character(step)) {
          if (length(current) > 0 && all(purrr::map_lgl(current, is.list))) {
            values <- purrr::map_chr(current, function(element) {
              if (is.list(element) && step %in% names(element)) {
                val <- element[[step]]
                if (is.null(val)) {
                  return(NA_character_)
                }
                as.character(val)
              } else {
                NA_character_
              }
            })
            values <- values[!is.na(values)]
            if (length(values) > 0) return(paste(values, collapse = sep))
          }
        }
        return(NA_character_)
      } else {
        if (is.numeric(step)) {
          if (length(current) >= step && step > 0) {
            current <- current[[step]]
          } else {
            return(NA_character_)
          }
        } else if (is.character(step)) {
          if (step %in% names(current)) {
            current <- current[[step]]
          } else {
            return(NA_character_)
          }
        } else {
          return(NA_character_)
        }
      }
    }
    return(NA_character_)
  }

  # Process each named ... arg and add its column, chaining .after the previous
  result <- .data
  after_col <- .col_name

  for (new_col_name in names(dot_args)) {
    path <- dot_args[[new_col_name]]
    # cat("new_col_name:", new_col_name, "\n")
    # cat("last_col class:", class(after_col), "\n")
    # cat("last_col typeof:", typeof(after_col), "\n")
    # cat("last_col value:", after_col, "\n")

    extracted_values <- purrr::map_chr(col_data, function(x) {
      tryCatch(
        extract_nested_values(x, path),
        error = function(e) NA_character_
      )
    })
    result <- mutate(
      result,
      !!new_col_name := extracted_values,
      .after = all_of(after_col)
    )
    last_col <- new_col_name
  }

  if (.remove) {
    result <- select(result, -all_of(.col_name))
  }

  result
}
