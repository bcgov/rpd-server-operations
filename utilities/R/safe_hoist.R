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

#' Safe Hoist All - Extract and concatenate values from deeply nested list columns
#'
#' Extracts specified fields from all elements at a given level in nested list
#' structures and concatenates them into a single string. Handles variable list
#' lengths, NA values, and complex nested structures commonly found in API responses.
#'
#' @param .data A data frame
#' @param .col Column name containing nested lists (supports tidy selection)
#' @param path A vector or list specifying the path to navigate through the nested
#'   structure. Can contain numeric indices (for list positions) and character
#'   strings (for named elements). The final element should be the field name
#'   to extract from all elements at that level.
#' @param sep Character string to separate multiple extracted values (default: ";")
#' @param .remove Logical; if TRUE, removes the original nested column from the
#'   result (default: FALSE)
#'
#' @return A data frame with a new column containing the concatenated extracted
#'   values. The new column name follows the pattern: original_col_path_elements
#'
#' @details This function is designed for complex nested API responses where you
#' need to extract the same field from multiple nested objects. For example,
#' extracting all participant names from a list of participant objects.
#'
#' The path parameter works as follows:
#' - Numeric values: Navigate to that index in a list (1-based indexing)
#' - Character values: Navigate to that named element
#' - Final character value: Extract this field from all elements at the current level
#'
#' @examples
#' \dontrun{
#' # Extract displayName from all participants
#' issues %>% safe_hoist_all(RequestParticipants, path = list("displayName"))
#'
#' # Extract from deeper nested structure
#' data %>% safe_hoist_all(nested_col,
#'                         path = list("users", "profile", "name"),
#'                         sep = " | ")
#'
#' # Navigate by index then extract field
#' data %>% safe_hoist_all(responses,
#'                         path = list(1L, "answers", "text"),
#'                         .remove = TRUE)
#' }
#'
#' @seealso \code{\link{safe_hoist}} for simpler list column extraction
#' @export
#'
safe_hoist_all <- function(.data, .col, path, sep = ";", .remove = FALSE) {
  .col <- tidyselect::vars_pull(names(.data), {{ .col }})
  col_data <- .data[[.col]]

  # Function to extract values from a single nested list element
  extract_nested_values <- function(x, path) {
    # Handle NA/NULL cases more carefully
    if (length(x) == 1 && (is.na(x) || is.null(x))) {
      return(NA_character_)
    }
    if (!is.list(x)) {
      return(NA_character_)
    }

    # Navigate through the path step by step
    current <- x
    for (i in seq_along(path)) {
      step <- path[[i]]

      # Check if current is valid before proceeding
      if (is.null(current) || !is.list(current)) {
        return(NA_character_)
      }

      # If this is the last step in the path
      if (i == length(path)) {
        # This should be the field we want to extract from all elements
        if (is.character(step)) {
          # Extract the field from all elements at this level
          if (length(current) > 0 && all(map_lgl(current, is.list))) {
            # We have a list of list elements (participants)
            values <- purrr::map_chr(current, function(element) {
              if (is.list(element) && step %in% names(element)) {
                val <- element[[step]]
                if (is.null(val)) {
                  return(NA_character_)
                }
                return(as.character(val))
              } else {
                return(NA_character_)
              }
            })
            # Remove NA values and concatenate
            values <- values[!is.na(values)]
            if (length(values) > 0) {
              return(paste(values, collapse = sep))
            }
          }
        }
        return(NA_character_)
      } else {
        # Navigate deeper - this is an intermediate step
        if (is.numeric(step)) {
          # Index into the list
          if (length(current) >= step && step > 0) {
            current <- current[[step]]
          } else {
            return(NA_character_)
          }
        } else if (is.character(step)) {
          # Navigate by name
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

  # Apply the extraction to each row - handle potential errors
  extracted_values <- purrr::map_chr(col_data, function(x) {
    purrr::possibly(extract_nested_values, NA_character_)(x, path)
  })

  # Create new column name
  new_col_name <- paste0(.col, "_", paste(path, collapse = "_"))

  # Add the new column to the data frame
  result <- .data %>%
    mutate(!!new_col_name := extracted_values, .after = all_of(.col))

  # Optionally remove the original column
  if (.remove) {
    result <- result %>% select(-all_of(.col))
  }

  return(result)
}
