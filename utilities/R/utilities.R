#' Extract Data from CBRE EDP API
#'
#' Queries a specified table from the CBRE Enterprise Data Platform (EDP) API,
#' handling pagination, retries with exponential backoff, and partial data
#' recovery on failure. Supports resuming from a specific page to continue
#' interrupted extractions.
#'
#' @param edp_table Character. The EDP table/entity name to query. Required.
#' @param username Character. The API username. Defaults to
#'   \code{"kWNLCjcu05JmqrlSsmDtMoSreuUa"}.
#' @param keyring_service Character. The keyring service name used to retrieve
#'   API credentials. Defaults to \code{"CBRE_API"}.
#' @param base_url Character. Base URL of the CBRE API. Defaults to
#'   \code{"https://api.cbre.com:443/"}.
#' @param endpoint_url Character. Path appended to \code{base_url} to form the
#'   data endpoint. Defaults to
#'   \code{"t/digitaltech_us_edp/vantageanalytics/prod/v1/data/"}.
#' @param start_time Character. ISO 8601 UTC timestamp for the start of the
#'   query window. Defaults to \code{"2025-01-01T00:00:00Z"}.
#' @param end_time Character. ISO 8601 UTC timestamp for the end of the query
#'   window. Defaults to the current system time in UTC.
#' @param max_retries Integer. Maximum number of retry attempts per page before
#'   aborting. Defaults to \code{3} (i.e. 4 total attempts).
#' @param retry_delay Numeric. Base delay in seconds between retries. Actual
#'   wait time increases exponentially: \code{retry_delay * 2^(attempt - 1)}.
#'   Defaults to \code{2}.
#' @param max_pages Integer or \code{NULL}. If set, limits the number of pages
#'   fetched per call. Useful for testing or chunking large extractions.
#'   Defaults to \code{NULL} (no limit).
#' @param start_page Integer. Page number to begin extraction from. Set to
#'   \code{result$resume_args$start_page} to resume from a partial result.
#'   Defaults to \code{1}.
#' @param ETL_ENV Character. The ETL environment, read from the \code{ETL_ENV}
#'   environment variable. Used by \code{apply_proxy_if_needed()}.
#'
#' @return A named list with the following fields:
#'   \describe{
#'     \item{\code{status}}{Character. Either \code{"complete"} or
#'       \code{"partial"}.}
#'     \item{\code{data}}{A \code{\link[tibble]{tibble}} containing all rows
#'       retrieved up to the point of completion or failure.}
#'     \item{\code{rows_collected}}{Integer. Number of rows in \code{data}.}
#'     \item{\code{failed_at_page}}{Integer. The page number where extraction
#'       failed. Only present when \code{status == "partial"}.}
#'     \item{\code{page_count}}{Integer. Total number of pages reported by the
#'       API. Only present when \code{status == "partial"}.}
#'     \item{\code{resume_args}}{Named list of arguments to pass directly to
#'       \code{extract_cbre_data()} via \code{do.call()} to resume the
#'       extraction. Only present when \code{status == "partial"}.}
#'     \item{\code{error}}{Character. The error message that caused the failure.
#'       Only present when \code{status == "partial"}.}
#'   }
#'
#' @details
#' On failure, the function returns all data collected up to that point rather
#' than throwing an error, allowing the extraction to be resumed. The typical
#' resume pattern is:
#'
#' \preformatted{
#' result <- extract_cbre_data("your_table")
#'
#' if (result$status == "partial") {
#'   resumed  <- do.call(extract_cbre_data, result$resume_args)
#'   full_data <- dplyr::bind_rows(result$data, resumed$data)
#' }
#' }
#'
#' Bearer token acquisition is delegated to \code{\link{get_bearer_token}},
#' which caches the token for the session and re-checks validity on every page,
#' ensuring long-running jobs refresh automatically without over-requesting the
#' auth endpoint.
#'
#' @seealso \code{\link{get_bearer_token}}
#'
#' @importFrom dplyr bind_rows mutate select
#' @importFrom httr2 request req_url_path_append req_auth_bearer_token
#'   req_method req_body_json req_perform resp_status resp_body_json
#' @importFrom keyring key_get
#' @importFrom purrr pluck
#' @importFrom tibble tibble enframe deframe
#' @importFrom tidyr unnest_wider
#' @importFrom plyr rename

call_cbre_api <- function(
  edp_table,
  username = "kWNLCjcu05JmqrlSsmDtMoSreuUa",
  keyring_service = "CBRE_API",
  base_url = "https://api.cbre.com:443/",
  endpoint_url = "t/digitaltech_us_edp/vantageanalytics/prod/v1/data/",
  start_time = "2025-01-01T00:00:00Z",
  end_time = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
  max_retries = 3,
  retry_delay = 2,
  max_pages = NULL,
  start_page = 1, # Resume from a partial result: result$resume_args$start_page
  ETL_ENV = Sys.getenv("ETL_ENV")
) {
  required_packages <- c(
    "dplyr",
    "httr2",
    "keyring",
    "purrr",
    "tibble",
    "magrittr"
  )
  missing_packages <- required_packages[
    !sapply(required_packages, requireNamespace, quietly = TRUE)
  ]
  if (length(missing_packages) > 0) {
    stop(
      "Missing required packages: ",
      paste(missing_packages, collapse = ", ")
    )
  }

  if (missing(edp_table) || is.null(edp_table) || edp_table == "") {
    stop("Table parameter is required and cannot be empty")
  }

  tryCatch(
    {
      credentials <- keyring::key_get(
        service = keyring_service,
        username = username,
        keyring = NULL
      )
      if (!exists("credentials")) {
        stop("Client credentials are missing or empty")
      }
    },
    error = function(e) stop("Error reading credentials: ", e$message)
  )

  # Token is now fetched/reused via cache rather than always refreshed
  bearer_token <- get_bearer_token(
    base_url = base_url,
    username = username,
    credentials = credentials
  )

  rows_returned <- 0
  total_rows <- 3000
  current_page <- start_page # Honour resume point
  page_count <- if (start_page > 1) Inf else 25 # Will be corrected on first response
  Data <- NULL
  failed_at_page <- NULL

  cat("Starting data extraction for table:", edp_table, "\n")
  if (start_page > 1) {
    cat("Resuming from page:", start_page, "\n")
  }
  if (!is.null(max_pages)) {
    cat("Applying page cutoff: max_pages =", max_pages, "\n")
  }

  extraction_error <- tryCatch(
    {
      while (
        current_page <= page_count &&
          (is.null(max_pages) || current_page <= (start_page - 1 + max_pages))
      ) {
        cat(
          "Processing page",
          current_page,
          "of",
          if (is.finite(page_count)) page_count else "?",
          "\n"
        )

        retry_count <- 0
        success <- FALSE

        while (retry_count <= max_retries && !success) {
          tryCatch(
            {
              # Re-check token freshness on each page in case of long-running jobs
              bearer_token <- get_bearer_token(
                base_url = base_url,
                username = username,
                credentials = credentials
              )

              req <- httr2::request(base_url) |>
                httr2::req_url_path_append(endpoint_url) |>
                httr2::req_auth_bearer_token(bearer_token) |>
                httr2::req_method("POST") |>
                httr2::req_body_json(list(
                  entity = edp_table,
                  page = current_page,
                  startTime = start_time,
                  endTime = end_time,
                  format = "json"
                )) |>
                apply_proxy_if_needed() |>
                httr2::req_perform()

              status_code <- httr2::resp_status(req)
              if (status_code < 200 || status_code >= 300) {
                stop("HTTP error ", status_code)
              }

              resp <- httr2::resp_body_json(req)

              if (is.null(resp$data) || is.null(resp$data$rows)) {
                stop("NO_DATA_SENTINEL")
              }

              rows_returned <- rows_returned + resp$rowsReturned
              total_rows <- resp$totalRows
              current_page <- resp$currentPage + 1
              page_count <- resp$numberOfPages

              column_names <- resp |>
                purrr::pluck("data") |>
                purrr::pluck("columns") |>
                tibble::enframe() |>
                dplyr::mutate(name = paste0("value_", name)) |>
                tibble::deframe()

              data <- resp |>
                purrr::pluck("data") |>
                purrr::pluck("rows") |>
                tibble::enframe() |>
                tidyr::unnest_wider(value, names_sep = "_") |>
                dplyr::select(-name) |>
                plyr::rename(column_names)

              Data <- if (is.null(Data)) data else dplyr::bind_rows(Data, data)
              success <- TRUE
            },
            error = function(e) {
              retry_count <<- retry_count + 1

              if (retry_count <= max_retries) {
                wait <- retry_delay * 2^(retry_count - 1)
                cat(
                  "Request failed (attempt",
                  retry_count,
                  "of",
                  max_retries + 1,
                  "):",
                  e$message,
                  "\n"
                )
                cat("Retrying in", wait, "seconds...\n")
                Sys.sleep(wait)
              } else {
                stop(
                  "Failed to retrieve data after ",
                  max_retries + 1,
                  " attempts. Last error: ",
                  e$message
                )
              }
            }
          )
        }
      }
      NULL
    },
    error = function(e) e
  )

  if (!is.null(extraction_error)) {
    failed_at_page <- current_page

    if (
      grepl(
        "NO_DATA_SENTINEL",
        conditionMessage(extraction_error),
        fixed = TRUE
      )
    ) {
      cat("\n--- No data returned ---\n")
      return(list(
        status = "no_data",
        data = tibble::tibble(),
        rows_collected = 0
      ))
    }

    cat("\n--- Extraction failed ---\n")
    cat("Error:", conditionMessage(extraction_error), "\n")
    cat(
      "Failed at page:",
      failed_at_page,
      "of",
      if (is.finite(page_count)) page_count else "?",
      "\n"
    )
    cat(
      "Rows collected before failure:",
      if (!is.null(Data)) nrow(Data) else 0,
      "\n"
    )
    return(list(
      status = "partial",
      data = if (!is.null(Data)) Data else tibble::tibble(),
      failed_at_page = failed_at_page,
      page_count = if (is.finite(page_count)) page_count else NA,
      rows_collected = if (!is.null(Data)) nrow(Data) else 0,
      resume_args = list(
        edp_table = edp_table,
        start_time = start_time,
        end_time = end_time,
        start_page = failed_at_page
      ),
      error = conditionMessage(extraction_error)
    ))
  }

  if (is.null(Data) || nrow(Data) == 0) {
    warning("No data was retrieved for table: ", edp_table)
    return(list(
      status = "complete",
      data = tibble::tibble(),
      rows_collected = 0
    ))
  }

  cat("Data extraction completed successfully!\n")
  cat("Total rows retrieved:", nrow(Data), "\n")
  cat("Total columns:", ncol(Data), "\n")

  return(list(
    status = "complete",
    data = Data,
    rows_collected = nrow(Data)
  ))
}

#' Get Bearer Token with Caching
#'
#' Retrieves a bearer token for CBRE API authentication, reusing a cached token
#' if one exists and is still valid. A new token is only fetched when the cached
#' token is absent or within \code{buffer_secs} of expiry.
#'
#' @param base_url Character. The base URL of the CBRE API, e.g.
#'   \code{"https://api.cbre.com:443/"}.
#' @param username Character. The API username used to construct the Basic Auth
#'   credential.
#' @param credentials Character. The API secret/password retrieved from the
#'   keyring, paired with \code{username} to form the Basic Auth header.
#' @param buffer_secs Numeric. Number of seconds before true token expiry at
#'   which a refresh is triggered. Defaults to \code{60}.
#'
#' @return Character. A valid bearer token (access token) string.
#'
#' @details
#' Token state is stored in a module-level environment \code{.token_cache},
#' which persists for the duration of the R session. This avoids redundant
#' auth requests when \code{extract_cbre_data()} is called repeatedly across
#' multiple tables. The API response is expected to include both
#' \code{access_token} and \code{expires_in} fields.
#'
#' @seealso \code{\link{extract_cbre_data}}
#'
#' @importFrom base64enc base64encode
#' @importFrom httr2 request req_url_path_append req_headers req_url_query
#'   req_method req_perform resp_body_json

# Module-level token cache — lives for the duration of your R session
.token_cache <- new.env(parent = emptyenv())

get_bearer_token <- function(
  base_url,
  username,
  credentials,
  buffer_secs = 60
) {
  now <- as.numeric(Sys.time())

  # Return cached token if it's still valid (with a buffer before true expiry)
  if (
    !is.null(.token_cache$token) &&
      !is.null(.token_cache$expires_at) &&
      now < (.token_cache$expires_at - buffer_secs)
  ) {
    cat(
      "Reusing cached bearer token (expires in",
      round(.token_cache$expires_at - now),
      "seconds)\n"
    )
    return(.token_cache$token)
  }

  cat("Fetching new bearer token...\n")
  token_b64 <- base64enc::base64encode(
    charToRaw(paste0(username, ":", credentials))
  )

  resp <- httr2::request(base_url) |>
    httr2::req_url_path_append("token") |>
    httr2::req_headers(Authorization = paste("Basic", token_b64)) |>
    httr2::req_url_query(grant_type = "client_credentials") |>
    httr2::req_method("POST") |>
    apply_proxy_if_needed() |>
    httr2::req_perform() |>
    httr2::resp_body_json()

  # Cache the token and its expiry (API should return expires_in seconds)
  .token_cache$token <- resp$access_token
  .token_cache$expires_at <- now + as.numeric(resp$expires_in)

  cat("New token cached, valid for", resp$expires_in, "seconds\n")
  return(.token_cache$token)
}

call_jira_api <- function(
  query_url,
  expand_opts,
  dashboard_id,
  max_results = 100,
  token_string,
  start_time
) {
  nextPageToken = NULL
  progress = 0
  round = 1

  while (progress < 2) {
    req <- request(query_url) |>
      req_headers_redacted(Authorization = token_string) |> # redacted by httr2 in printed output
      req_url_query(
        jql = I(
          # I wrapper skips auto-formatting of the extra "=" sign
          utils::URLencode(
            paste0(
              "project=",
              DASHBOARD_ID,
              " AND updated >= \"",
              etl_window$jira_start_time,
              "\""
            ),
            repeated = TRUE
          )
        ),
        expand = expand_opts,
        maxResults = max_results,
        fields = "*all",
        # startAt = start_at, #deprecated for nextPageToken
        nextPageToken = nextPageToken,
        .multi = "comma" # control how vectors are appended, for expand_opts
      ) |>
      apply_proxy_if_needed() |>
      req_error(
        is_error = function(resp) {
          lr <- resp_header(resp, "x-seraph-loginreason")
          bad_auth <- !is.null(lr) &&
            grepl("AUTHENTICATED_FAILED|AUTHENTICATION_DENIED", lr)
          empty_ok <- FALSE # we only care about bad_auth here
          bad_auth || empty_ok
        },

        body = function(resp) {
          paste0(
            "Auth Failure for ",
            SCRIPT_NAME,
            " reason: ",
            resp_header(resp, "x-seraph-loginreason") %||% "UNKNOWN",
            " traceid: ",
            resp_header(resp, "atl-traceid") %||% "NA",
            " url: ",
            resp_url(resp)
          )
        }
      )

    # Perform request with error handling and structured logging
    resp <- tryCatch(
      req_perform(req) |> resp_body_json(),
      error = function(e) {
        # Compose a one-line description with context
        desc <- if (!is.null(e$body) && is.character(e$body)) {
          e$body
        } else {
          e$message
        }
        # Log error to daily run file
        log_daily_etl_run(
          api_name = API_NAME,
          script_name = SCRIPT_NAME,
          table_name = DASHBOARD_ID,
          status = "FAILURE",
          message = substr(desc, 1, 500)
        )
        stop(e) # rethrow so task scheduler flags a failure (current monitoring is by Nagios)
      }
    )
    # Used to update total_results in while loop
    nextPageToken <- resp["nextPageToken"][[1]]

    if (is.null(nextPageToken)) {
      progress <- 2
    }

    if (length(resp$issues) == 0) {
      # API succeeded, nothing to load
      no_data_msg <- paste0(
        "No data returned from API for window ",
        etl_window$jira_start_time,
        " to ",
        format(Sys.time(), tz = "UTC"),
        " UTC"
      )

      cat(no_data_msg, "— nothing to load. Exiting gracefully.\n")

      log_daily_etl_run(
        api_name = API_NAME,
        script_name = SCRIPT_NAME,
        table_name = DASHBOARD_ID,
        duration = as.numeric(difftime(Sys.time(), task_start, units = "secs")),
        status = "NO_DATA",
        message = no_data_msg
      )
      cond <- structure(
        class = c("no_data_condition", "condition"),
        list(message = no_data_msg)
      )
      stop(cond)
    }
    if (round == 1) {
      data <- resp$issues
    } else {
      data <- full_join(data, resp$issues)
    }

    round <- 2
  }
}
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

etl_stage <- function(stage_name, expr) {
  withCallingHandlers(
    expr,
    error = function(e) {
      rlang::abort(
        conditionMessage(e),
        class = "etl_stage_error",
        stage = stage_name,
        parent = e
      )
    }
  )
}

log_daily_etl_run <- function(
  api_name,
  script_name,
  table_name = NA_character_,
  status,
  duration = NA_integer_,
  n_inserted = NA_integer_,
  n_updated = NA_integer_,
  n_deleted = NA_integer_,
  message = NA_character_,
  etl_env = Sys.getenv("ETL_ENV", unset = "UNKNOWN")
) {
  # Resolve repo root & log directory
  log_dir <- here::here("logs")
  if (!dir.exists(log_dir)) {
    dir.create(log_dir, recursive = TRUE)
  }

  # Daily log file (overwritten each day)
  log_file <- file.path(
    log_dir,
    paste0("daily_etl_log_", Sys.Date(), ".csv")
  )

  # Construct log row
  log_row <- tibble::tibble(
    run_timestamp = format(
      lubridate::with_tz(Sys.time(), tzone = "America/Vancouver"),
      "%Y-%m-%d %H:%M:%S"
    ),
    etl_env = etl_env,
    host = Sys.info()[["nodename"]],
    api_name = api_name,
    script_name = script_name,
    table_name = table_name,
    duration = duration,
    status = status,
    n_inserted = n_inserted,
    n_updated = n_updated,
    n_deleted = n_deleted,
    message = message
  )

  # Write or append (single-writer assumption on Muon)
  if (!file.exists(log_file)) {
    readr::write_csv(log_row, log_file)
  } else {
    readr::write_csv(log_row, log_file, append = TRUE)
  }

  invisible(log_row)
}

max_char_lengths <- function(df) {
  char_cols <- vapply(df, is.character, logical(1))

  vapply(
    df[, char_cols, drop = FALSE],
    function(x) max(nchar(x, keepNA = FALSE)),
    integer(1)
  )
}

fiscal_year_label <- function(date) {
  yr <- year(date)
  mo <- month(date)

  # If before April, fiscal year started in the previous calendar year
  fy_start <- if_else(mo < 4, yr - 1L, yr)
  fy_end <- fy_start + 1L

  sprintf("FY%02d%02d", fy_start %% 100, fy_end %% 100)
}

apply_proxy_if_needed <- function(
  req,
  etl_env = Sys.getenv("ETL_ENV", unset = "UNKNOWN")
) {
  if (etl_env == "Muon") {
    req <- req |> httr2::req_proxy("142.34.229.249", 8080)
  }
  req
}

get_etl_window <- function(today = Sys.Date()) {
  day_of_week <- weekdays(today)

  lookback_days <- if (day_of_week == "Monday") 3L else 1L

  list(
    cbre_start_time = paste0(today - lookback_days, "T00:00:00Z"),
    cbre_end_time = paste0(
      today,
      "T",
      format(lubridate::with_tz(Sys.time(), tzone = "UTC"), "%H:%M:%S"),
      "Z"
    ),
    jira_start_time = paste(today - lookback_days, "00:00")
  )
}
