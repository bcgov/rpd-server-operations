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
                stop("Invalid response structure: missing data or rows")
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
    start_time = paste0(today - lookback_days, "T00:00:00Z"),
    end_time = paste0(today, "T", format(Sys.time(), "%H:%M:%S"), "Z")
  )
}
