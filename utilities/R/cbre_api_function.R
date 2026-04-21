#' Extract data from CBRE API (JSON response)
#'
#' Retrieves paginated JSON data from the CBRE EDP API using bearer token
#' authentication and returns the results as a tidy tibble.
#'
#' @param edp_table Character string specifying the table to query (required).
#' @param username API client ID used for authentication.
#' @param keyring_service Name of the keyring service containing the API secret.
#' @param base_url Base URL for the CBRE API.
#' @param endpoint_url Endpoint path for data extraction.
#' @param start_time Start time for data extraction in ISO 8601 format
#'   (e.g. "2025-01-01T00:00:00Z").
#' @param end_time End time for data extraction in ISO 8601 format.
#'   Defaults to the current system time in UTC.
#' @param max_retries Maximum number of retries for failed requests.
#' @param retry_delay Delay in seconds between retries.
#' @param max_pages Optional maximum number of pages to retrieve.
#'
#' @details
#' Authentication is performed using a bearer token retrieved via the client
#' credentials grant flow. Client secrets are expected to be stored securely
#' using the \code{keyring} package.
#'
#' @return A tibble containing the extracted data.
#'
#' @export

extract_cbre_data <- function(
  edp_table,
  username = "kWNLCjcu05JmqrlSsmDtMoSreuUa",
  keyring_service = "CBRE_API",
  base_url = "https://api.cbre.com:443/",
  endpoint_url = "t/digitaltech_us_edp/vantageanalytics/prod/v1/data/",
  start_time = "2025-01-01T00:00:00Z", # Should maybe update this in the future??
  end_time = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
  max_retries = 3,
  retry_delay = 2,
  max_pages = NULL
) {
  # Check required packages are available (but don't load them)
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

  # Input validation
  if (missing(edp_table) || is.null(edp_table) || edp_table == "") {
    stop("Table parameter is required and cannot be empty")
  }

  # Token Setup with error handling
  tryCatch(
    {
      # credentials <- read.csv(credentials_path, stringsAsFactors = FALSE)
      credentials <- keyring::key_get(
        service = keyring_service,
        username = username,
        keyring = NULL
      )
      if (!exists("credentials")) {
        stop("Client credentials are missing or empty")
      }
    },
    error = function(e) {
      stop("Error reading credentials: ", e$message)
    }
  )

  bearer_token <- get_bearer_token(
    base_url = base_url,
    username = username,
    credentials = credentials
  )

  # Initialize variables
  rows_returned <- 0
  total_rows <- 3000
  current_page <- 1
  page_count <- 25
  Data <- NULL

  cat("Starting data extraction for table:", edp_table, "\n")
  if (!is.null(max_pages)) {
    cat("Applying page cutoff: max_pages =", max_pages, "\n")
  }

  # Main data extraction loop
  while (
    current_page <= page_count &&
      (is.null(max_pages) || current_page <= max_pages)
  ) {
    cat("Processing page", current_page, "of", page_count, "\n")

    retry_count <- 0
    success <- FALSE

    # Retry loop for each page
    while (retry_count <= max_retries && !success) {
      tryCatch(
        {
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
            httr2::req_perform()

          # Check response status
          status_code <- httr2::resp_status(req)

          if (status_code < 200 || status_code >= 300) {
            stop("HTTP error ", status_code)
          }

          resp <- httr2::resp_body_json(req)

          # Validate response structure
          if (is.null(resp$data) || is.null(resp$data$rows)) {
            stop("Invalid response structure: missing data or rows")
          }

          # Update pagination info
          rows_returned <- rows_returned + resp$rowsReturned
          total_rows <- resp$totalRows
          current_page <- resp$currentPage + 1
          page_count <- resp$numberOfPages

          # Process column names
          column_names <- resp |>
            purrr::pluck("data") |>
            purrr::pluck("columns") |>
            tibble::enframe() |>
            dplyr::mutate(name = paste0("value_", name)) |>
            tibble::deframe()

          # Process data
          data <- resp |>
            purrr::pluck("data") |>
            purrr::pluck("rows") |>
            tibble::enframe() |>
            tidyr::unnest_wider(value, names_sep = "_") |>
            dplyr::select(-c(name)) |>
            plyr::rename(column_names)

          # Combine data
          if (is.null(Data)) {
            Data <- data
          } else {
            Data <- dplyr::bind_rows(Data, data)
          }

          success <- TRUE
        },
        error = function(e) {
          retry_count <<- retry_count + 1

          if (retry_count <= max_retries) {
            cat(
              "Request failed (attempt",
              retry_count,
              "of",
              max_retries + 1,
              "):",
              e$message,
              "\n"
            )
            cat("Retrying in", retry_delay, "seconds...\n")
            Sys.sleep(retry_delay)
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

  # Final validation
  if (is.null(Data) || nrow(Data) == 0) {
    warning("No data was retrieved for table: ", edp_table)
    return(tibble::tibble())
  }

  cat("Data extraction completed successfully!\n")
  cat("Total rows retrieved:", nrow(Data), "\n")
  cat("Total columns:", ncol(Data), "\n")

  return(Data)
}

# Example usage:
# data <- extract_cbre_data("archibus_ls")
# data <- extract_cbre_data("archibus_bl", start_time = "2024-01-01T00:00:00Z")
# data <- extract_cbre_data("archibus_property", credentials_path = "path/to/other/creds.csv")

#' Extract CSV data from CBRE EDP API (asynchronous export)
#'
#' Submits a CSV export request to the CBRE EDP API, waits for the export
#' to be processed, then retrieves and reassembles one or more CSV file
#' fragments into a single tibble.
#'
#' @param edp_table Character string specifying the table to query (required).
#' @param start_time Start time for data extraction in ISO 8601 format
#'   (e.g. "2025-01-01T00:00:00Z").
#' @param end_time End time for data extraction in ISO 8601 format.
#'   Defaults to the current system time in UTC.
#' @param base_url Base URL for the CBRE API.
#' @param ask_endpoint Endpoint path used to initiate the CSV export request.
#' @param file_endpoint Endpoint path used to retrieve generated CSV files.
#' @param username API client ID used for authentication.
#' @param keyring_service Name of the keyring service containing the API secret.
#' @param wait_seconds Number of seconds to wait before attempting to retrieve
#'   generated CSV files.
#'
#' @details
#' The function handles:
#' \itemize{
#'   \item Authentication via bearer token
#'   \item Asynchronous CSV export initiation
#'   \item Sequential retrieval of one or more CSV file parts
#'   \item Automatic reassembly into a single tibble
#' }
#'
#' All columns are returned as character data to preserve consistency across
#' CSV fragments.
#'
#' @return A tibble containing the full extracted dataset.
#'
#' @export

fetch_edp_csv <- function(
  edp_table,
  start_time,
  end_time = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
  base_url = "https://api.cbre.com:443/",
  ask_endpoint = "t/digitaltech_us_edp/vantageanalytics/prod/v1/data/",
  file_endpoint = "/t/digitaltech_us_edp/vantageanalytics/prod/v1/file/",
  username = "kWNLCjcu05JmqrlSsmDtMoSreuUa",
  keyring_service = "CBRE_API",
  wait_seconds = 1200
) {
  library(httr2)
  library(readr)
  library(dplyr)

  ## ---- credentials ----
  credentials <- keyring::key_get(
    service = keyring_service,
    username = username
  )

  ## ---- initial token ----
  bearer_token <- get_bearer_token(base_url, username, credentials)

  ## ---- ask for export ----
  resp_ask <- httr2::request(base_url) |>
    httr2::req_url_path_append(ask_endpoint) |>
    httr2::req_auth_bearer_token(bearer_token) |>
    httr2::req_method("POST") |>
    httr2::req_body_json(list(
      entity = edp_table,
      page = 1,
      startTime = start_time,
      endTime = end_time,
      format = "csv"
    )) |>
    httr2::req_perform() |>
    httr2::resp_body_json()

  message(resp_ask$message)

  fileId <- resp_ask$uuid

  ## ---- wait for files to be prepared ----
  Sys.sleep(wait_seconds)

  ## ---- refresh token (important) ----
  bearer_token <- get_bearer_token(base_url, username, credentials)

  ## ---- retrieve files ----
  all_data <- NULL
  fileNumber <- "1"
  lastFile <- "false"

  while (lastFile != "true") {
    req_retrieve <- httr2::request(base_url) |>
      httr2::req_url_path_append(file_endpoint) |>
      httr2::req_url_path_append(fileId) |>
      httr2::req_headers("file-num" = fileNumber) |>
      httr2::req_auth_bearer_token(bearer_token) |>
      httr2::req_perform()

    chunk <- req_retrieve |>
      httr2::resp_body_string() |>
      readr::read_csv(col_types = readr::cols(.default = "c"))

    all_data <- dplyr::bind_rows(all_data, chunk)

    fileNumber <- req_retrieve$headers$`next-file-num`
    lastFile <- req_retrieve$headers$`last-file`
  }

  all_data
}

#' Retrieve CBRE API bearer token
#'
#' Internal helper function that retrieves an OAuth bearer token from the
#' CBRE API using client credentials authentication.
#'
#' @param base_url Base URL for the CBRE API.
#' @param username API client ID used for authentication.
#' @param credentials API client secret associated with the username.
#'
#' @details
#' This function performs a client credentials grant flow and returns
#' a short-lived bearer token suitable for use with subsequent API requests.
#'
#' @return A character string containing the bearer access token.
#'
#' @keywords internal
get_bearer_token <- function(base_url, username, credentials) {
  token <- base64enc::base64encode(
    charToRaw(paste0(username, ":", credentials))
  )

  httr2::request(base_url) |>
    httr2::req_url_path_append("token") |>
    httr2::req_headers(
      Authorization = paste("Basic", token)
    ) |>
    httr2::req_url_query(grant_type = "client_credentials") |>
    httr2::req_method("POST") |>
    httr2::req_perform() |>
    httr2::resp_body_json() |>
    magrittr::extract2("access_token")
}

# Add documentation at some point
submit_edp_export <- function(
  edp_table,
  start_time = "2025-01-01T00:00:00Z", # Should maybe update this in the future??
  end_time = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
  base_url = "https://api.cbre.com:443/",
  ask_endpoint = "t/digitaltech_us_edp/vantageanalytics/prod/v1/data/",
  username = "kWNLCjcu05JmqrlSsmDtMoSreuUa",
  keyring_service = "CBRE_API"
) {
  # ---- credentials ----
  credentials <- keyring::key_get(
    service = keyring_service,
    username = username
  )

  # ---- authenticate ----
  bearer_token <- get_bearer_token(base_url, username, credentials)

  # ---- submit export ----
  resp <- request(base_url) |>
    httr2::req_url_path_append(ask_endpoint) |>
    httr2::req_auth_bearer_token(bearer_token) |>
    httr2::req_method("POST") |>
    httr2::req_body_json(list(
      entity = edp_table,
      page = 1,
      startTime = start_time,
      endTime = end_time,
      format = "csv"
    )) |>
    httr2::req_perform() |>
    resp_body_json()

  message(resp$message)

  list(
    file_id = resp$uuid,
    edp_table = edp_table,
    submitted_at = Sys.time()
  )
}

retrieve_edp_export <- function(
  file_id,
  base_url = "https://api.cbre.com:443/",
  file_endpoint = "/t/digitaltech_us_edp/vantageanalytics/prod/v1/file/",
  username = "kWNLCjcu05JmqrlSsmDtMoSreuUa",
  keyring_service = "CBRE_API",
  poll_interval = 60,
  timeout = 1200
) {
  credentials <- keyring::key_get(
    service = keyring_service,
    username = username
  )

  start_time <- Sys.time()

  # ---- wait until first file is available ----
  repeat {
    bearer_token <- get_bearer_token(base_url, username, credentials)

    probe <- try(
      request(base_url) |>
        httr2::req_url_path_append(file_endpoint) |>
        httr2::req_url_path_append(file_id) |>
        httr2::req_headers("file-num" = "1") |>
        httr2::req_auth_bearer_token(bearer_token) |>
        httr2::req_perform(),
      silent = TRUE
    )

    if (!inherits(probe, "try-error") && resp_status(probe) == 200) {
      print("successful response")
      break
    }

    if (difftime(Sys.time(), start_time, units = "secs") > timeout) {
      stop("Timed out waiting for EDP export to be ready")
    }
    print("Going for another loop")
    Sys.sleep(poll_interval)
  }

  # ---- retrieve all files ----
  all_data <- NULL
  fileNumber <- "1"
  lastFile <- "false"

  while (lastFile != "true") {
    bearer_token <- get_bearer_token(base_url, username, credentials)

    resp <- request(base_url) |>
      httr2::req_url_path_append(file_endpoint) |>
      httr2::req_url_path_append(file_id) |>
      httr2::req_headers("file-num" = fileNumber) |>
      httr2::req_auth_bearer_token(bearer_token) |>
      httr2::req_perform()

    chunk <- resp |>
      resp_body_string() |>
      readr::read_csv(col_types = cols(.default = "c"))

    all_data <- dplyr::bind_rows(all_data, chunk)

    fileNumber <- resp$headers$`next-file-num`
    lastFile <- resp$headers$`last-file`
  }

  all_data
}
