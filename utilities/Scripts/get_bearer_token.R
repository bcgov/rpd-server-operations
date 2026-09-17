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
