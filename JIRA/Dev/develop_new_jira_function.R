token_string <- TOKEN_STRING
dashboard_id <- "SBP"
start_time <- "2024-01-01 00:00"
expand_opts <- c("changelog", "names", "fields")

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

  # while (progress < 2) {
    req <- request(query_url) |>
      req_headers_redacted(Authorization = token_string) |> # redacted by httr2 in printed output
      req_url_query(
        jql = I(
          # I wrapper skips auto-formatting of the extra "=" sign
          utils::URLencode(
            paste0(
              "project=",
              DASHBOARD_ID
              ,
              " AND Updated >= \"",
              start_time,
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
      ) #|>
      # apply_proxy_if_needed() |>
      # req_error(
      #   is_error = function(resp) {
      #     lr <- resp_header(resp, "x-seraph-loginreason")
      #     bad_auth <- !is.null(lr) &&
      #       grepl("AUTHENTICATED_FAILED|AUTHENTICATION_DENIED", lr)
      #     empty_ok <- FALSE # we only care about bad_auth here
      #     bad_auth || empty_ok
      #   },
      #
      #   body = function(resp) {
      #     paste0(
      #       "Auth Failure for ",
      #       SCRIPT_NAME,
      #       " reason: ",
      #       resp_header(resp, "x-seraph-loginreason") %||% "UNKNOWN",
      #       " traceid: ",
      #       resp_header(resp, "atl-traceid") %||% "NA",
      #       " url: ",
      #       resp_url(resp)
      #     )
      #   }
      # )

    resp <- req_perform(req) |> resp_body_json()
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
      data <- append(data, resp$issues)
    }

    round <- 2
  }
}
