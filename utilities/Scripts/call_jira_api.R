call_jira_api <- function(
  api_name,
  script_name,
  dashboard_id,
  query_url,
  expand_opts,
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
              dashboard_id,
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
        nextPageToken = nextPageToken,
        .multi = "comma"
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
            script_name,
            " reason: ",
            resp_header(resp, "x-seraph-loginreason") %||% "UNKNOWN",
            " traceid: ",
            resp_header(resp, "atl-traceid") %||% "NA",
            " url: ",
            resp_url(resp)
          )
        }
      )

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
          api_name = api_name,
          script_name = script_name,
          table_name = dashboard_id,
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
        start_time,
        " to ",
        format(Sys.time(), tz = "UTC"),
        " UTC"
      )

      cat(no_data_msg, "— nothing to load. Exiting gracefully.\n")

      log_daily_etl_run(
        api_name = api_name,
        script_name = script_name,
        table_name = dashboard_id,
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

    cat("Completed Round: ", round, "\n")
    if (round == 1) {
      data <- resp$issues
      names <- resp$names
    } else {
      data <- append(data, resp$issues)
    }

    round <- round + 1
  }
  list(issues = data, names = names)
}
