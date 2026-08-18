api_name <- "Jira"
script_name <- "Jira_SBP"
dashboard_id <- "SBP"
# query_url from orchestrator
expand_opts <- c("changelog", "names", "fields")
# max_results keep default
# token_string from orchestrator
start_time <- etl_window$jira_start_time

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
        etl_window$jira_start_time,
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

output <- call_jira_api(
  api_name,
  schema_name,
  dashboard_id,
  query_url,
  expand_opts,
  max_results = 100,
  token_string,
  start_time
)

names <- output |>
  purrr::pluck("names") |>
  tibble::enframe() |>
  safe_hoist(value, Value = 1L) |>
  group_by(Value) |>
  mutate(row_name = row_number(), row_count = n()) |>
  mutate(
    Value = case_when(
      row_count > 1 ~ paste0(Value, "-", row_name),
      .default = Value
    )
  ) |>
  select(-c(row_name, row_count)) |>
  tibble::deframe()


issues <- output |>
  purrr::pluck("issues") |>
  tibble::enframe() |>
  tidyr::unnest_wider(value) |>
  tidyr::unnest_wider(fields) |>
  plyr::rename(names) |>
  rename_with(~ gsub(" ", "", .)) |>
  # Parent column is sometimes missing as sparsely populated
  mutate(
    Parent = if ("Parent" %in% names(pick(everything()))) Parent else NA
  ) |>
  # Select fields of interest
  select(
    IssueKey = key,
    IssueType,
    Address,
    Assignee,
    Created,
    RequestedDueDate,
    SpaceBookingAdmin = `NameofSpaceBookingAdmin`,
    NumberOfSpaces = `NumberofSpacestoOnboard`,
    FloorPlan = `Doyouhaveafloorplan?`,
    FurniturePlan = `Doyouhaveafurnitureplan?`,
    LastUpdatedStatus,
    Department = `Department-1`,
    DueDate = Duedate,
    Organization = `Ministry/BPSOrganization`,
    Priority,
    Reporter,
    RequestParticipants = Requestparticipants,
    RequestType,
    Resolved,
    Status,
    Summary,
    Updated,
    Parent,
    changelog
  ) |>
  safe_hoist(IssueType, IssueType = "name", .remove = FALSE) |>
  safe_hoist(
    Address,
    Address = list("content", 1L, "content", 1L, "text"),
    .remove = FALSE
  ) |>
  safe_hoist(Assignee, Assignee = "displayName", .remove = FALSE) |>
  safe_hoist(
    RequestedDueDate,
    RequestedDueDate = "value",
    .remove = FALSE
  ) |>
  safe_hoist(FloorPlan, FloorPlan = "value", .remove = FALSE) |>
  safe_hoist(FurniturePlan, FurniturePlan = "value", .remove = FALSE) |>
  safe_hoist(Organization, Organization = "value", .remove = FALSE) |>
  safe_hoist(Priority, Priority = "name", .remove = FALSE) |>
  safe_hoist(Reporter, Reporter = "displayName", .remove = FALSE) |>
  safe_hoist_all(
    RequestParticipants,
    RequestParticipants = list("displayName"),
    .remove = FALSE
  ) |>
  # mutate(
  #   RequestParticipants = RequestParticipants_displayName,
  #   .keep = "unused"
  # ) |>
  safe_hoist(
    RequestType,
    RequestType = list("requestType", "name"),
    .remove = FALSE
  ) |>
  safe_hoist(Status, Status = "name", .remove = FALSE) |>
  safe_hoist(Parent, Parent = "key", .remove = FALSE)
