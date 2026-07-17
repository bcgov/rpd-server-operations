query_url = "https://citz-rpd.atlassian.net/rest/api/3/dashboard"
# https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-search/#api-rest-api-3-search-jql-get
# https://developer.atlassian.com/changelog/#CHANGE-2046

test_url <- paste0(base_url, "dashboard")
req <- request(test_url) |>
  req_headers(Authorization = token_string) |>
  req_perform()

if (base_url != stringr::str_extract(req$url, ".+3/")) {}
resp <- req |> resp_body_json()

req <- request(base_url) |>
  req_headers(Authorization = token_string) |>
  req_url_path_append("dashboard")
# configure project, max_results, and start_at
req_url_query(
  # jql = I(paste0("project=", DASHBOARD_ID)), # I wrapper skips auto-formatting of the extra "=" sign
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
  req_perform()
