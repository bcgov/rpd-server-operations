source(here::here("utilities/R/utilities.R"))
base_url = "https://api.cbre.com:443/"
endpoint_url = "t/digitaltech_us_edp/vantageanalytics/prod/v1/entities/"
start_time = "2010-01-01T00:00:00Z"
end_time = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
username = "kWNLCjcu05JmqrlSsmDtMoSreuUa"
credentials <- keyring::key_get(service = "CBRE_API", username = username)
token <- base64encode(charToRaw(paste0(username, ":", credentials)))
token_string <- paste("Basic", token)

bearer_token <- get_bearer_token(
  base_url = base_url,
  username = username,
  credentials = credentials
)

req <- httr2::request(base_url) |>
  httr2::req_url_path_append(endpoint_url) |>
  httr2::req_auth_bearer_token(bearer_token) |>
  apply_proxy_if_needed() |>
  httr2::req_perform()

resp <- req |> resp_body_json()

entities <- resp |>
  purrr::pluck("entities") |>
  tibble::enframe() |>
  tidyr::unnest_wider(value) |>
  select(-c(name)) |>
  tidyr::unnest_wider(attributes, names_sep = "-") |>
  tidyr::unnest_wider(where(is.list), names_sep = "-") |>
  select(!(ends_with("datatype"))) |>
  arrange(entity)

result <- entities |>
  filter(if_any(-1, ~ . == "location_id"))
