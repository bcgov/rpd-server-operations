location <- "ƛ̓éxətəm"

query_location <- paste0(
  "70",
  "%20",
  location,
  "%20",
  "Rd",
  "%20",
  "Coquitlam"
)

test_location <- paste0(
  stringr::str_replace_all(AddressList[ii, "linkAddress"], " ", "%20"),
  "%20",
  stringr::str_replace_all(AddressList[ii, "linkCity"], " ", "%20")
)
addr <- "70 ƛ̓éxətəm Rd Coquitlam"
addr <- enc2utf8(addr)

addr <- "2563 šxʷməθkʷəy̓əmasəm Vancouver"
addr <- enc2utf8(addr)

req <- request("https://geocoder.api.gov.bc.ca/addresses.geojson") |>
  req_url_query(addressString = addr) |>
  req_headers(API_KEY = API_KEY) |>
  req_timeout(30) |>
  # req_options(resolve = "geocoder.api.gov.bc.ca:443:142.34.229.4") |>
  # req_retry(
  #   max_tries = 3,
  #   backoff = ~ 10,
  #   is_transient = \(resp) resp_status(resp) %in% c(429, 500, 502, 503, 504)
  # ) |>
  # req_perform(verbosity = 3)
  req_perform()
resp <- req |> resp_body_json()
