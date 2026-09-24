# 1. Can you resolve the host at all?
nslookup <- system("nslookup geocoder.api.gov.bc.ca", intern = TRUE)
cat(nslookup, sep = "\n")

# 2. Try with a longer timeout to rule out a slow response vs hard down
req <- request(paste0(query_url, location)) |>
  req_headers(API_KEY = API_KEY) |>
  req_timeout(60) |>
  req_perform(verbosity = 3)

Sys.setenv(HTTPS_PROXY = "142.34.229.249:8080")
Sys.unsetenv("HTTPS_PROXY")

system("curl -v --max-time 10 telnet://geocoder.api.gov.bc.ca:80", intern = TRUE)

# Try HTTP instead of HTTPS (unlikely to work but rules out TLS-specific issue)
system("curl -v --max-time 10 http://geocoder.api.gov.bc.ca/addresses.geojson?addressString=test", intern = TRUE)

# Try hitting the IP directly, bypassing DNS
system("curl -v --max-time 10 --resolve geocoder.api.gov.bc.ca:443:142.34.64.4 https://geocoder.api.gov.bc.ca/addresses.geojson?addressString=test", intern = TRUE)

# Check if another gov.bc.ca API is reachable from Muon (e.g. data.gov.bc.ca)
system("curl -s --max-time 10 https://data.gov.bc.ca -o /dev/null -w '%{http_code}'", intern = TRUE)

library(httr2)

ii = 1
location <- paste0(
  stringr::str_replace_all(AddressList[ii, "linkAddress"], " ", "%20"),
  "%20",
  stringr::str_replace_all(AddressList[ii, "linkCity"], " ", "%20")
)
req <- request(paste0(query_url, location)) |>
  req_headers(API_KEY = API_KEY) |>
  req_timeout(30) |>
  req_retry(
    max_tries = 5,
    backoff = ~ 5  # wait 5 seconds between retries
  ) |>
  req_options(resolve = "geocoder.api.gov.bc.ca:443:142.34.229.4")

resp <- req_perform(req, verbosity = 3)

# 142.34.229.4
