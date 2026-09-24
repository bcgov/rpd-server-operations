apply_proxy_if_needed <- function(
  req,
  etl_env = Sys.getenv("ETL_ENV", unset = "UNKNOWN")
) {
  if (etl_env == "Muon") {
    req <- req |> httr2::req_proxy("142.34.229.249", 8080)
  }
  req
}
