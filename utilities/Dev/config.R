# Handy functions to note environment that code is running in

get_env <- function() {
  env <- Sys.getenv("ETL_ENV")
  if (env == "") {
    warning("ETL_ENV not set in .Renviron — defaulting to 'dev'")
    return("dev")
  }
  env
}

is_prod <- function() get_env() == "prod"

db_name <- function() {
  if (is_prod()) "Dynamo" else "Windfarm"
}

get_credential <- function(service, username = NULL) {
  credential <- keyring::key_get(service, username)
}
