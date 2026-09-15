ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "RealProperty"

# Load libraries
library(base64enc, quietly = TRUE, warn.conflicts = FALSE)
library(dplyr, quietly = TRUE, warn.conflicts = FALSE)
library(here, quietly = TRUE, warn.conflicts = FALSE)
library(httr2, quietly = TRUE, warn.conflicts = FALSE)
library(jsonlite, quietly = TRUE, warn.conflicts = FALSE)
library(lubridate, quietly = TRUE, warn.conflicts = FALSE)
library(purrr, quietly = TRUE, warn.conflicts = FALSE)
library(tibble, quietly = TRUE, warn.conflicts = FALSE)
library(tidyr, quietly = TRUE, warn.conflicts = FALSE)
library(stringr, quietly = TRUE, warn.conflicts = FALSE)
library(openxlsx2, quietly = TRUE, warn.conflicts = FALSE)
library(odbc, quietly = TRUE, warn.conflicts = FALSE)
library(DBI, quietly = TRUE, warn.conflicts = FALSE)

options(scipen = 999)
options(digits = 7)

# Load helper functions
source(here::here("./utilities/R/event_logger.R"))
source(here::here("./utilities/R/sql_helper_functions.R"))

# Connect to SQL database
con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = SQL_SERVER,
  database = DB_NAME,
  Trusted_Connection = "Yes"
)

etFile <- list.files(
  here::here("input"),
  pattern = "Employee_Telework"
) |>
  sort(decreasing = TRUE)

hqFile <- list.files(
  here::here("input"),
  pattern = "HQ_Telework"
) |>
  sort(decreasing = TRUE)

Employee_Telework_Parquet <- arrow::read_parquet(here(
  paste0("input/", etFile[1])
))

HQ_Telework_Parquet <- arrow::read_parquet(here(
  paste0("input/", etFile[1])
))

EmployeeAddressList <- Employee_Telework_Parquet |>
  select(Start, Match) |>
  filter(Match != "No Match") |>
  distinct() |>
  mutate(
    geo_name = "",
    address_type = "",
    score = "",
    precision = "",
    lat = "",
    lon = ""
  )

# Use geocoder to improve addresses
API_KEY = read.csv("C:/Projects/credentials/bc_geocoder_api_key.csv") |> pull()
query_url = 'https://geocoder.api.gov.bc.ca/addresses.geojson?addressString='

for (ii in 1:nrow(EmployeeAddressList)) {
  location <- paste0(
    str_replace_all(EmployeeAddressList[ii, "Match"], " ", "%20")
  )
  req <- request(paste0(query_url, location)) |>
    req_headers(API_KEY = API_KEY) |>
    req_perform()
  resp <- req |> resp_body_json()
  EmployeeAddressList$lon[ii] <- resp$features[[1]]$geometry$coordinates[[1]]
  EmployeeAddressList$lat[ii] <- resp$features[[1]]$geometry$coordinates[[2]]
  EmployeeAddressList$geo_name[ii] <- resp$features[[1]]$properties$fullAddress
  EmployeeAddressList$address_type[ii] <- resp$features[[
    1
  ]]$properties$matchPrecision
  EmployeeAddressList$precision[ii] <- resp$features[[
    1
  ]]$properties$precisionPoints
  EmployeeAddressList$score[ii] <- resp$features[[1]]$properties$score
}

EmployeeAddressListFinal <- EmployeeAddressList |>
  separate_wider_delim(
    geo_name,
    delim = ",",
    names = c("geo_Street", "geo_City", "Province"),
    too_few = "align_start"
  ) |>
  mutate(
    geo_Street = trimws(geo_Street),
    geo_City = trimws(geo_City),
    Province = trimws(Province),
    score = as.numeric(score),
    precision = as.numeric(precision)
  ) |>
  mutate(
    geo_Street = gsub("--", "-", geo_Street),
  ) |>
  mutate(
    BestAddress = case_when(
      score >= 85 & precision >= 99 ~ geo_Street,
      .default = Match
    ),
    BestCity = case_when(
      score >= 85 & precision >= 99 ~ geo_City,
      .default = NA
    )
  )

Employee_Telework_Table <- Employee_Telework_Parquet |>
  left_join(EmployeeAddressListFinal, by = join_by(Match, Start)) |>
  relocate(Start, Match, BestAddress, BestCity, .after = ImportDate) |>
  select(-c(Province, address_type)) |>
  mutate(
    BestAddress = case_when(is.na(BestAddress) ~ Match, .default = BestAddress)
  ) |>
  mutate(Address = BestAddress, AddressEdit = Match, .after = EmailAddress) |>
  select(
    -c(
      BestAddress,
      BestCity,
      geo_Street,
      geo_City,
      lat,
      lon,
      `__index_level_0__`
    )
  )


HQ_Telework_Table <- HQ_Telework_Parquet |>
  left_join(EmployeeAddressListFinal, by = join_by(Match, Start)) |>
  relocate(Start, Match, BestAddress, BestCity, .after = ImportDate) |>
  mutate(
    BestAddress = case_when(is.na(BestAddress) ~ Match, .default = BestAddress)
  ) |>
  mutate(Address = BestAddress, AddressEdit = Match, .after = EmailAddress) |>
  select(
    -c(
      BestAddress,
      BestCity,
      geo_Street,
      geo_City,
      lat,
      lon,
      Province,
      address_type,
      `__index_level_0__`
    )
  )

dbWriteTable(
  con,
  name = Id(SCHEMA_NAME, "EmployeeTelework"),
  value = Employee_Telework_Table,
  append = FALSE,
  overwrite = TRUE
)

dbWriteTable(
  con,
  name = Id(SCHEMA_NAME, "HQTelework"),
  value = HQ_Telework_Table,
  append = FALSE,
  overwrite = TRUE
)

R_Bridge_Address_Table <- Employee_Telework_Table |>
  select(Address) |>
  distinct() |>
  full_join(
    HQ_Telework_Table |> select(Address) |> distinct(),
    by = join_by(Address)
  )

dbWriteTable(
  con,
  name = Id(SCHEMA_NAME, "BridgeAddress"),
  value = R_Bridge_Address_Table,
  append = FALSE,
  overwrite = TRUE
)
