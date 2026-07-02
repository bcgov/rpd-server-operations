# For server logging
# Begin timer
task_start <- Sys.time()

# Load helper functions
source(here::here("utilities/R/utilities.R"))

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

# Setup necessary variables
ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "RealProperty"
TABLE_NAME <- "PORT_PRR2015"
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
SCRIPT_NAME <- "PORT_PRR2015"
API_NAME <- "None"

# Connect to SQL database
con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = SQL_SERVER,
  database = DB_NAME,
  Trusted_Connection = "Yes"
)

# Query SQL Datasets ####
query <- dbSendQuery(
  con,
  "SELECT
   property_skey AS property_skey,
   address_line1 AS com_address,
   client_property_name AS com_client_property_name,
   client_property_id AS com_client_property_id,
   alternate_property_id AS com_alternate_property_id
   FROM CbreStaging.com_dim_property"
)

ComDimProperty <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(
  con,
  "SELECT
   DISTINCT property_skey
   FROM CbreStaging.es_fact_invoice"
)
EsFactInvoice <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(
  con,
  "SELECT
   property_skey,
   client_property_id,
   client_property_name,
   Identifier,
   address_line1
   FROM CbreStaging.dim_property"
)
DimProperty <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(
  con,
  "SELECT
   *
   FROM CbreStaging.dim_property"
)
DimFullProperty <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(
  con,
  "SELECT * FROM RealProperty.FacilityDetail"
)
FacilityDetail <- dbFetch(query, n = -1)
dbClearResult(query)

test <- EsFactInvoice |>
  left_join(ComDimProperty, by = join_by(property_skey)) |>
  left_join(
    DimProperty,
    by = join_by(com_alternate_property_id == client_property_id)
  ) |>
  mutate(
    Unified_Identifer = case_when(
      is.na(Identifier) ~ com_alternate_property_id, # cases where this is an RPD### value, need fix
      .default = Identifier
    ),
    envizi_property_skey = property_skey.x,
    archibus_property_skey = property_skey.y,
    .before = everything()
  )

test2 <- test |>
  filter(!startsWith(Unified_Identifer, "B"))

test3 <- test2 |>
  select(-c(property_skey.x, property_skey.y)) |>
  left_join(
    ComFullProperty,
    by = join_by(com_client_property_id == client_property_id)
  )
# Need to get by building,
# RentableArea, ParkingStalls, BaseRent, O&M, Utilities, Admin (all kinds), LLO&M,

# mirror for B0027003

# Build property and building sets.

query <- dbSendQuery(
  con,
  "SELECT
  *
  FROM CbreStaging.com_dim_property"
)

ComFullProperty <- dbFetch(query, n = -1)
dbClearResult(query)

ComSet <- ComFullProperty |>
  select(
    property_skey,
    com_address_line1 = address_line1,
    com_client_property_name = client_property_name,
    com_client_property_id = client_property_id,
    com_source_system_code = source_system_code
  )

query <- dbSendQuery(
  con,
  "SELECT
   *
   FROM CbreStaging.dim_property"
)
DimFullProperty <- dbFetch(query, n = -1)
dbClearResult(query)

DimPropSet <- DimFullProperty |>
  select(
    property_skey,
    Identifier,
    dim_address_line1 = address_line1,
    dim_client_property_name = client_property_name,
    dim_client_property_id = client_property_id
  )

test_skey_join <- ComSet |>
  full_join(DimPropSet, by = join_by(property_skey)) |>
  mutate(
    dim_address_line1 = str_replace(
      dim_address_line1,
      "\\s{2,}",
      NA_character_
    ),
    Identifier = na_if(Identifier, "")
  )

subset <- test_skey_join |>
  filter(!is.na(Identifier))

subset_na <- test_skey_join |>
  filter(is.na(Identifier))

# 100% fails to join by property_skey
subset_envizi <- test_skey_join |>
  filter(com_source_system_code == "Envizi")

# fix by joining on client_property_id? -- Nope
subset_envizi_client_id <- subset_envizi |>
  select(
    com_property_skey = property_skey,
    com_address_line1,
    com_client_property_name,
    com_client_property_id,
    com_source_system_code
  ) |>
  left_join(
    DimPropSet,
    by = join_by(com_client_property_id == dim_client_property_id)
  )

envizi_full_set <- ComFullProperty |>
  filter(source_system_code == "Envizi")
#  only one row falls into this condition, but it links of property_skey and has a populated Identifier
# subset_no_com <- test_skey_join |>
#   filter(is.na(com_source_system_code))
range(envizi_full_set$property_skey)
# [1] "20725346" "26083602"
range(DimFullProperty$property_skey)
# [1] "14418084" "16337411"

query <- dbSendQuery(
  con,
  "SELECT property_skey, Identifier FROM CbreStaging.fm_dim_property_extended_attribute;"
)
DimProperty <- dbFetch(query, n = -1)
dbClearResult(query)

range(DimProperty$property_skey)
range(DimProperty$property_skey)
# [1] "14418084" "27779682"

test_envizi <- envizi_full_set |>
  left_join(DimProperty, by = join_by(property_skey))


test_client_id <- ComFullProperty |>
  select(
    property_skey,
    address_line1,
    client_property_name,
    client_property_id
  ) |>
  left_join(DimFullProperty, by = join_by(client_property_id)) |>
  filter(!(is.na(address_line1.y) | grepl("\\s{2,}", address_line1.y)))


query <- dbSendQuery(
  con,
  "SELECT DISTINCT property_skey
  FROM CbreStaging.es_fact_invoice;"
)
EsInvoice <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(
  con,
  "SELECT
  *
  FROM CbreStaging.com_dim_property
  "
)

ComFullProperty <- dbFetch(query, n = -1)
dbClearResult(query)

EsInvoiceSet <- EsInvoice |>
  left_join(ComFullProperty, by = join_by(property_skey)) |>
  select(
    property_skey,
    property_status,
    address_line1,
    client_property_name,
    client_property_id,
    alternate_property_id,
    reporting_code_1,
    reporting_code_2,
    reporting_code_3,
    reporting_code_4,
    reporting_code_5,
    reporting_code_6,
    reporting_code_7
  )

query <- dbSendQuery(
  con,
  "SELECT
   property_skey,
   client_property_id,
   Identifier,
   address_line1,
   city_name,
   location_id,
   source_unique_id
   FROM CbreStaging.dim_property"
)
DimFullSetProperty <- dbFetch(query, n = -1)
dbClearResult(query)

# Fails to join anything
# EsInvoiceSetTwo <- EsInvoiceSet |>
#   left_join(DimFullProperty, by = join_by(property_skey)) |>
#   # filter(!is.na(client_property_id.y))
#   filter(!is.na(Identifier))

# Nope
# EsInvoiceSetTwo <- EsInvoiceSet |>
#   left_join(DimFullProperty, by = join_by(client_property_id))

EsInvoiceSetTwo <- EsInvoiceSet |>
  left_join(
    DimFullSetProperty,
    by = join_by(alternate_property_id == client_property_id)
  ) |>
  select(
    envizi_property_skey = property_skey.x,
    com_property_skey = property_skey.y,
    Identifier,
    property_status,
    client_property_name,
    client_property_id,
    alternate_property_id,
    envizi_address_line1 = address_line1.x,
    com_address_line1 = address_line1.y,
    city_name,
    location_id,
    reporting_code_1,
    reporting_code_2,
    reporting_code_3,
    reporting_code_4,
    reporting_code_5,
    reporting_code_6,
    reporting_code_7
  )

EsInvoiceSetTwoCleaned <- EsInvoiceSetTwo |>
  mutate(Identifier = na_if(Identifier, "")) |>
  mutate(
    Identifier = case_when(
      is.na(Identifier) &
        startsWith(alternate_property_id, "B") ~ alternate_property_id,
      .default = Identifier
    )
  )

EsInvoiceMissingId <- EsInvoiceSetTwoCleaned |>
  filter(is.na(Identifier)) |>
  left_join(ComFullProperty, join_by(com_property_skey == property_skey))

# Check if missing ones are outside a reasonable date range
query <- dbSendQuery(
  con,
  "SELECT *
  FROM CbreStaging.es_fact_invoice;"
)
EsInvoiceFull <- dbFetch(query, n = -1)
dbClearResult(query)

subset_prop_keys <- subset |>
  select(property_skey) |>
  distinct() |>
  mutate(Source = "DateRangeRestriction")

test <- EsInvoiceMissingId |>
  left_join(
    subset_prop_keys,
    by = join_by(envizi_property_skey == property_skey)
  )
# nope
