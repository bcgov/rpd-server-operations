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

"K1013092"

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
  "WITH dim_proj AS (
  SELECT project_skey, project_number
  FROM CbreStaging.dim_project
  WHERE project_number = 'K1013092'
  )

  SELECT
  *
  FROM CbreStaging.fact_budget budg
  RIGHT JOIN dim_proj proj
  ON budg.project_skey = proj.project_skey
  WHERE record_type = 'budget'
  "
)

Output <- dbFetch(query, n = -1)
dbClearResult(query)

final <- Output |>
  select(-c(34)) |>
  select_if(~ !all(is.na(.))) |>
  select(
    project_number,
    record_type,
    budget_desc,
    budget_capital_value,
    budget_status
  )

query <- dbSendQuery(
  con,
  "
  SELECT
  *
  FROM RealProperty.ActivityCodeReport
  WHERE ProjectNumber = 'K1013092'
  "
)

Output <- dbFetch(query, n = -1)
dbClearResult(query)

final <- Output |>
  mutate(
    across(
      everything(),
      as.character
    )
  ) |>
  pivot_longer(
    cols = everything(),
    names_to = "column_name",
    values_to = "values"
  )
