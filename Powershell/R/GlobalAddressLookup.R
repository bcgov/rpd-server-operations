source(here::here("renv/activate.R"))

ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "RealProperty"
TABLE_NAME <- "GlobalAddressLookup"
# STAGE_TABLE <- paste0(TABLE_NAME, "_Stage")
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
SCRIPT_NAME <- "GlobalAddressLookup"
API_NAME <- "None"

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

ldaps <- read.csv(here::here("input/Powershell/gal_users.csv"))

output <- ldaps |>
  filter(employeeid != "") |>
  filter(bcgovhrcompany == "GOV")

AddressList <- output |>
  select(linkAddress = streetaddress, linkCity = l, postalcode) |>
  distinct() |>
  mutate(
    geo_name = "",
    score = "",
    precision = ""
  )

# Use geocoder to improve addresses

api_key <- keyring::key_get(
  service = "BCGEOCODER_API",
  keyring = NULL
)

query_url = 'https://geocoder.api.gov.bc.ca/addresses.geojson?addressString='

for (ii in 1:nrow(AddressList)) {
  location <- paste0(
    stringr::str_replace_all(
      AddressList[ii, "linkAddress"],
      c(
        "#" = "",
        "@" = "",
        ";" = "",
        "’" = "",
        "–" = "",
        "[|]" = "",
        "[P][O][\\s+][B][Oo][Xx][\\s+][0-9]+[\\s+][Ss][Tt][Nn][\\s+][Pp][Rr][Oo][Vv][\\s+][Gg][Oo][Vv][Tt]*" = "",
        "\\s+" = "%20"
      )
    ),
    "%20",
    stringr::str_replace_all(
      AddressList[ii, "linkCity"],
      c(
        ";" = "",
        "’" = "",
        "–" = "",
        "[|]" = "",
        "\\s+" = "%20"
      )
    )
  )

  req <- request(paste0(query_url, location)) |>
    req_headers(API_KEY = api_key) |>
    req_perform()
  resp <- req |> resp_body_json()
  AddressList$geo_name[ii] <- resp$features[[1]]$properties$fullAddress
  AddressList$precision[ii] <- resp$features[[1]]$properties$precisionPoints
  AddressList$score[ii] <- resp$features[[1]]$properties$score
}

AddressListFinal <- AddressList |>
  separate_wider_delim(
    geo_name,
    delim = ",",
    names = c("geoAddress", "geoCity", "Province"),
    too_few = "align_start"
  ) |>
  mutate(
    geoAddress = trimws(geoAddress),
    geoCity = trimws(geoCity),
    Province = trimws(Province),
    score = as.numeric(score),
    precision = as.numeric(precision)
  ) |>
  mutate(
    geoAddress = gsub("--", "-", geoAddress),
  ) |>
  mutate(
    Address = case_when(
      score >= 85 & precision >= 99 ~ geoAddress,
      .default = linkAddress
    ),
    City = case_when(
      score >= 85 & precision >= 99 ~ geoCity,
      .default = linkCity
    ),
    GeoFlag = case_when(
      score >= 85 & precision >= 99 ~ TRUE,
      .default = FALSE
    ),
    Prefix = case_when(
      stringr::str_detect(
        Address,
        "(FLR|RM|UNIT|SUITE)[\\s][A-Z]?[0-9]*[A-Z]?[\\s]?[-][\\s]"
      ) ~ stringr::str_extract(
        Address,
        "(FLR|RM|UNIT|SUITE)[\\s][A-Z]?[0-9]+[A-Z]?"
      ),
      .default = NA
    ),
    Address = case_when(
      stringr::str_detect(
        Address,
        "(FLR|RM|UNIT|SUITE)[\\s][A-Z]?[0-9]*[A-Z]?[\\s]?[-][\\s]"
      ) ~ stringr::str_replace(
        Address,
        "(FLR|RM|UNIT|SUITE)[\\s][A-Z]?[0-9]*[A-Z]?[\\s]?[-][\\s]",
        ""
      ),
      .default = Address
    )
  ) |>
  relocate(Prefix, .before = Address)

GlobalAddressLookup <- output |>
  left_join(
    AddressListFinal,
    by = join_by(streetaddress == linkAddress, l == linkCity, postalcode)
  ) |>
  mutate(RefreshDate = as.POSIXct(Sys.time())) |>
  mutate(
    GeoFlag = as.integer(GeoFlag)
  ) |>
  select(
    RefreshDate,
    FirstName = givenname,
    LastName = sn,
    Company = company,
    Department = department,
    Office = physicaldeliveryofficename,
    JobTitle = title,
    Displayname = displayname,
    EmailAddress = mail,
    EmployeeId = employeeid,
    GeoFlag,
    Prefix,
    Address,
    City,
    Score = score,
    Precision = precision,
    LinkAddress = streetaddress,
    linkCity = l,
    linkPostalCode = postalcode,
    bcgovaccountstatus,
    bcgovaccounttype,
    bcgovemploymenttype,
    bcgovhrcity,
    bcgovhrdepartmentid,
    bcgovhrpositionnumber,
    bcgovhrstatus,
    bcgovhrcompany,
    bcgovhrbusinessunit,
    bcgovhrlocationcode,
    mailboxorgcode
  )

# dbRemoveTable(con, Id(schema = SCHEMA_NAME, table = TABLE_NAME))
if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- paste0(
    " CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    TABLE_NAME,
    " (
        RefreshDate               DATETIME2(3)    NOT NULL,
        FirstName                 NVARCHAR(100)   NOT NULL,
        LastName                  NVARCHAR(100)   NOT NULL,
        Company                   NVARCHAR(250)   NULL,
        Department                NVARCHAR(250)   NULL,
        Office                    NVARCHAR(250)   NULL,
        JobTitle                  NVARCHAR(250)   NULL,
        Displayname               NVARCHAR(100)   NOT NULL,
        EmailAddress              NVARCHAR(100)   NOT NULL,
        EmployeeId                NVARCHAR(20)    NOT NULL,
        GeoFlag                   BIT             NULL,
        Prefix                    NVARCHAR(20)    NULL,
        Address                   NVARCHAR(200)   NULL,
        City                      NVARCHAR(100)   NULL,
        Score                     INT             NULL,
        Precision                 INT             NULL,
        LinkAddress               NVARCHAR(200)   NULL,
        linkCity                  NVARCHAR(100)   NULL,
        linkPostalCode            NVARCHAR(40)    NULL,
        bcgovaccountstatus        NVARCHAR(10)    NULL,
        bcgovaccounttype          NVARCHAR(10)    NULL,
        bcgovemploymenttype       NVARCHAR(10)    NULL,
        bcgovhrcity               NVARCHAR(70)    NULL,
        bcgovhrdepartmentid       NVARCHAR(20)    NULL,
        bcgovhrpositionnumber     INT             NULL,
        bcgovhrstatus             NVARCHAR(10)    NULL,
        bcgovhrcompany            NVARCHAR(10)    NULL,
        bcgovhrbusinessunit       NVARCHAR(10)    NULL,
        bcgovhrlocationcode       NVARCHAR(20)    NULL,
        mailboxorgcode            NVARCHAR(10)    NULL
  );
  "
  )

  dbExecute(con, sql)
}

# Database Transaction ####
etl_start_time <- Sys.time()

etl_error <- NULL
# Control database transaction to ensure all steps done together or not at all
dbBegin(con)

tryCatch(
  {
    if (dbExistsTable(con, TEMP_TABLE)) {
      dbRemoveTable(con, TEMP_TABLE)
    }

    # Create temp table to hold new data
    dbExecute(
      con,
      paste0(
        "
    CREATE TABLE  ",
        SCHEMA_NAME,
        ".",
        TEMP_TABLE,
        " (
        RefreshDate               DATETIME2(3)    NOT NULL,
        FirstName                 NVARCHAR(100)   NOT NULL,
        LastName                  NVARCHAR(100)   NOT NULL,
        Company                   NVARCHAR(250)   NULL,
        Department                NVARCHAR(250)   NULL,
        Office                    NVARCHAR(250)   NULL,
        JobTitle                  NVARCHAR(250)   NULL,
        Displayname               NVARCHAR(100)   NOT NULL,
        EmailAddress              NVARCHAR(100)   NOT NULL,
        EmployeeId                NVARCHAR(20)    NOT NULL,
        GeoFlag                   BIT             NULL,
        Prefix                    NVARCHAR(20)    NULL,
        Address                   NVARCHAR(200)   NULL,
        City                      NVARCHAR(100)   NULL,
        Score                     INT             NULL,
        Precision                 INT             NULL,
        LinkAddress               NVARCHAR(200)   NULL,
        linkCity                  NVARCHAR(100)   NULL,
        linkPostalCode            NVARCHAR(40)    NULL,
        bcgovaccountstatus        NVARCHAR(10)    NULL,
        bcgovaccounttype          NVARCHAR(10)    NULL,
        bcgovemploymenttype       NVARCHAR(10)    NULL,
        bcgovhrcity               NVARCHAR(70)    NULL,
        bcgovhrdepartmentid       NVARCHAR(20)    NULL,
        bcgovhrpositionnumber     INT             NULL,
        bcgovhrstatus             NVARCHAR(10)    NULL,
        bcgovhrcompany            NVARCHAR(10)    NULL,
        bcgovhrbusinessunit       NVARCHAR(10)    NULL,
        bcgovhrlocationcode       NVARCHAR(20)    NULL,
        mailboxorgcode            NVARCHAR(10)    NULL
    );
  "
      )
    )

    dbWriteTable(
      con,
      name = TEMP_TABLE,
      value = GlobalAddressLookup,
      append = TRUE,
      overwrite = FALSE
    )

    dbExecute(
      con,
      paste0(
        "DELETE FROM ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        ";"
      )
    )

    n_inserted <- dbExecute(
      con,
      paste0(
        "INSERT INTO ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        "(
        RefreshDate,
        FirstName,
        LastName,
        Company,
        Department,
        Office,
        JobTitle,
        Displayname,
        EmailAddress,
        EmployeeId,
        GeoFlag,
        Prefix,
        Address,
        City,
        Score,
        Precision,
        LinkAddress,
        linkCity,
        linkPostalCode,
        bcgovaccountstatus,
        bcgovaccounttype,
        bcgovemploymenttype,
        bcgovhrcity,
        bcgovhrdepartmentid,
        bcgovhrpositionnumber,
        bcgovhrstatus,
        bcgovhrcompany,
        bcgovhrbusinessunit,
        bcgovhrlocationcode,
        mailboxorgcode
      )
       SELECT * FROM ",
        TEMP_TABLE,
        ";"
      )
    )

    # Complete the transaction
    dbCommit(con)
    #     n_deleted <<- n_deleted
    n_inserted <<- n_inserted
    #     n_updated <<- n_updated
    #     # Rollback transaction on failure
  },
  error = function(e) {
    dbRollback(con)
    etl_error <<- e
    # stop(e)
  }
)

if (is.null(etl_error)) {
  log_daily_etl_run(
    api_name = API_NAME,
    script_name = SCRIPT_NAME,
    table_name = TABLE_NAME,
    status = "SUCCESS",
    n_inserted = n_inserted,
    n_updated = NA,
    n_deleted = NA,
    message = "ETL completed successfully"
  )
} else {
  log_daily_etl_run(
    api_name = API_NAME,
    script_name = SCRIPT_NAME,
    table_name = TABLE_NAME,
    status = "FAILURE",
    message = substr(etl_error$message, 1, 500)
  )
  stop(etl_error)
}
