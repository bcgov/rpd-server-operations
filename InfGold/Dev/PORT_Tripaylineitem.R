# For server logging
# Begin timer
task_start <- Sys.time()

# Setup necessary variables
ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "RealProperty"
TABLE_NAME <- "PORT_Tripayment"
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
SCRIPT_NAME <- "PORT_Tripayment"
API_NAME <- "None"

options(scipen = 999)
options(digits = 7)

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
source(here::here("utilities/R/utilities.R"))

# Connect to Oracle database
password <- keyring::key_get(service = "IWP", username = "DRATTRAY")
OracleDb <- dbConnect(
  odbc::odbc(),
  driver = "Oracle in instantclient_23_0", # Must match your installed driver name
  DBQ = "IWP.BCGOV",
  host = "pollux.bcgov",
  uid = "DRATTRAY",
  pwd = password,
  port = 1521
)

query <- dbSendQuery(
  OracleDb,
  "
SELECT
T2.triIdTX,
T2.triNameTX,
T2.areAresNumberNU,
T1.triPaymentTypeCL,
T1.triPaymentTypeCLObjId AS
T1_1091_OBJID,
TO_CHAR(
  (TIMESTAMP '1970-01-01 00:00:00' + NUMTODSINTERVAL(T1.areActualDueDateDA / 1000, 'SECOND')),
  'DD-MON-YYYY'
) AS areActualDueDate,
T1.arePaymentScheduleType,
T1.triActualAmountNU,
T1.areNALeasePaymentLI,
T1.triStatusCL,
T1.triStatusCLObjId,
TO_CHAR(
  (TIMESTAMP '1970-01-01 00:00:00' + NUMTODSINTERVAL(t1.tridueda / 1000, 'SECOND')),
  'DD-MON-YYYY'
) AS triDueDA,T1.triRemitToOrganization,
T1.triDescriptionTX,
T1.SYS_TYPE1,
T1.SYS_GUIID AS T1_SYS_GUIID,
T1.SPEC_ID AS T1_SPEC_ID,
T1.triCurrencyUO
FROM TRIDATA.T_TRIPAYMENTLINEITEM T1
LEFT OUTER JOIN TRIDATA.IBS_SPEC_ASSIGNMENTS T3 ON T1.SPEC_ID = T3.SPEC_ID
AND T3.ASS_SPEC_CLASS_TYPE = 21
AND T3.ASS_SPEC_TEMPLATE_ID = 10002490
AND T3.ASS_TYPE = 'Is Payment For'
LEFT OUTER JOIN TRIDATA.T_TRIREALESTATECONTRACT T2 ON T3.ASS_SPEC_ID = T2.SPEC_ID
AND T2.SYS_OBJECTID > 0
WHERE ( T1.triAccountingTypeLI = 'Accounts Payable (AP)'
        AND UPPER(T2.areContractTypeLI) LIKE '%OWNED%'
        AND T1.triStatusCL IN ('Paid','Accepted','Rejected','Appropriated') )
AND T1.SYS_OBJECTID > 0
AND T2.SYS_GUIID = 10017882
AND T2.SYS_PROJECTID = 1
ORDER BY T1.areActualDueDateDA DESC, UPPER(T1.triPaymentTypeCL)"
)
TripayData <- dbFetch(query, n = -1)
dbClearResult(query)

tripay <- TripayData |>
  select(
    -c(
      TRIIDTX,
      AREARESNUMBERNU,
      ARENALEASEPAYMENTLI,
      TRISTATUSCL,
      TRISTATUSCLOBJID,
      T1_1091_OBJID,
      T1_SPEC_ID,
      T1_SYS_GUIID,
      TRICURRENCYUO,
      SYS_TYPE1,
      AREPAYMENTSCHEDULETYPE
    )
  ) |>
  rename_with(~ stringr::str_to_title(tolower(.)), .cols = everything()) |>
  filter(Trinametx != "*NONPROP") |>
  mutate(
    PaidDate = as.Date(Areactualduedate, format = "%d-%b-%Y"),
    .after = Trinametx
  ) |>
  filter(PaidDate > "2020-03-31") |>
  rename(
    Identifier = Trinametx,
    PaymentType = Tripaymenttypecl,
    AmountPaid = Triactualamountnu,
    OrgPaid = Triremittoorganization
  ) |>
  mutate(
    FiscalYear = case_when(
      PaidDate |> timetk::between_time('2024-04-01', '2025-03-31') ~ "FY2425",
      PaidDate |> timetk::between_time('2023-04-01', '2024-03-31') ~ "FY2324",
      PaidDate |> timetk::between_time('2022-04-01', '2023-03-31') ~ "FY2223",
      PaidDate |> timetk::between_time('2021-04-01', '2022-03-31') ~ "FY2122",
      PaidDate |> timetk::between_time('2020-04-01', '2021-03-31') ~ "FY2021"
    ),
    .after = PaidDate
  ) |>
  group_by(Identifier, FiscalYear, PaymentType) |>
  summarise(TotalPaid = sum(AmountPaid)) |>
  ungroup() |>
  filter(PaymentType %in% c("O&M Total", "Utilities")) |>
  pivot_wider(names_from = PaymentType, values_from = TotalPaid) |>
  rename(OnMTotal = `O&M Total`) |>
  mutate(RefreshDate = as.POSIXct(Sys.Date()), .before = everything())

# dbRemoveTable(con, Id(schema = SCHEMA_NAME, table = TABLE_NAME))
if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- paste0(
    " CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    TABLE_NAME,
    " (
    RefreshDate             DATETIME2(3)    NOT NULL,
    Identifier              NVARCHAR(20)    NOT NULL,
    FiscalYear              NVARCHAR(10)    NOT NULL,
    OnMTotal                INT             NULL,
    Utilities               INT             NULL
  );
  "
  )

  dbExecute(con, sql)
}

# Database Transaction ####
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
          RefreshDate             DATETIME2(3)    NOT NULL,
          Identifier              NVARCHAR(50)    NOT NULL,
          FiscalYear              NVARCHAR(10)    NOT NULL,
          OnMTotal                INT             NULL,
          Utilities               INT             NULL
          );
  "
      )
    )

    dbWriteTable(
      con,
      name = TEMP_TABLE,
      value = tripay,
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
        Identifier,
        FiscalYear,
        OnMTotal,
        Utilities
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
    # rollback transaction on fail, completion of error handling
  },
  error = function(e) {
    dbRollback(con)
    stop(e)
  }
)

task_end <- Sys.time()
task_duration <- interval(task_start, task_end) / dseconds()

if (is.null(etl_error)) {
  log_daily_etl_run(
    api_name = API_NAME,
    script_name = SCRIPT_NAME,
    table_name = TABLE_NAME,
    duration = task_duration,
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
