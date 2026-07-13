ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "RealProperty"
TABLE_NAME <- "Tripayment"
STAGE_TABLE <- paste0(TABLE_NAME, "_Stage")
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)

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


# Connect to SQL database
con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = SQL_SERVER,
  database = DB_NAME,
  Trusted_Connection = "Yes"
)

query <- dbSendQuery(con, "SELECT * FROM RealProperty.FacilityDetail")
FacilityDetail <- dbFetch(query, n = -1)
dbClearResult(query)

# Connect to SQL database
OracleDb <- dbConnect(
  odbc(),
  driver = "Oracle in instantclient_23_0",
  Host = "your_host",
  SVC = "your_service_name",
  UID = "your_username",
  PWD = "your_password",
  Port = 1521
)

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


dbGetQuery(con, "SELECT * FROM dual")


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

tripayFile <- list.files(
  here::here("input"),
  pattern = "tripay"
) |>
  sort(decreasing = TRUE)

tripaymentlineitem <- readr::read_csv(here(
  "input/",
  tripayFile[1]
))

tripay <- tripaymentlineitem |>
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
  rename_with(~ toTitleCase(tolower(.)), .cols = everything()) |>
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
  pivot_wider(names_from = PaymentType, values_from = TotalPaid)

dbWriteTable(
  con,
  name = Id(SCHEMA_NAME, TABLE_NAME),
  value = tripay,
  append = FALSE,
  overwrite = TRUE
)
