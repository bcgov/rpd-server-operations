# For server logging
# Begin timer
task_start <- Sys.time()

# Set necessary variables
ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "Chefs"
FORM <- "Drawings_Request_Submission"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = FORM)
TEMP_TABLE <- paste0("#", FORM, "Temp")
API_NAME <- "CHEFS"
SCRIPT_NAME <- "CHEFS_Drawings_Request_Submission"
base_url <- "https://submit.digital.gov.bc.ca/app/api/v1/"
formId <- "a3d13d43-3b29-4d3b-b950-c7a580790643"
username <- "a3d13d43-3b29-4d3b-b950-c7a580790643"

# Connect to SQL database
con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = SQL_SERVER,
  database = DB_NAME,
  Trusted_Connection = "Yes"
)

# Retrieve API Key
api_key <- keyring::key_get(
  service = "CHEFS_API",
  username = username,
  keyring = NULL
)

# Main query
req <- request(base_url) |>
  req_url_path_append(c("forms", formId, "export")) |>
  req_url_query(
    format = "json",
    type = "submissions"
  ) |>
  req_auth_basic(username, api_key) |>
  req_headers(Accept = "application/json") |>
  req_perform()

resp <- req |> resp_body_json(check_type = FALSE)

clean_data <- resp |>
  tibble::enframe() |>
  tidyr::unnest_wider("value") |>
  tidyr::unnest_wider(drawingType) |>
  tidyr::unnest_wider(requestorInofrmation) |>
  tidyr::unnest_wider(
    doTheseFilesContainInformationThatIfCompromisedCouldCauseExtremelyGraveInjuryToAnIndividualOrganizationOrGovernment
  ) |>
  # can we get these two into a safe_hoist_all() single call?
  safe_hoist(
    form,
    SubmissionId = list("submissionId"),
    .remove = FALSE
  ) |>
  safe_hoist(
    form,
    SubmissionTime = list("submittedAt"),
    .remove = FALSE
  ) |>
  safe_hoist(
    form,
    Status = list("status"),
    .remove = FALSE
  ) |>
  safe_hoist(
    File,
    File = list(1L, "originalName"),
    .remove = FALSE
  ) |>
  safe_hoist(
    bcaddress,
    bcaddress = list("properties", "fullAddress"),
    .remove = FALSE
  ) |>
  safe_hoist(
    bcaddress1,
    bcaddress1 = list("properties", "fullAddress"),
    .remove = FALSE
  ) |>
  mutate(
    ProjectName = case_when(
      projectName == "" ~ projectName1,
      .default = projectName
    ),
    Building = case_when(
      building == "" ~ building1,
      .default = building
    ),
    BuildingName = case_when(
      buildingName == "" ~ buildingName1,
      .default = buildingName
    ),
    Property = case_when(
      property == "" ~ property1,
      .default = property
    ),
    Address = case_when(
      bcaddress == "" ~ bcaddress1,
      .default = bcaddress
    ),
    ProjectNumberOrWorkOrder = case_when(
      projectOrWo == "" ~ projectOrWo1,
      .default = projectOrWo
    ),
    RequestorName = case_when(
      requesterName == "" ~ requesterName1,
      .default = requesterName
    ),
    RequestorPhoneNumber = case_when(
      requesterPhoneNumber == "" ~ requesterPhoneNumber1,
      .default = requesterPhoneNumber
    ),
    RequestorEmail = case_when(
      requesterEmail == "" ~ requesterEmail1,
      .default = requesterEmail
    ),
    RequestorInformation = case_when(
      rowSums(across(c(RPD, CBRE, contractor))) >= 2 ~ "Multiple",
      RPD == "TRUE" ~ "RPD",
      CBRE == "TRUE" ~ "CBRE",
      contractor == "TRUE" ~ "Contractor"
    )
  ) |>
  mutate(
    DateRequired = as.POSIXct(dateRequired, format = "%Y-%m-%dT%H:%M:%OS")
  ) |>
  select(
    SubmissionId,
    SubmissionTime,
    Status,
    ProjectName,
    ProjectNumberOrWorkOrder,
    Building,
    BuildingName,
    Property,
    Address,
    Description = description,
    File,
    DrawingType_Civil = civil,
    DrawingType_Architectural = architectural,
    DrawingType_Electrical = electrical,
    DrawingType_FireProtection = fireProtection,
    DrawingType_Mechanical = mechanical,
    DrawingType_Other = other,
    DrawingType_Plumbing = plumbing,
    DrawingType_Structural = structural,
    DrawingType_Telecommunications = telecommunications,
    SensitiveFile_Yes = yes,
    SensitiveFile_No = no,
    SensitiveFile_Unsure = unsure,
    RequestorName,
    RequestorEmail,
    RequestorPhoneNumber,
    RequestorInformation,
    DateRequired
  ) |>
  mutate(
    SubmissionTime = as.POSIXct(SubmissionTime, format = "%Y-%m-%dT%H:%M:%OS")
  ) |>
  mutate(RefreshDate = Sys.time(), .before = everything()) |>
  mutate(across(where(is.character), ~ na_if(., "")))

# Start database transaction ####
# dbRemoveTable(con, TARGET_TABLE)
if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- paste0(
    "CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    FORM,
    " (
        RefreshDate                    DATETIME2(3)     NOT NULL,
        SubmissionId                   NVARCHAR(36)     NOT NULL,
        SubmissionTime                 DATETIME2(3)     NOT NULL,
        Status                         NVARCHAR(30)     NULL,
        ProjectName                    NVARCHAR(1500)   NULL,
        ProjectNumberOrWorkOrder       NVARCHAR(1500)   NULL,
        Building                       NVARCHAR(250)    NULL,
        BuildingName                   NVARCHAR(250)    NULL,
        Property                       NVARCHAR(200)    NULL,
        Address                        NVARCHAR(500)    NULL,
        Description                    NVARCHAR(3000)   NULL,
        [File]                         NVARCHAR(500)    NULL,
        DrawingType_Civil              BIT              NULL,
        DrawingType_Architectural      BIT              NULL,
        DrawingType_Electrical         BIT              NULL,
        DrawingType_FireProtection     BIT              NULL,
        DrawingType_Mechanical         BIT              NULL,
        DrawingType_Other              BIT              NULL,
        DrawingType_Plumbing           BIT              NULL,
        DrawingType_Structural         BIT              NULL,
        DrawingType_Telecommunications BIT              NULL,
        SensitiveFile_Yes              BIT              NULL,
        SensitiveFile_No               BIT              NULL,
        SensitiveFile_Unsure           BIT              NULL,
        RequestorName                  NVARCHAR(200)    NULL,
        RequestorEmail                 NVARCHAR(200)    NULL,
        RequestorPhoneNumber           NVARCHAR(50)     NULL,
        RequestorInformation           NVARCHAR(50)     NULL,
        DateRequired                   DATETIME2(3)     NULL
      );"
  )
  dbExecute(con, sql)
}

etl_error <- NULL

# Control database transaction to ensure all steps done together or not at all
dbBegin(con)

# Begin error handling and rollback of transaction on failure
tryCatch(
  {
    if (dbExistsTable(con, TEMP_TABLE)) {
      dbRemoveTable(con, TEMP_TABLE)
    }

    dbExecute(
      con,
      paste0(
        "CREATE TABLE ",
        TEMP_TABLE,
        " (
          RefreshDate                    DATETIME2(3)     NOT NULL,
          SubmissionId                   NVARCHAR(36)     NOT NULL,
          SubmissionTime                 DATETIME2(3)     NOT NULL,
          Status                         NVARCHAR(30)     NULL,
          ProjectName                    NVARCHAR(1500)   NULL,
          ProjectNumberOrWorkOrder       NVARCHAR(1500)   NULL,
          Building                       NVARCHAR(250)    NULL,
          BuildingName                   NVARCHAR(250)    NULL,
          Property                       NVARCHAR(200)    NULL,
          Address                        NVARCHAR(500)    NULL,
          Description                    NVARCHAR(3000)   NULL,
          [File]                         NVARCHAR(500)    NULL,
          DrawingType_Civil              BIT              NULL,
          DrawingType_Architectural      BIT              NULL,
          DrawingType_Electrical         BIT              NULL,
          DrawingType_FireProtection     BIT              NULL,
          DrawingType_Mechanical         BIT              NULL,
          DrawingType_Other              BIT              NULL,
          DrawingType_Plumbing           BIT              NULL,
          DrawingType_Structural         BIT              NULL,
          DrawingType_Telecommunications BIT              NULL,
          SensitiveFile_Yes              BIT              NULL,
          SensitiveFile_No               BIT              NULL,
          SensitiveFile_Unsure           BIT              NULL,
          RequestorName                  NVARCHAR(200)    NULL,
          RequestorEmail                 NVARCHAR(200)    NULL,
          RequestorPhoneNumber           NVARCHAR(50)     NULL,
          RequestorInformation           NVARCHAR(50)     NULL,
          DateRequired                   DATETIME2(3)     NULL
          );"
      )
    )

    dbWriteTable(
      con,
      name = TEMP_TABLE,
      value = clean_data,
      append = TRUE,
      overwrite = FALSE
    )

    # -- Guard: catch duplicate keys in source data before touching target --
    dup_count <- dbGetQuery(
      con,
      paste0(
        "SELECT COUNT(*) AS n
         FROM (
           SELECT SubmissionId
           FROM ",
        TEMP_TABLE,
        "
           GROUP BY SubmissionId
           HAVING COUNT(*) > 1
         ) dupes;"
      )
    )$n

    if (dup_count > 0) {
      stop(paste0(
        "Duplicate SubmissionId values detected in source data (",
        dup_count,
        " keys affected). Rolling back."
      ))
    }

    # Delete rows from target that are no longer in the source
    n_deleted <- dbExecute(
      con,
      paste0(
        "DELETE tgt
         FROM ",
        SCHEMA_NAME,
        ".",
        FORM,
        " tgt
         LEFT JOIN ",
        TEMP_TABLE,
        " src
           ON tgt.SubmissionId = src.SubmissionId
         WHERE src.SubmissionId IS NULL;"
      )
    )

    # Update existing rows
    n_updated <- dbExecute(
      con,
      paste0(
        "UPDATE tgt
        SET
          tgt.RefreshDate                    = src.RefreshDate,
          tgt.SubmissionTime                 = src.SubmissionTime,
          tgt.Status                         = src.Status,
          tgt.ProjectName                    = src.ProjectName,
          tgt.ProjectNumberOrWorkOrder       = src.ProjectNumberOrWorkOrder,
          tgt.Building                       = src.Building,
          tgt.BuildingName                   = src.BuildingName,
          tgt.Property                       = src.Property,
          tgt.Address                        = src.Address,
          tgt.Description                    = src.Description,
          tgt.[File]                         = src.[File],
          tgt.DrawingType_Civil              = src.DrawingType_Civil,
          tgt.DrawingType_Architectural      = src.DrawingType_Architectural,
          tgt.DrawingType_Electrical         = src.DrawingType_Electrical,
          tgt.DrawingType_FireProtection     = src.DrawingType_FireProtection,
          tgt.DrawingType_Mechanical         = src.DrawingType_Mechanical,
          tgt.DrawingType_Other              = src.DrawingType_Other,
          tgt.DrawingType_Plumbing           = src.DrawingType_Plumbing,
          tgt.DrawingType_Structural         = src.DrawingType_Structural,
          tgt.DrawingType_Telecommunications = src.DrawingType_Telecommunications,
          tgt.SensitiveFile_Yes              = src.SensitiveFile_Yes,
          tgt.SensitiveFile_No               = src.SensitiveFile_No,
          tgt.SensitiveFile_Unsure           = src.SensitiveFile_Unsure,
          tgt.RequestorName                  = src.RequestorName,
          tgt.RequestorEmail                 = src.RequestorEmail,
          tgt.RequestorPhoneNumber           = src.RequestorPhoneNumber,
          tgt.RequestorInformation           = src.RequestorInformation,
          tgt.DateRequired                   = src.DateRequired
        FROM ",
        SCHEMA_NAME,
        ".",
        FORM,
        " tgt
        INNER JOIN ",
        TEMP_TABLE,
        " src
          ON tgt.SubmissionId = src.SubmissionId;"
      )
    )

    # Insert new rows
    n_inserted <- dbExecute(
      con,
      paste0(
        "INSERT INTO ",
        SCHEMA_NAME,
        ".",
        FORM,
        "
          (
          RefreshDate,
          SubmissionId,
          SubmissionTime,
          Status,
          ProjectName,
          ProjectNumberOrWorkOrder,
          Building,
          BuildingName,
          Property,
          Address,
          Description,
          [File],
          DrawingType_Civil,
          DrawingType_Architectural,
          DrawingType_Electrical,
          DrawingType_FireProtection,
          DrawingType_Mechanical,
          DrawingType_Other,
          DrawingType_Plumbing,
          DrawingType_Structural,
          DrawingType_Telecommunications,
          SensitiveFile_Yes,
          SensitiveFile_No,
          SensitiveFile_Unsure,
          RequestorName,
          RequestorEmail,
          RequestorPhoneNumber,
          RequestorInformation,
          DateRequired
          )
        SELECT
          src.RefreshDate,
          src.SubmissionId,
          src.SubmissionTime,
          src.Status,
          src.ProjectName,
          src.ProjectNumberOrWorkOrder,
          src.Building,
          src.BuildingName,
          src.Property,
          src.Address,
          src.Description,
          src.[File],
          src.DrawingType_Civil,
          src.DrawingType_Architectural,
          src.DrawingType_Electrical,
          src.DrawingType_FireProtection,
          src.DrawingType_Mechanical,
          src.DrawingType_Other,
          src.DrawingType_Plumbing,
          src.DrawingType_Structural,
          src.DrawingType_Telecommunications,
          src.SensitiveFile_Yes,
          src.SensitiveFile_No,
          src.SensitiveFile_Unsure,
          src.RequestorName,
          src.RequestorEmail,
          src.RequestorPhoneNumber,
          src.RequestorInformation,
          src.DateRequired
        FROM ",
        TEMP_TABLE,
        " src
        LEFT JOIN ",
        SCHEMA_NAME,
        ".",
        FORM,
        " tgt
          ON tgt.SubmissionId = src.SubmissionId
        WHERE tgt.SubmissionId IS NULL;"
      )
    )

    dbCommit(con)

    n_deleted <<- n_deleted
    n_updated <<- n_updated
    n_inserted <<- n_inserted

    cat(
      "ETL complete — deleted:",
      n_deleted,
      "| updated:",
      n_updated,
      "| inserted:",
      n_inserted,
      "\n"
    )
  },
  error = function(e) {
    dbRollback(con)
    etl_error <<- e
  }
)

task_end <- Sys.time()
task_duration <- interval(task_start, task_end) / dseconds()

if (is.null(etl_error)) {
  log_daily_etl_run(
    api_name = API_NAME,
    script_name = SCRIPT_NAME,
    table_name = FORM,
    duration = task_duration,
    status = "SUCCESS",
    n_inserted = n_inserted,
    n_updated = n_updated,
    n_deleted = n_deleted,
    message = "ETL completed successfully"
  )
} else {
  log_daily_etl_run(
    api_name = API_NAME,
    script_name = SCRIPT_NAME,
    table_name = FORM,
    status = "FAILURE",
    message = substr(etl_error$message, 1, 500)
  )
  stop(etl_error)
}
