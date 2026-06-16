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
Form <- "Drawings_Request_Submission"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = Form)
TEMP_TABLE <- paste0("#", Form, "Temp")
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
      Address == "" ~ Address1,
      .default = Address
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
    )
  ) |>
  select(
    ProjectName,
    ProjectNumberOrWorkOrder,
    Building,
    BuildingName,
    Property,
    Address,
    Description = description,
    File,
    SensitiveFile = doTheseFilesContainInformationThatIfCompromisedCouldCauseExtremelyGraveInjuryToAnIndividualOrganizationOrGovernment,
    DrawingType_Civil = civil,
    DrawingType_Architectural = architectural,
    DrawingType_Electrical = electrical,
    DrawingType_FireProtection = fireProtection,
    DrawingType_Mechanical = mechanical,
    DrawingType_Other = other,
    DrawingType_Plumbing = plumbing,
    DrawingType_Structural = structural,
    DrawingType_Telecommunications = telecommunications,
    RequestorName,
    RequestorEmail,
    RequestorPhoneNumber,
    # RequestorInformation,
    DateRequired = dateRequired
  )
