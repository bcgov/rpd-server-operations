library(base64enc)
library(dplyr)
library(here)
library(httr2)
library(tidyr)
library(tools)
library(openxlsx2)
library(lubridate)

source("C:/Projects/rpd-utilities/R/cbre_api_function.R")

# Load Error File
errorFiles <- list.files(
  "Kahua-File-Validation/input",
  pattern = "(CBREErrorFile)"
) |>
  sort(decreasing = TRUE)

currentEFile <- read_xlsx(
  paste0(
    "Kahua-File-Validation/input",
    errorFiles[1]
  ),
  start_row = 3,
  check_names = TRUE
) |>
  filter(Is.Valid == "No")

# Load previous payable file for ERP information
kahuaPFiles <- list.files(
  "Kahua-File-Validation/output",
  pattern = "(RPDKahuaPayable)"
) |>
  sort(decreasing = TRUE)

currentPFile <- read_xlsx(
  paste0(
    "Kahua-File-Validation/output/",
    kahuaPFiles[1]
  )
)

# Get contact information for project coordinators from EDP
roleData <- extract_cbre_data("pjm_report_project_role_vw")

role <- roleData |>
  select_if(~ !all(is.na(.))) |>
  select_if(~ !all(. == 0)) |>
  select_if(~ !all(is.na(.))) |>
  select_if(~ !all(. == '-1')) |>
  select_if(~ !all(. == "N/A")) |>
  select_if(~ !all(. == "-")) |>
  select_if(~ !all(. == "")) |>
  select(
    project_skey,
    system_contact
  )

KahuaData <- extract_cbre_data(
  "pjm_dim_project_vw"
)

kahua <- KahuaData |>
  select_if(~ !all(is.na(.))) |>
  select_if(~ !all(. == 0)) |>
  select_if(~ !all(is.na(.))) |>
  select_if(~ !all(. == '-1')) |>
  select_if(~ !all(. == "N/A")) |>
  select_if(~ !all(. == "-")) |>
  select_if(~ !all(. == "")) |>
  select(project_skey, project_number)

contactData <- extract_cbre_data(
  "pjm_dim_contact_vw"
)

contact <- contactData |>
  select_if(~ !all(is.na(.))) |>
  select_if(~ !all(. == 0)) |>
  select_if(~ !all(is.na(.))) |>
  select_if(~ !all(. == '-1')) |>
  select_if(~ !all(. == "N/A")) |>
  select_if(~ !all(. == "-")) |>
  select_if(~ !all(. == "")) |>
  filter(stringr::str_detect(email_id, "@gov.bc.ca")) |>
  mutate(across(c(first_name, last_name, job_title), trimws)) |>
  mutate(Name = paste(first_name, last_name)) |>
  select(
    Name,
    email_id,
    job_title,
    contact_skey,
    company_name
  )

matchList <- kahua |>
  full_join(role, by = join_by(project_skey)) |>
  full_join(contact, by = join_by(system_contact == Name))

# Create output file
matchedErrors <- currentEFile |>
  left_join(matchList, by = join_by(Source == project_number)) |>
  select(
    Source,
    GL.Code,
    Line.ID,
    Is.Valid,
    Validation.Message,
    name = system_contact,
    email_id,
    job_title
  )

output <- currentPFile |>
  full_join(
    matchedErrors,
    by = join_by(
      `Kahua Project Number` == Source,
      `GL Code` == GL.Code,
      `Line ID` == Line.ID
    )
  ) |>
  mutate(across(where(is.character), ~ replace_na(.x, "")))

openxlsx2::write_xlsx(
  output,
  paste0(
    "Kahua-File-Validation/output/RPDKahuaValidationLog-",
    Sys.Date(),
    ".xlsx"
  ),
  as_table = FALSE
)
