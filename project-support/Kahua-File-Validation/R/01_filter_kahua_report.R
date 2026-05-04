library(base64enc)
library(dplyr)
library(here)
library(httr2)
library(tidyr)
library(tools)
library(openxlsx2)
library(lubridate)

source("C:/Projects/rpd-utilities/R/cbre_api_function.R")

# Validate that the sorting works consistently as we bring in new files
kahuaFiles <- list.files(
  "Kahua-File-Validation/input",
  pattern = "(RPDKahuaPayable)"
) |>
  sort(decreasing = TRUE)

pastKFile <- openxlsx2::read_xlsx(paste0(
  "Kahua-File-Validation/input/",
  kahuaFiles[2]
)) |>
  select(`Line ID`)

currentKFile <- openxlsx2::read_xlsx(paste0(
  "Kahua-File-Validation/input/",
  kahuaFiles[1]
))

# Generate output ####
output <- currentKFile |>
  filter(!(`Line ID` %in% pastKFile$`Line ID`)) |>
  filter(
    !(`Cost Category` == "CBRE PJM Reimburseables" &
      `ERP Vendor Name` %in%
        c(
          "BGIS WORKPLACE SOLUTIONS INC. - 001",
          "CBRE",
          "CBRE Limited - 005"
        ))
  ) |>
  mutate(across(where(is.character), ~ replace_na(.x, "")))

openxlsx2::write_xlsx(
  output,
  paste0(
    "Kahua-File-Validation/output/RPDKahuaPayableFiltered-",
    Sys.Date(),
    ".xlsx"
  ),
  as_table = FALSE
)
