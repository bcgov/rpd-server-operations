# Load helper functions
source(here::here("utilities/R/utilities.R"))

# Load libraries
library(base64enc, quietly = TRUE, warn.conflicts = FALSE)
library(dplyr, quietly = TRUE, warn.conflicts = FALSE)
library(ggplot2, quietly = TRUE, warn.conflicts = FALSE)
library(here, quietly = TRUE, warn.conflicts = FALSE)
library(httr2, quietly = TRUE, warn.conflicts = FALSE)
library(jsonlite, quietly = TRUE, warn.conflicts = FALSE)
library(lubridate, quietly = TRUE, warn.conflicts = FALSE)
library(purrr, quietly = TRUE, warn.conflicts = FALSE)
library(scales, quietly = TRUE, warn.conflicts = FALSE)
library(tibble, quietly = TRUE, warn.conflicts = FALSE)
library(tidyr, quietly = TRUE, warn.conflicts = FALSE)

library(odbc, quietly = TRUE, warn.conflicts = FALSE)
library(DBI, quietly = TRUE, warn.conflicts = FALSE)

ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "RealProperty"

con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = SQL_SERVER,
  database = DB_NAME,
  Trusted_Connection = "Yes"
)

query <- dbSendQuery(
  con,
  "SELECT
  ProjectNumber,
  LineCategory,
  AllocationAmount,
  AllocationDate
  FROM CbreStaging.kahua_cashflow
  WHERE CashflowParentId IS NOT NULL"
)
CashflowData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(
  con,
  "SELECT
   project_skey,
   project_number,
   csf_pmosource,
   csf_ministryparentorg,
   csf_branchchildorg,
   csf_fundingsource,
   csf_fundingtype
  FROM CbreStaging.pjm_dim_project"
)
DimProjData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(
  con,
  "SELECT
   project_skey,
   project_status,
   project_phase
   FROM CbreStaging.pjm_fact_project"
)
FactProjData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(
  con,
  "SELECT
   project_skey,
   milestone_desc,
   estimated_start_date,
   revised_start_date,
   actual_start_date,
   estimated_end_date,
   revised_end_date,
   actual_end_date,
   project_start_milestone_f,
   project_end_milestone_f
   FROM CbreStaging.pjm_fact_milestone"
)
PjmMilestoneData <- dbFetch(query, n = -1)
dbClearResult(query)

ProjData <- FactProjData |>
  left_join(DimProjData, by = join_by(project_skey)) |>
  select(
    project_skey,
    project_number,
    project_status,
    project_phase,
    csf_pmosource,
    csf_fundingsource,
    csf_fundingtype
  ) |>
  # Pregroup PMO source
  mutate(
    csf_pmosource = case_when(
      csf_pmosource == "ARES" ~ "CBRE",
      csf_pmosource %in%
        c("Forum/Honeywell", "Plenary/Honeywell", "Plenary/JCI") ~ "P3",
      csf_pmosource == "Non-Project Contracts" ~ "NPC",
      .default = csf_pmosource
    )
  )

ProjMilestoneData <- ProjData |>
  left_join(PjmMilestoneData, by = join_by(project_skey))

milestone_order <- c(
  "Authorization to Proceed",
  "Feasibility Complete",
  "Design Development Complete",
  "Construction Documents Complete",
  "Tender Award",
  "Commence Construction",
  "Substantial Completion",
  "Facility Open for Business",
  "Deficiencies List Complete",
  "Technical Closeout",
  "Project Closeout",
  "Warranty"
)

# CBRE Data ####
CbreMilestoneData <- ProjMilestoneData |>
  filter(csf_pmosource == "CBRE") |>
  mutate(
    milestone_start_date = case_when(
      !is.na(actual_start_date) ~ actual_start_date,
      !is.na(revised_start_date) ~ revised_start_date,
      .default = estimated_start_date
    ),
    milestone_end_date = case_when(
      !is.na(actual_end_date) ~ actual_end_date,
      !is.na(revised_end_date) ~ revised_end_date,
      .default = estimated_end_date
    ),
    .keep = "unused"
  ) |>
  filter(!is.na(milestone_end_date) | !is.na(milestone_start_date)) |>
  mutate(
    milestone_date = case_when(
      !is.na(milestone_end_date) ~ milestone_end_date,
      !is.na(milestone_start_date) ~ milestone_start_date,
      .default = milestone_end_date
    ),
    .keep = "unused",
    .after = milestone_desc
  )

CbreProjectCount <- ProjMilestoneData |>
  filter(csf_pmosource == "CBRE") |>
  group_by(project_number) |>
  n_distinct()

CbreMilestoneCount <- CbreMilestoneData |>
  group_by(milestone_desc) |>
  summarise(count = n())

CbreMilestoneProjCount <- CbreMilestoneData |>
  group_by(project_number) |>
  n_distinct()

CbreMilestones <- CbreMilestoneData |>
  mutate(
    milestone = case_when(
      milestone_desc %in%
        c(
          "Authorization to Proceed"
        ) ~ "Authorization to Proceed",
      milestone_desc %in%
        c(
          "Feasibility Complete",
          "Feasibility Completed"
        ) ~ "Feasibility Complete",
      milestone_desc %in%
        c(
          "Design Complete",
          "Design Development Complete",
          "Design Completed", # 2 instances
          "Design Development Complete ", # extra space at end 1 instance
          "Design Development  Complete", # extra space before Complete, 1 instance
          "Design Documents Complete" # 1 instance
        ) ~ "Design Development Complete",
      milestone_desc %in%
        c(
          "Construction Documents Complete",
          "Construction Documents Complete " # 70 instances
        ) ~ "Construction Documents Complete",
      milestone_desc %in%
        c(
          "Tender Award",
          "Out to Tender", # 10 instances
          "Tender Award ", # 3 instances
          "Tender Awards" # 1 instance
        ) ~ "Tender Award",
      milestone_desc %in%
        c(
          "Commence Construction"
        ) ~ "Commence Construction",
      milestone_desc %in%
        c(
          "Substantial Completion"
        ) ~ "Substantial Completion",
      milestone_desc %in%
        c(
          "Facility Open for Business"
        ) ~ "Facility Open for Business",
      milestone_desc %in%
        c(
          "Deficiencies List Complete"
        ) ~ "Deficiencies List Complete",
      milestone_desc %in%
        c(
          "Technical Closeout",
          "Technical closeout" # 1 instance
        ) ~ "Technical Closeout",
      milestone_desc %in%
        c(
          "Project Closeout",
          "Warranty" # 10 instances
        ) ~ "Project Closeout",
      .default = NA
    ),
    .after = milestone_desc
  ) |>
  # 189 milestones do not fit into the above and are set to NA to be filtered out
  filter(
    milestone %in%
      c(
        "Authorization to Proceed",
        "Feasibility Complete",
        "Design Development Complete",
        "Construction Documents Complete",
        "Tender Award",
        "Commence Construction",
        "Substantial Completion",
        "Facility Open for Business",
        "Deficiencies List Complete",
        "Technical Closeout",
        "Project Closeout"
      )
  ) |>
  mutate(milestone = factor(milestone, levels = milestone_order)) |>
  pivot_wider(
    id_cols = c(
      project_skey,
      project_number,
      project_status,
      project_phase,
      csf_pmosource,
      csf_fundingsource,
      csf_fundingtype
    ),
    names_from = milestone,
    values_from = milestone_date,
    names_glue = "{stringr::str_remove_all(milestone, ' ')}{'Date'}",
    names_sort = TRUE
  )

cbre_na_milestone_summary <- CbreMilestones |>
  select(AuthorizationtoProceedDate:ProjectCloseoutDate) |>
  summarise(across(everything(), ~ sum(is.na(.)))) |>
  pivot_longer(
    everything(),
    names_to = "Milestone",
    values_to = "Count_Missing"
  ) |>
  mutate(
    Percent_Missing = round(100 * Count_Missing / nrow(CbreMilestoneData), 1)
  ) |>
  arrange(desc(Count_Missing))

# RPD Data ####
RpdMilestoneData <- ProjMilestoneData |>
  filter(csf_pmosource == "RPD WDS") |>
  mutate(
    milestone_start_date = case_when(
      !is.na(actual_start_date) ~ actual_start_date,
      !is.na(revised_start_date) ~ revised_start_date,
      .default = estimated_start_date
    ),
    milestone_end_date = case_when(
      !is.na(actual_end_date) ~ actual_end_date,
      !is.na(revised_end_date) ~ revised_end_date,
      .default = estimated_end_date
    ),
    .keep = "unused"
  ) |>
  filter(!is.na(milestone_end_date) | !is.na(milestone_start_date)) |>
  mutate(
    milestone_date = case_when(
      !is.na(milestone_end_date) ~ milestone_end_date,
      !is.na(milestone_start_date) ~ milestone_start_date,
      .default = milestone_end_date
    ),
    .keep = "unused",
    .after = milestone_desc
  )

RpdProjectCount <- ProjMilestoneData |>
  filter(csf_pmosource == "RPD WDS") |>
  group_by(project_number) |>
  n_distinct()

RpdMilestoneCount <- RpdMilestoneData |>
  group_by(milestone_desc) |>
  summarise(count = n())

RpdMilestoneProjCount <- RpdMilestoneData |>
  group_by(project_number) |>
  n_distinct()

RpdMilestones <- RpdMilestoneData |>
  mutate(
    milestone = case_when(
      milestone_desc %in%
        c(
          "Authorization to Proceed"
        ) ~ "Authorization to Proceed",
      milestone_desc %in%
        c(
          "Feasibility Complete"
        ) ~ "Feasibility Complete",
      milestone_desc %in%
        c(
          "Design Complete"
        ) ~ "Design Development Complete",
      #       Completely missing
      # milestone_desc %in%
      #   c(
      #     "Construction Documents Complete",
      #     "Construction Documents Complete"
      #   ) ~ "Construction Documents Complete",
      milestone_desc %in%
        c(
          "Out to Tender", # 39 instances
          "Out to Tender - Prime" # 1 instances
        ) ~ "Tender Award",
      milestone_desc %in%
        c(
          "Commence Construction"
        ) ~ "Commence Construction",
      milestone_desc %in%
        c(
          "Substantial Completion",
          "Project Substantial Completion" # 1 instance
        ) ~ "Substantial Completion",
      milestone_desc %in%
        c(
          "Facility Open for Business",
          "Facility Transition Completion / Handover" # 1 instance
        ) ~ "Facility Open for Business",
      milestone_desc %in%
        c(
          "Deficiencies List Complete"
        ) ~ "Deficiencies List Complete",
      milestone_desc %in%
        c(
          "Technical Closeout",
          "Technical Submission Complete" # 1 instance
        ) ~ "Technical Closeout",
      milestone_desc %in%
        c(
          "Project Closeout"
        ) ~ "Project Closeout",
      milestone_desc %in%
        c(
          "Warranty",
          "WARRANTY" # 1 instance
        ) ~ "Warranty",
      .default = NA
    ),
    .after = milestone_desc
  ) |>
  filter(
    milestone %in%
      c(
        "Authorization to Proceed",
        "Feasibility Complete",
        "Design Development Complete",
        "Construction Documents Complete",
        "Tender Award",
        "Commence Construction",
        "Substantial Completion",
        "Facility Open for Business",
        "Deficiencies List Complete",
        "Technical Closeout",
        "Project Closeout",
        "Warranty"
      )
  ) |>
  # Couple weird milestones, especially Tender Award for project_skey 9459766
  group_by(project_skey, milestone) |>
  arrange(desc(milestone_date)) |>
  slice(1) |>
  ungroup() |>
  mutate(milestone = factor(milestone, levels = milestone_order)) |>
  pivot_wider(
    id_cols = c(
      project_skey,
      project_number,
      project_status,
      project_phase,
      csf_pmosource,
      csf_fundingsource,
      csf_fundingtype
    ),
    names_from = milestone,
    values_from = milestone_date,
    names_glue = "{stringr::str_remove_all(milestone, ' ')}{'Date'}",
    names_sort = TRUE
  )

rpd_na_milestone_summary <- RpdMilestones |>
  select(AuthorizationtoProceedDate:ProjectCloseoutDate) |>
  summarise(across(everything(), ~ sum(is.na(.)))) |>
  pivot_longer(
    everything(),
    names_to = "Milestone",
    values_to = "Count_Missing"
  ) |>
  mutate(
    Percent_Missing = round(100 * Count_Missing / nrow(CbreMilestoneData), 1)
  ) |>
  arrange(desc(Count_Missing))
