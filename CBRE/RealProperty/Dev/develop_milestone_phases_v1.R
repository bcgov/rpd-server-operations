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
  CashflowId,
  CashflowParentId,
  Period,
  LineCategory,
  ActivityCode,
  ActivityCodeDesc,
  AllocationItemId,
  ItemAmount,
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

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.pjm_fact_milestone")
PjmMilestoneData <- dbFetch(query, n = -1)
dbClearResult(query)

ProjData <- FactProjData |>
  left_join(DimProjData, by = join_by(project_skey)) |>
  left_join(
    CashflowData,
    by = join_by(project_number == ProjectNumber),
    relationship = "many-to-many"
  )

milestone_counts <- PjmMilestoneData |>
  group_by(milestone_desc) |>
  summarise(count = n()) |>
  ungroup()

phase_order <- c(
  "Planning",
  "Feasibility",
  "Design",
  "Pre-Tender",
  "Tender",
  "Construction",
  "Closeout",
  "Warranty Period"
)

milestones <- PjmMilestoneData |>
  filter(
    milestone_desc %in%
      c(
        "Authorization to Proceed",
        "Authorization to Proceed - RFS created and sent to Client",
        "Feasibility Complete",
        "Feasibility Completed",
        "Design Complete",
        "Design Development Complete",
        "Construction Documents Complete",
        "Construction Documents Complete ",
        "Out to Tender",
        "Out to Tender - Prime",
        "Project Tender Complete",
        "Tender Award",
        "Tender Award ",
        "Tender Awards",
        "Commence Construction",
        # "Substantial Completion",
        "Project Substantial Completion",
        "Facility Open for Business",
        "Deficiencies List Complete",
        "Technical Closeout",
        "Technical closeout",
        "Closeout Complete",
        "Closeout Summary Submitted to Client",
        "Project Closeout - Project Cancelled",
        "Project Closeout"
      )
  ) |>
  mutate(
    CBRE_Proj_Milestone = case_when(
      milestone_desc %in%
        c(
          "Authorization to Proceed",
          "Authorization to Proceed - RFS created and sent to Client"
        ) ~ "Authorization to Proceed",
      milestone_desc %in%
        c(
          "Feasibility Complete",
          "Feasibility Completed"
        ) ~ "Feasibility Complete",
      milestone_desc %in%
        c(
          "Design Complete",
          "Design Development Complete"
        ) ~ "Design Development Complete",
      milestone_desc %in%
        c(
          "Construction Documents Complete",
          "Construction Documents Complete "
        ) ~ "Construction Documents Complete",
      milestone_desc %in%
        c(
          "Out to Tender",
          "Out to Tender - Prime",
          "Project Tender Complete",
          "Tender Award",
          "Tender Award ",
          "Tender Awards"
        ) ~ "Tender Award",
      milestone_desc %in%
        c(
          "Commence Construction"
        ) ~ "Commence Construction",
      milestone_desc %in%
        c(
          "Facility Open for Business",
          "Facility Transition Completion / Handover"
        ) ~ "Facility Open for Business",
      milestone_desc %in%
        c(
          "Deficiencies List Complete"
        ) ~ "Deficiencies List Complete",
      milestone_desc %in%
        c(
          "Project Closeout - Project Cancelled",
          "Project Closeout",
          "Technical Closeout",
          "Technical closeout",
          "Closeout Complete",
          "Closeout Summary Submitted to Client"
        ) ~ "Project Closeout"
    ),
    .default = milestone_desc
  ) |>
  filter(
    CBRE_Proj_Milestone %in%
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
        "Project Closeout"
      )
  ) |>
  mutate(
    CBRE_Proj_Phase = case_when(
      CBRE_Proj_Milestone == "Authorization to Proceed" ~ "Planning",
      CBRE_Proj_Milestone == "Feasibility Complete" ~ "Feasibility",
      CBRE_Proj_Milestone == "Design Development Complete" ~ "Design",
      CBRE_Proj_Milestone == "Construction Documents Complete" ~ "Pre-Tender",
      CBRE_Proj_Milestone == "Tender Award" ~ "Tender",
      CBRE_Proj_Milestone == "Commence Construction" ~ "Construction",
      CBRE_Proj_Milestone == "Facility Open for Business" ~ "Closeout",
      CBRE_Proj_Milestone == "Deficiencies List Complete" ~ "Closeout",
      CBRE_Proj_Milestone == "Project Closeout" ~ "Warranty Period"
    )
  ) |>
  mutate(CBRE_Proj_Phase = factor(CBRE_Proj_Phase, levels = phase_order)) |>
  select(
    project_skey,
    milestone_desc,
    CBRE_Proj_Milestone,
    CBRE_Proj_Phase,
    estimated_start_date,
    actual_start_date,
    estimated_end_date,
    actual_end_date
  ) |>
  mutate(
    PhaseStartDate = case_when(
      is.na(actual_start_date) ~ estimated_start_date,
      .default = actual_start_date
    ),
    PhaseEndDate = case_when(
      is.na(actual_end_date) ~ estimated_end_date,
      .default = actual_end_date
    ),
    .keep = "unused"
  )

test_multiple_dates <- milestones |>
  filter(!is.na(PhaseStartDate)) |>
  group_by(project_skey, CBRE_Proj_Phase) |>
  mutate(count = n()) |>
  filter(count >= 2)

milestone_phases <- milestones |>
  group_by(project_skey, CBRE_Proj_Phase) |>
  summarise(
    CBRE_Proj_Milestone = first(CBRE_Proj_Milestone),
    PhaseStartDate = min(PhaseStartDate),
    PhaseEndDate = min(PhaseEndDate),
    .groups = "drop"
  ) |>
  pivot_wider(
    id_cols = project_skey,
    names_from = CBRE_Proj_Phase,
    values_from = c(PhaseStartDate, PhaseEndDate),
    names_glue = "{stringr::str_remove_all(CBRE_Proj_Phase, ' ')}{stringr::str_remove(.value, '^Phase')}",
    names_vary = "slowest"
  )

test_closeout <- check_closeout |>
  group_by(project_skey) |>
  mutate(count = n()) |>
  filter(count >= 2) |>
  ungroup()


ProjMilestones <- ProjData |>
  filter(project_skey %in% milestone_phases$project_skey) |>
  select(
    project_skey,
    project_status,
    project_phase,
    project_number,
    Period,
    LineCategory,
    ItemAmount,
    AllocationAmount,
    AllocationDate
  ) |>
  filter(between(
    AllocationDate,
    as.POSIXct("2026-04-01"),
    as.POSIXct("2027-03-31")
  )) |>
  left_join(milestone_phases, by = join_by(project_skey))

# set <- ProjMilestones |>
#   filter(project_number == "K1009759") |>
#   arrange(AllocationDate)
#
# openxlsx2::write_xlsx(set, here::here("output/ProjectForecast/K1009759_cashflow_milestones.xlsx"))

phase_order <- c(
  "Planning",
  "Feasibility",
  "Design",
  "Pre-Tender",
  "Tender",
  "Construction",
  "Closeout",
  "Warranty Period"
)

phase_dates_long <- ProjMilestones |>
  distinct(project_skey, across(ends_with("EndDate"))) |>
  pivot_longer(
    cols = -project_skey,
    names_to = "CBRE_Proj_Phase",
    names_pattern = "^(.*)EndDate$",
    values_to = "PhaseEndDate"
  ) |>
  filter(!is.na(PhaseEndDate)) |>
  mutate(
    CBRE_Proj_Phase = factor(CBRE_Proj_Phase, levels = phase_order),
    PhaseEndDate = as.Date(PhaseEndDate) # strip time component for clean comparison
  )

milestone_phase_lookup <- ProjMilestones |>
  distinct(project_skey, AllocationDate) |>
  left_join(
    phase_dates_long,
    by = "project_skey",
    relationship = "many-to-many"
  ) |>
  filter(PhaseEndDate <= AllocationDate) |>
  arrange(
    project_skey,
    AllocationDate,
    desc(PhaseEndDate),
    desc(as.integer(CBRE_Proj_Phase))
  ) |>
  group_by(project_skey, AllocationDate) |>
  slice(1) |>
  ungroup() |>
  select(project_skey, AllocationDate, Milestone_Phase = CBRE_Proj_Phase)

ProjMilestonePhase <- ProjMilestones |>
  left_join(milestone_phase_lookup, by = c("project_skey", "AllocationDate")) |>
  relocate(Milestone_Phase, .after = project_phase)
