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

# hue_pal()(10)
phase_colors <- c(
  "Strategic Planning" = "#F8766D",
  "Initiation" = "#A3A500",
  "Planning" = "#39B600",
  "Feasibility" = "#00BF7D",
  "Design" = "#00BFC4",
  "Pre-Tender" = "#00B0F6",
  "Tender" = "#9590FF",
  "Construction" = "#D89000",
  "Closeout" = "#FF62BC",
  "Warranty Period" = "#E76BF3",
  "Unknown" = "#B3B3B3" # explicit color for NA phases
)

Output <- FactProjData |>
  left_join(DimProjData, by = join_by(project_skey)) |>
  left_join(
    CashflowData,
    by = join_by(project_number == ProjectNumber),
    relationship = "many-to-many"
  )

forecastData <- Output |>
  filter(LineCategory == "Forecast") |>
  mutate(
    MonthLabel = format(AllocationDate, "%b %Y"),
    MonthLabel = factor(
      MonthLabel,
      levels = format(
        seq(as.Date("2019-04-01"), as.Date("2029-12-01"), by = "month"),
        "%b %Y"
      )
    ),
    project_phase = forcats::fct_na_level_to_value(
      project_phase,
      extra_levels = "Unknown"
    ),
    project_phase = factor(project_phase, levels = names(phase_colors))
  ) |>
  group_by(project_phase, AllocationDate, MonthLabel) |>
  summarise(
    AllocationAmount = sum(AllocationAmount),
    .groups = "drop"
  )

plot_data <- forecastData |>
  filter(between(
    AllocationDate,
    as.POSIXct("2026-04-01"),
    as.POSIXct("2027-03-31")
  ))

ggplot(
  plot_data,
  aes(x = MonthLabel, y = AllocationAmount, fill = project_phase)
) +
  geom_col(width = 0.7, position = "stack") +
  scale_y_continuous(labels = label_currency()) +
  scale_fill_manual(values = phase_colors, drop = FALSE) +
  labs(
    title = "FY2026-27 Monthly Allocation Forecast by Project Phase",
    x = "Month",
    y = "Allocation Amount",
    fill = "Project Phase"
  ) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
