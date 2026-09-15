# Load helper functions
source(here::here("utilities/R/utilities.R"))

# Load libraries
library(base64enc, quietly = TRUE, warn.conflicts = FALSE)
library(dplyr, quietly = TRUE, warn.conflicts = FALSE)
library(forcats, quietly = TRUE, warn.conflicts = FALSE)
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

# sourced from develop_milestone_phases_v2.R
query <- dbSendQuery(
  con,
  "SELECT *
  FROM RealProperty.PjmMilestonePhase"
)
PjmMilestonePhase <- dbFetch(query, n = -1)
dbClearResult(query)

# Strategic Planning and Initiation are in project_phase but not in milestones.
phase_colors <- c(
  "Strategic Planning" = "#F52750",
  "Initiation" = "#F5DD27",
  "Planning" = "#96BF26",
  "Feasibility" = "#47BF26",
  "Design" = "#65F777",
  "Pre-Tender" = "#65F7DC",
  "Tender" = "#65D5F7",
  "Construction" = "#F5A627",
  "Closeout" = "#F5BFFF",
  "Warranty Period" = "#F74FE1",
  "Unknown" = "#B3B3B3" # explicit color for NA phases
)

data <- PjmMilestonePhase |>
  # setup factors
  mutate(
    project_phase = fct_explicit_na(project_phase, na_level = "Unknown"),
    Milestone_Phase = fct_explicit_na(Milestone_Phase, na_level = "Unknown"),
    project_phase = factor(project_phase, levels = names(phase_colors)),
    Milestone_Phase = factor(Milestone_Phase, levels = names(phase_colors))
  )

# Phases and PMO plot ####
# Now advancing to adding in pmosource and selecting whether using phase or the milestone inferred phase
# --- 1. Precompute fiscal year & month ---
data_prepped <- data |>
  mutate(
    AllocationDate = as.Date(AllocationDate),
    month_date = floor_date(AllocationDate, "month"),
    fy_start_year = if_else(
      month(AllocationDate) >= 4,
      year(AllocationDate),
      year(AllocationDate) - 1L
    ),
    fiscal_year = paste0(
      fy_start_year,
      "-",
      sprintf("%02d", (fy_start_year + 1) %% 100)
    ),
    LineCategory = factor(
      LineCategory,
      levels = c("Budget", "Forecast", "Actuals")
    )
  )

# --- 2. Precompute a long-format summary: project_phase & Milestone_Phase stacked ---
monthly_summary <- bind_rows(
  data_prepped |>
    group_by(
      fiscal_year,
      month_date,
      LineCategory,
      csf_pmosource,
      phase = project_phase
    ) |>
    summarise(
      AllocationAmount = sum(AllocationAmount, na.rm = TRUE),
      .groups = "drop"
    ) |>
    mutate(phase_type = "project_phase"),
  data_prepped |>
    group_by(
      fiscal_year,
      month_date,
      LineCategory,
      csf_pmosource,
      phase = Milestone_Phase
    ) |>
    summarise(
      AllocationAmount = sum(AllocationAmount, na.rm = TRUE),
      .groups = "drop"
    ) |>
    mutate(phase_type = "Milestone_Phase")
)

# --- 3. Filter step: pick fiscal year, pmosource(s), and phase type to review ---
fy_selected <- "2026-27" # <- change as needed
pmosource_selected <- "CBRE" # <- subset for a specific source, or leave as "all"
# pmosource_selected <- c("CBRE", "NPC", "P3", "Other", "RPD WDS")
phase_type_selected <- "Milestone_Phase"
# phase_type_selected <- "project_phase"
plot_data <- monthly_summary |>
  filter(
    fiscal_year == fy_selected,
    csf_pmosource %in% pmosource_selected,
    phase_type == phase_type_selected
  ) |>
  group_by(fiscal_year, month_date, LineCategory, phase) |>
  summarise(
    AllocationAmount = sum(AllocationAmount, na.rm = TRUE),
    phase_type = first(phase_type),
    .groups = "drop"
  )


# --- 4. Plot: facet by LineCategory, fill by phase ---
ggplot(plot_data, aes(x = month_date, y = AllocationAmount, fill = phase)) +
  geom_col() +
  facet_wrap(~LineCategory, ncol = 1, scales = "free_y") +
  scale_x_date(date_labels = "%b %Y", date_breaks = "1 month") +
  scale_y_continuous(labels = label_currency(scale = 1e-6, suffix = "M")) +
  scale_fill_manual(values = phase_colors, drop = FALSE) +
  labs(
    title = paste0(
      "Monthly Allocation by ",
      phase_type_selected,
      " — ",
      fy_selected
    ),
    subtitle = if (
      length(pmosource_selected) < length(unique(data_prepped$csf_pmosource))
    ) {
      paste("PMO Source:", paste(pmosource_selected, collapse = ", "))
    } else {
      "All PMO Sources"
    },
    x = NULL,
    y = "Sum of Allocation Amount",
    fill = phase_type_selected
  ) +
  theme_minimal(base_size = 12) +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1),
    legend.position = "bottom"
  )

# Compare phases by one line type ####
# --- Comparison plot: one LineCategory, faceted by phase_type ---
# --- Filter step: pick fiscal year, pmosource(s), and LineCategory to review ---
fy_selected <- "2026-27"
pmosource_selected <- "CBRE"
line_category_selected <- "Actuals" # <- "Budget" | "Forecast" | "Actuals"

plot_data_compare <- monthly_summary |>
  filter(
    fiscal_year == fy_selected,
    csf_pmosource %in% pmosource_selected,
    LineCategory == line_category_selected
  ) |>
  group_by(fiscal_year, month_date, phase_type, phase) |>
  summarise(
    AllocationAmount = sum(AllocationAmount, na.rm = TRUE),
    .groups = "drop"
  )

# --- 4b. Plot: facet by phase_type, fill by phase ---
ggplot(
  plot_data_compare,
  aes(x = month_date, y = AllocationAmount, fill = phase)
) +
  geom_col() +
  facet_wrap(~phase_type, ncol = 1) +
  scale_x_date(date_labels = "%b %Y", date_breaks = "1 month") +
  scale_y_continuous(labels = label_currency(scale = 1e-6, suffix = "M")) +
  scale_fill_manual(values = phase_colors, drop = FALSE) +
  labs(
    title = paste0(
      line_category_selected,
      " — project_phase vs Milestone_Phase — ",
      fy_selected
    ),
    subtitle = if (
      length(pmosource_selected) < length(unique(data_prepped$csf_pmosource))
    ) {
      paste("PMO Source:", paste(pmosource_selected, collapse = ", "))
    } else {
      "All PMO Sources"
    },
    x = NULL,
    y = "Sum of Allocation Amount",
    fill = "Phase"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1),
    legend.position = "bottom"
  )
