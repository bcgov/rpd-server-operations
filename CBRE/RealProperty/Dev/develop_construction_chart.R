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

# query <- dbSendQuery(con, "SELECT * FROM CbreStaging.pjm_dim_invoice")
# DimInvoiceData <- dbFetch(query, n = -1)
# dbClearResult(query)
#
# query <- dbSendQuery(con, "SELECT * FROM CbreStaging.pjm_fact_invoice")
# FactInvoiceData <- dbFetch(query, n = -1)
# dbClearResult(query)

# query <- dbSendQuery(con, "SELECT * FROM CbreStaging.pjm_dim_project_activity")
# DimActivityData <- dbFetch(query, n = -1)
# dbClearResult(query)
#
# query <- dbSendQuery(con, "SELECT * FROM CbreStaging.pjm_fact_project_activity")
# FactActivityData <- dbFetch(query, n = -1)
# dbClearResult(query)

Output <- FactProjData |>
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
  select(
    project_skey,
    milestone_desc,
    CBRE_Proj_Milestone,
    CBRE_Proj_Phase,
    estimated_start_date,
    actual_start_date
  ) |>
  mutate(
    PhaseStartDate = case_when(
      is.na(actual_start_date) ~ estimated_start_date,
      .default = actual_start_date
    ),
    .keep = "unused"
  )

test_multiple_dates <- milestones %>%
  filter(!is.na(PhaseStartDate)) %>%
  group_by(project_skey, CBRE_Proj_Phase) |>
  mutate(count = n()) |>
  filter(count >= 2)

milestone_phases <- milestones |>
  group_by(project_skey, CBRE_Proj_Phase) |>
  arrange(is.na(PhaseStartDate), PhaseStartDate, .by_group = TRUE) |>
  slice(1) |>
  ungroup()

sum(is.na(milestone_phases$PhaseStartDate))

pivot_wider(
  id_cols = project_skey,
  names_from = CBRE_Proj_Phase,
  values_from = StartDate,
  names_glue = "{stringr::str_remove_all(milestone_desc, ' ')}{'StartDate'}"
) |>
  relocate(
    AuthorizationtoProceedStartDate,
    FeasibilityCompleteStartDate,
    DesignCompleteStartDate,
    CommenceConstructionStartDate,
    SubstantialCompletionStartDate,
    DeficienciesListCompleteStartDate,
    FacilityOpenforBusinessStartDate,
    TechnicalCloseoutStartDate,
    ProjectCloseoutStartDate,
    .after = project_skey
  )


OutputTable <- Output |>
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
  group_by(project_phase, project_status, LineCategory, AllocationDate) |>
  summarise(
    AllocationAmount = sum(AllocationAmount),
    .groups = "drop_last"
  ) |>
  ungroup() |>
  mutate(
    MonthLabel = format(AllocationDate, "%b %Y"),
    MonthLabel = factor(
      MonthLabel,
      levels = format(
        seq(as.Date("2026-04-01"), as.Date("2027-03-01"), by = "month"),
        "%b %Y"
      )
    )
  ) |>
  group_by(MonthLabel, LineCategory) |>
  summarise(AllocationAmount = sum(AllocationAmount), .groups = "drop")

# --- Plot 1: Grouped (dodged) bar chart, all categories side-by-side per month ---
ggplot(
  OutputTable,
  aes(x = MonthLabel, y = AllocationAmount, fill = LineCategory)
) +
  geom_col(position = position_dodge(width = 0.8), width = 0.7) +
  scale_y_continuous(labels = scales::label_currency()) +
  labs(
    title = "FY2026-27 Monthly Allocations by Category",
    x = "Month",
    y = "Allocation Amount",
    fill = "Line Category"
  ) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

# --- Plot 2: Faceted, one panel per LineCategory ---
ggplot(
  OutputTable,
  aes(x = MonthLabel, y = AllocationAmount, fill = LineCategory)
) +
  geom_col(width = 0.7, show.legend = FALSE) +
  facet_wrap(~LineCategory, ncol = 1, scales = "free_y") +
  scale_y_continuous(labels = scales::label_currency()) +
  labs(
    title = "FY2026-27 Monthly Allocations",
    x = "Month",
    y = "Allocation Amount"
  ) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))


# Add phase component
OutputTable2 <- Output |>
  filter(project_skey %in% milestones$project_skey) |>
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
  mutate(
    MonthLabel = format(AllocationDate, "%b %Y"),
    MonthLabel = factor(
      MonthLabel,
      levels = format(
        seq(as.Date("2026-04-01"), as.Date("2027-03-01"), by = "month"),
        "%b %Y"
      )
    ),
    project_phase = factor(
      project_phase,
      levels = c(
        "Strategic Planning",
        "Initiation",
        "Planning",
        "Feasibility",
        "Design",
        "Pre-Tender",
        "Tender",
        "Construction",
        "Closeout",
        "Warranty Period"
      )
    )
  ) |>
  group_by(MonthLabel, LineCategory, project_phase) |>
  summarise(AllocationAmount = sum(AllocationAmount), .groups = "drop")


# --- Plot 2 (updated): Stacked bars by project_phase, faceted by LineCategory ---
ggplot(
  OutputTable2,
  aes(x = MonthLabel, y = AllocationAmount, fill = project_phase)
) +
  geom_col(width = 0.7) +
  facet_wrap(~LineCategory, ncol = 1, scales = "free_y") +
  scale_y_continuous(labels = label_dollar()) +
  labs(
    title = "FY2026-27 Monthly Allocations by Project Phase",
    x = "Month",
    y = "Allocation Amount",
    fill = "Project Phase"
  ) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))


library(dplyr)
library(ggplot2)
library(scales)

# Set the order for LineCategory (dodge order within each month)
line_category_levels <- c("Budget", "Forecast", "Actuals")

plot_data <- OutputTable2 |>
  mutate(
    LineCategory = factor(LineCategory, levels = line_category_levels),
    month_index = as.numeric(MonthLabel) # numeric position for each month
  ) |>
  # Stack project_phase within each MonthLabel/LineCategory bar
  arrange(MonthLabel, LineCategory, project_phase) |>
  group_by(MonthLabel, LineCategory) |>
  mutate(
    ymax = cumsum(AllocationAmount),
    ymin = ymax - AllocationAmount
  ) |>
  ungroup() |>
  # Dodge offset: spread LineCategory bars within each month
  mutate(
    n_cat = length(line_category_levels),
    bar_width = 0.25,
    cat_index = as.numeric(LineCategory),
    x_offset = (cat_index - (n_cat + 1) / 2) * bar_width,
    xmin = month_index + x_offset - bar_width / 2,
    xmax = month_index + x_offset + bar_width / 2
  )

ggplot(plot_data) +
  geom_rect(
    aes(
      xmin = xmin,
      xmax = xmax,
      ymin = ymin,
      ymax = ymax,
      fill = project_phase,
      color = LineCategory
    ),
    linewidth = 0.6
  ) +
  scale_color_manual(
    values = c(
      "Budget" = "grey20",
      "Forecast" = "steelblue",
      "Actuals" = "black"
    )
  ) +
  scale_x_continuous(
    breaks = seq_along(levels(plot_data$MonthLabel)),
    labels = levels(plot_data$MonthLabel)
  ) +
  scale_y_continuous(labels = label_currency()) +
  labs(
    title = "FY2026-27 Monthly Allocations by Line Category and Project Phase",
    x = "Month",
    y = "Allocation Amount",
    fill = "Project Phase"
  ) +
  theme_minimal() +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1),
    panel.grid.minor.x = element_blank()
  )

# Alternative option

# One row per bar (for the icon layer + total bar height)
bar_tops <- plot_data |>
  group_by(MonthLabel, LineCategory, month_index, xmin, xmax, cat_index) |>
  summarise(bar_total = max(ymax), .groups = "drop") |>
  mutate(x_center = (xmin + xmax) / 2)

# Icon offset above the tallest bar so it doesn't feel cramped per-bar
icon_y <- max(bar_tops$bar_total) * 1.05
bar_tops <- bar_tops |>
  mutate(icon_y = bar_total + (max(bar_tops$bar_total) * 0.03))

ggplot() +
  geom_rect(
    data = plot_data,
    aes(
      xmin = xmin,
      xmax = xmax,
      ymin = ymin,
      ymax = ymax,
      fill = project_phase
    ),
    color = "white",
    linewidth = 0.1
  ) +
  geom_point(
    data = bar_tops,
    aes(x = x_center, y = icon_y, shape = LineCategory),
    size = 3,
    stroke = 1
  ) +
  scale_shape_manual(
    values = c(
      "Budget" = 22, # square
      "Forecast" = 24, # triangle
      "Actuals" = 21 # circle
    )
  ) +
  scale_x_continuous(
    breaks = seq_along(levels(plot_data$MonthLabel)),
    labels = levels(plot_data$MonthLabel),
    expand = expansion(mult = 0.02)
  ) +
  scale_y_continuous(
    labels = label_dollar(),
    expand = expansion(mult = c(0, 0.08))
  ) +
  labs(
    title = "FY2026-27 Monthly Allocations by Line Category and Project Phase",
    x = "Month",
    y = "Allocation Amount",
    fill = "Project Phase",
    shape = "Line Category"
  ) +
  theme_minimal() +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1),
    panel.grid.minor.x = element_blank(),
    plot.margin = margin(t = 10, r = 10, b = 10, l = 10)
  ) +
  guides(fill = guide_legend(order = 1), shape = guide_legend(order = 2))


library(dplyr)
library(ggplot2)
library(scales)

# Aggregate by month + LineCategory (collapsing project_phase), then cumsum
cumulative_data <- OutputTable2 |>
  group_by(MonthLabel, LineCategory) |>
  summarise(AllocationAmount = sum(AllocationAmount), .groups = "drop") |>
  arrange(LineCategory, MonthLabel) |>
  group_by(LineCategory) |>
  mutate(CumulativeAmount = cumsum(AllocationAmount)) |>
  ungroup() |>
  mutate(
    LineCategory = factor(
      LineCategory,
      levels = c("Budget", "Forecast", "Actuals")
    )
  )

ggplot(
  cumulative_data,
  aes(
    x = MonthLabel,
    y = CumulativeAmount,
    color = LineCategory,
    group = LineCategory
  )
) +
  geom_line(linewidth = 1) +
  geom_point(size = 2) +
  geom_text(
    aes(
      label = label_currency(scale = 1 / 1000, suffix = "K")(CumulativeAmount)
    ),
    vjust = -1,
    size = 3,
    show.legend = FALSE
  ) +
  scale_color_manual(
    values = c(
      "Budget" = "grey40",
      "Forecast" = "steelblue",
      "Actuals" = "firebrick"
    )
  ) +
  scale_y_continuous(labels = label_dollar()) +
  labs(
    title = "FY2026-27 Cumulative Allocations",
    x = "Month",
    y = "Cumulative Allocation Amount",
    color = "Line Category"
  ) +
  theme_minimal() +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1),
    panel.grid.minor.x = element_blank()
  )
