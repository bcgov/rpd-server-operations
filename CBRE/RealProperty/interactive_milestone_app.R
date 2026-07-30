# app.R
# ---------------------------------------------------------------------------
# Interactive Milestone Allocation Dashboard
#
# One bar chart per tab (faceted out as requested):
#   - Tab 1: bars stacked by project_phase
#   - Tab 2: bars stacked by Milestone_Phase
# Each month shows 3 dodged bars (Budget / Forecast / Actuals), each stacked
# by phase, with a small marker symbol above each bar identifying its
# LineCategory.
#
# Filters: multi-select (with select-all) on csf_pmosource and on fiscal year
# (Apr 1 - Mar 31), applied to both tabs.
#
# Required packages:
#   install.packages(c("shiny", "shinyWidgets", "dplyr", "tidyr", "lubridate",
#                       "plotly", "RColorBrewer", "forcats", "rlang"))
# ---------------------------------------------------------------------------

library(shiny)
library(shinyWidgets)
library(dplyr)
library(tidyr)
library(lubridate)
library(plotly)
library(RColorBrewer)
library(forcats)
library(rlang)
library(odbc)
library(DBI)

# ---------------------------------------------------------------------------
# 1. DATA PREP ---------------------------------------------------------------
# ---------------------------------------------------------------------------
# `data` is assumed to already exist in your R environment (per the str()
# you shared). If running this app standalone, load it here instead, e.g.:
# data <- readRDS("milestone_data.rds")
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
  "SELECT *
  FROM RealProperty.PjmMilestonePhase"
)
PjmMilestonePhase <- dbFetch(query, n = -1)
dbClearResult(query)


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

data <- PjmMilestonePhase |>
  mutate(
    project_phase = factor(project_phase, levels = phase_order),
    Milestone_Phase = factor(Milestone_Phase, levels = phase_order)
  )

data <- data %>%
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
    ),
    project_phase = fct_explicit_na(project_phase, na_level = "Unknown"),
    Milestone_Phase = fct_explicit_na(Milestone_Phase, na_level = "Unknown")
  )

# Shared phase levels/colors so project_phase & Milestone_Phase use identical
# colour coding across both tabs
phase_levels <- union(levels(data$project_phase), levels(data$Milestone_Phase))
phase_colors <- setNames(
  colorRampPalette(brewer.pal(8, "Set2"))(length(phase_levels)),
  phase_levels
)

# LineCategory -> marker symbol (identifies which bar is which)
lc_levels <- levels(data$LineCategory)
lc_symbols <- setNames(c("square", "diamond", "circle"), lc_levels)
lc_offset <- setNames(
  seq(-0.27, 0.27, length.out = length(lc_levels)),
  lc_levels
)

pmosource_choices <- sort(unique(data$csf_pmosource))
fiscal_year_choices <- sort(unique(data$fiscal_year), decreasing = TRUE)

# ---------------------------------------------------------------------------
# 2. UI -----------------------------------------------------------------
# ---------------------------------------------------------------------------
ui <- fluidPage(
  titlePanel("Milestone Allocation Dashboard"),
  sidebarLayout(
    sidebarPanel(
      pickerInput(
        inputId = "pmosource",
        label = "PMO Source (csf_pmosource)",
        choices = pmosource_choices,
        selected = pmosource_choices,
        multiple = TRUE,
        options = pickerOptions(
          actionsBox = TRUE,
          liveSearch = TRUE,
          selectedTextFormat = "count > 3",
          size = 10
        )
      ),
      pickerInput(
        inputId = "fiscal_year",
        label = "Fiscal Year (Apr 1 - Mar 31)",
        choices = fiscal_year_choices,
        selected = fiscal_year_choices,
        multiple = TRUE,
        options = pickerOptions(
          actionsBox = TRUE,
          selectedTextFormat = "count > 3",
          size = 10
        )
      ),
      width = 3
    ),
    mainPanel(
      tabsetPanel(
        tabPanel(
          "By Project Phase",
          plotlyOutput("plot_project_phase", height = "650px")
        ),
        tabPanel(
          "By Milestone Phase",
          plotlyOutput("plot_milestone_phase", height = "650px")
        )
      ),
      width = 9
    )
  )
)

# ---------------------------------------------------------------------------
# 3. HELPER: build a dodge+stack plotly figure -------------------------------
# ---------------------------------------------------------------------------
build_dodge_stack_plot <- function(df, phase_col, chart_title) {
  phase_sym <- sym(phase_col)

  summarized <- df %>%
    group_by(month_date, LineCategory, phase = !!phase_sym) %>%
    summarise(
      AllocationAmount = sum(AllocationAmount, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    complete(month_date, LineCategory, phase, fill = list(AllocationAmount = 0))

  if (nrow(summarized) == 0) {
    return(
      plotly_empty(type = "bar") %>%
        layout(title = "No data for current filter selection")
    )
  }

  months_sorted <- sort(unique(summarized$month_date))
  month_index <- setNames(seq_along(months_sorted), as.character(months_sorted))

  summarized <- summarized %>%
    mutate(
      x_base = month_index[as.character(month_date)],
      x_pos = x_base + lc_offset[as.character(LineCategory)]
    )

  totals <- summarized %>%
    group_by(LineCategory, x_pos) %>%
    summarise(total = sum(AllocationAmount), .groups = "drop")

  fig <- plot_ly()

  # Stacked bars, one trace per phase (color = phase)
  for (ph in levels(summarized$phase)) {
    d <- summarized %>% filter(phase == ph)
    if (nrow(d) == 0 || all(d$AllocationAmount == 0)) {
      next
    }
    fig <- fig %>%
      add_trace(
        data = d,
        x = ~x_pos,
        y = ~AllocationAmount,
        type = "bar",
        name = ph,
        marker = list(color = phase_colors[[ph]]),
        legendgroup = ph,
        customdata = ~ as.character(LineCategory),
        hovertemplate = paste0(
          "Phase: ",
          ph,
          "<br>Category: %{customdata}",
          "<br>Amount: $%{y:,.0f}<extra></extra>"
        )
      )
  }

  # Marker symbol above each bar, one trace per LineCategory
  for (lc in lc_levels) {
    d <- totals %>% filter(LineCategory == lc)
    if (nrow(d) == 0) {
      next
    }
    fig <- fig %>%
      add_trace(
        data = d,
        x = ~x_pos,
        y = ~ total * 1.03,
        type = "scatter",
        mode = "markers",
        marker = list(symbol = lc_symbols[[lc]], size = 11, color = "black"),
        name = paste0(lc, " (marker)"),
        legendgroup = paste0("lc_", lc),
        customdata = ~total,
        hovertemplate = paste0(lc, ": $%{customdata:,.0f}<extra></extra>")
      )
  }

  fig %>%
    layout(
      title = chart_title,
      barmode = "stack",
      xaxis = list(
        title = "Month",
        tickvals = unname(month_index),
        ticktext = format(months_sorted, "%b %Y"),
        tickangle = -45
      ),
      yaxis = list(title = "Sum of Allocation Amount", tickformat = "$,.0f"),
      legend = list(title = list(text = "Phase / Category marker"))
    )
}

# ---------------------------------------------------------------------------
# 4. SERVER ---------------------------------------------------------------
# ---------------------------------------------------------------------------
server <- function(input, output, session) {
  filtered_data <- reactive({
    req(input$pmosource, input$fiscal_year)
    data %>%
      filter(
        csf_pmosource %in% input$pmosource,
        fiscal_year %in% input$fiscal_year
      )
  })

  output$plot_project_phase <- renderPlotly({
    build_dodge_stack_plot(
      filtered_data(),
      "project_phase",
      "Allocation by Month \u2014 stacked by Project Phase"
    )
  })

  output$plot_milestone_phase <- renderPlotly({
    build_dodge_stack_plot(
      filtered_data(),
      "Milestone_Phase",
      "Allocation by Month \u2014 stacked by Milestone Phase"
    )
  })
}

shinyApp(ui, server)
