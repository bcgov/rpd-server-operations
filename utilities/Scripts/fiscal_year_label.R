fiscal_year_label <- function(date) {
  yr <- year(date)
  mo <- month(date)

  # If before April, fiscal year started in the previous calendar year
  fy_start <- if_else(mo < 4, yr - 1L, yr)
  fy_end <- fy_start + 1L

  sprintf("FY%02d%02d", fy_start %% 100, fy_end %% 100)
}
