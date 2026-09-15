get_etl_window <- function(today = Sys.Date()) {
  day_of_week <- weekdays(today)

  lookback_days <- if (day_of_week == "Monday") 3L else 1L

  list(
    cbre_start_time = paste0(today - lookback_days, "T00:00:00Z"),
    cbre_end_time = paste0(
      today,
      "T",
      format(lubridate::with_tz(Sys.time(), tzone = "UTC"), "%H:%M:%S"),
      "Z"
    ),
    jira_start_time = paste(today - lookback_days, "00:00")
  )
}
