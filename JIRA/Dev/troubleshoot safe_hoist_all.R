source(here::here("utilities/R/utilities.R"))

# From SBP
test_set <- issues[26:45, 17:18]

output <- test_set |>
  safe_hoist_all(
    RequestParticipants,
    RequestParticipants = list("displayName")
  )

safe_hoist_all <- function(.data, .col, ..., sep = ";", .remove = FALSE) {
  .col_name <- tidyselect::vars_pull(names(.data), {{ .col }})
  col_data <- .data[[.col_name]]

  col_data
}


test_set |>
  safe_hoist_all(
    RequestParticipants,
    Output = list("displayName")
  )

.col_name <- tidyselect::vars_pull(names(test_set), RequestParticipants)
col_data <- test_set[[.col_name]]

.col_name
typeof(.col_name)

col_data

dot_args <- list(Output = list("displayName"))
if (
  length(dot_args) == 0 ||
    is.null(names(dot_args)) ||
    any(names(dot_args) == "")
) {
  rlang::abort(
    "All `...` arguments must be named. Use `new_col_name = path` syntax."
  )
}

names(dot_args)

for (new_col_name in names(dot_args)) {
  path <- dot_args[[new_col_name]]
  print(path)
}

.col_name <- tidyselect::vars_pull(names(test_set), RequestParticipants)

print(.col_name)
names(test_set)
which(names(test_set) == .col_name)

dot_args <- list(Output = list("displayName"))
for (new_col_name in names(dot_args)) {
  path <- dot_args[[new_col_name]]
  cat("new_col_name:", new_col_name, "\n")
  cat("last_col class:", class(last_col), "\n")
  cat("last_col typeof:", typeof(last_col), "\n")
  cat("last_col value:", last_col, "\n")
}
