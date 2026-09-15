etl_stage <- function(stage_name, expr) {
  withCallingHandlers(
    expr,
    error = function(e) {
      rlang::abort(
        conditionMessage(e),
        class = "etl_stage_error",
        stage = stage_name,
        parent = e
      )
    }
  )
}
