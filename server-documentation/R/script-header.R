#' @title TODO short human-readable name
#'
#' @description
#' TODO What this table is for and how it is meant to be used. Several lines
#' are fine; a blank `#'` line starts a new paragraph. Markdown is allowed.
#'
#' @database  TODO e.g. BuildingIntelligence
#' @schema    TODO e.g. RealProperty
#' @table     TODO e.g. ActivityCodeReport
#'
#' @source    Schema.Table  optional note on how it is used
#' @source    Schema.Table
#'
#' @load_type TODO full reload | incremental | SCD2
#' @grain     TODO what one row represents
#'
#' @filters   TODO a business rule or filter applied (repeat the tag per rule)
#'
#' @consumers TODO main report/dashboard/app this table was built for
#'
#' @depends_on TODO (optional) anything that must run first
#'
#' @column    ColumnName  Description, only for columns that need explaining
#'
#' @notes     TODO (optional) caveats, known quirks
#'
#' @owner     TODO
#' @last_reviewed YYYY-MM-DD
#'
# ---------------------------------------------------------------------------
# Notes on the header (delete this block in real scripts)
#  - The header is the first block of consecutive #' lines in the file.
#  - Required: title, description, database, schema, table, source,
#              load_type, grain, owner, last_reviewed
#  - Repeatable: source, filters, consumers, depends_on, column, notes
#  - A tag's value can continue on following #' lines (indent them).
#  - Column names, types and nullability are read from the script's own
#    CREATE TABLE statement. Do not repeat them here.
#  - Anything still containing TODO is reported as a warning at build time.
# ---------------------------------------------------------------------------
