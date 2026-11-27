# For server logging
# Begin timer
task_start <- Sys.time()

# gentle update
# Set necessary variables
etl_status <- "PROD"
sql_server <- if (etl_status == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
db_name <- "BuildingIntelligence"
schema_name <- "Jira"
dry_run <- TRUE   # change to TRUE if you want to preview issues table without writing to SQL

# Connect to SQL database
con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = sql_server,
  database = db_name,
  Trusted_Connection = "Yes"
)

dashboard_id <- "IMS"
target_table <- DBI::Id(schema = schema_name, table = dashboard_id)
temp_table <- paste0("#", dashboard_id, "Temp")
api_name <- "Jira"
script_name <- "PROD_Jira_IMS"

# Setup API parameters ####
expand_opts = c("names", "fields")
max_results = 100
start_time <- etl_window$jira_start_time

# Issues Loop ####
data <- call_jira_api(
  api_name,
  script_name,
  dashboard_id,
  query_url,
  expand_opts,
  max_results,
  token_string,
  start_time
)

if (length(data$issues) == 0) {
  # API succeeded, nothing to load
  no_data_msg <- paste0(
    "No data returned from API for window ",
    etl_window$jira_start_time,
    " to ",
    format(Sys.time(), tz = "UTC"),
    " UTC"
  )

  cat(no_data_msg, "— nothing to load. Exiting gracefully.\n")

  log_daily_etl_run(
    api_name = api_name,
    script_name = script_name,
    table_name = dashboard_id,
    duration = as.numeric(difftime(Sys.time(), task_start, units = "secs")),
    status = "NO_DATA",
    message = no_data_msg
  )
  cond <- structure(
    class = c("no_data_condition", "condition"),
    list(message = no_data_msg)
  )
  stop(cond)
}

# tryCatch(
#   {
names <- data |>
  purrr::pluck("names") |>
  tibble::enframe() |>
  safe_hoist(value, Value = 1L) |>
  group_by(Value) |>
  mutate(row_name = row_number(), row_count = n()) |>
  mutate(
    Value = case_when(
      row_count > 1 ~ paste0(Value, "-", row_name),
      .default = Value
    )
  ) |>
  select(-c(row_name, row_count)) |>
  tibble::deframe()

Issues <- data |>
  purrr::pluck("issues") |>
  tibble::enframe() |>
  tidyr::unnest_wider(value) |>
  tidyr::unnest_wider(fields) |>
  plyr::rename(names) |>
  rename_with(~ gsub(" ", "", .)) |>
  select(
    Assignee,
    IssueKey = key,
    IssueType,
    RequestType,
    Resolution,
    Reporter,
    Created,
    Updated,
    Resolved,
    Duedate,
    Status,
    StatusCategory = Statuscategory,
    StatusCategoryChanged = Statuscategorychanged,
    InterfaceType,
    InfTeam = `INFDivision/Branch`,
    KahuaRef = `KahuaReference#`,
    RequestParticipants = Requestparticipants,
    Urgency,
    PONumber,
    InvoiceNumber,
    Comment,
    Labels,
    Summary
  ) |>
  safe_hoist(InterfaceType, InterfaceType = "value", .remove = FALSE) |>
  filter(InterfaceType %in% c("D10", "D11")) |>
  safe_hoist(Assignee, Assignee = "displayName", .remove = FALSE) |>
  safe_hoist(IssueType, IssueType = "name", .remove = FALSE) |>
  safe_hoist(
    RequestType,
    RequestType = list("requestType", "name"),
    .remove = FALSE
  ) |>
  safe_hoist(Resolution, Resolution = "name", .remove = FALSE) |>
  safe_hoist(Reporter, Reporter = "displayName", .remove = FALSE) |>
  safe_hoist(StatusCategory, StatusCategory = "name", .remove = FALSE) |>
  safe_hoist(Status, Status = "name", .remove = FALSE) |>
  safe_hoist(InfTeam, InfDivision = "value", .remove = FALSE) |>
  safe_hoist(InfTeam, InfBranch = c("child", "value"), .remove = FALSE) |>
  mutate(
    RequestParticipants = purrr::map_chr(RequestParticipants, \(x) {
      n <- purrr::map_chr(
        x,
        \(p) purrr::pluck(p, "displayName", .default = NA_character_)
      )
      n <- n[!is.na(n)]
      if (length(n) == 0) NA_character_ else paste(n, collapse = "; ")
    })
  ) |>
  safe_hoist(Urgency, Urgency = "value", .remove = FALSE) |>
  mutate(
    Labels = purrr::map_chr(Labels, \(x) {
      n <- unlist(x)
      if (length(n) == 0) NA_character_ else paste(n, collapse = "; ")
    })
  ) |>
  select(-c(InfTeam))

parse_jira_dt <- function(x) {
  as.POSIXct(x, tz = "UTC", format = "%Y-%m-%dT%H:%M:%OS%z")
}

# Convert Jira's rich-text (Atlassian Document Format) comment body to plain text
adf_to_text <- function(node) {
  if (is.null(node)) return(NA_character_)
  if (is.character(node)) return(paste(node, collapse = ""))
  type <- node$type
  if (identical(type, "text")) return(node$text %||% "")
  if (identical(type, "hardBreak")) return("\n")
  if (identical(type, "mention")) return(node$attrs$text %||% "")
  kids <- purrr::map_chr(node$content %||% list(), adf_to_text)
  out <- paste(kids, collapse = "")
  if (type %in% c("paragraph", "heading", "listItem", "codeBlock")) out <- paste0(out, "\n")
  out
}

Comments <- Issues |>
  select(IssueKey, Comment) |>
  mutate(Comment = purrr::map(Comment, "comments")) |>
  tidyr::unnest_longer(Comment) |>
  mutate(
    CommentID   = purrr::map_chr(Comment, \(x) purrr::pluck(x, "id", .default = NA_character_)),
    Author      = purrr::map_chr(Comment, \(x) purrr::pluck(x, "author", "displayName", .default = NA_character_)),
    Created     = parse_jira_dt(purrr::map_chr(Comment, \(x) purrr::pluck(x, "created", .default = NA_character_))),
    Updated     = parse_jira_dt(purrr::map_chr(Comment, \(x) purrr::pluck(x, "updated", .default = NA_character_))),
    IsPublic    = purrr::map_lgl(Comment, \(x) purrr::pluck(x, "jsdPublic", .default = NA)),
    CommentBody = trimws(purrr::map_chr(Comment, \(x) adf_to_text(x$body))),
    RefreshDate = Sys.time()
  ) |>
  select(RefreshDate, CommentID, IssueKey, Author, Created, Updated, CommentBody)

Issues <- Issues |>
  select(-any_of(c("Comment"))) |>
  mutate(
    across(c(Created, Updated, Resolved, StatusCategoryChanged), parse_jira_dt),
    Duedate = as.Date(Duedate, format = "%Y-%m-%d"),
    TimeToCompletion = as.numeric(difftime(Resolved, Created, units = "hours")),
    RefreshDate = Sys.time()
  ) |>
  select(
    RefreshDate, IssueKey, IssueType, RequestType, Status, StatusCategory,
    StatusCategoryChanged, Created, Updated, Resolved, Duedate, Assignee,
    Reporter, RequestParticipants, Resolution, InterfaceType, InfDivision, InfBranch, KahuaRef,
    Urgency, PONumber, InvoiceNumber, Labels, Summary, TimeToCompletion
  )

tryCatch(
  {
    Issues <- Issues |> mutate(RefreshDate = Sys.time(), .before = everything())
  },
  error = function(e) {
    log_daily_etl_run(
      api_name = api_name,
      script_name = script_name,
      table_name = dashboard_id,
      status = "FAILURE",
      message = paste0(
        "Issues assignment failure: ",
        substr(conditionMessage(e), 1, 500)
      )
    )
    stop(e) # rethrow so Task Scheduler/Nagios still flags it
  }
)

if (dry_run) {
  cat("DRY RUN:", nrow(Issues), "rows,", ncol(Issues), "columns\n")
  str(Issues)
  print(head(Issues, 20))
  View(Issues)
  write.csv(Issues, "issues_preview.csv", row.names = FALSE)
  print(head(Comments, 20))
  View(Comments)
  stop("Dry run finished. Nothing was written to the database.")
}

# Start database transaction ####
full_target <- paste0(schema_name, ".", dashboard_id)

col_defs <- "
  RefreshDate            DATETIME2(3)    NOT NULL,
  IssueKey               NVARCHAR(20)    NOT NULL,
  IssueType              NVARCHAR(100)   NULL,
  RequestType            NVARCHAR(100)   NULL,
  Status                 NVARCHAR(100)   NULL,
  StatusCategory         NVARCHAR(100)   NULL,
  StatusCategoryChanged  DATETIME2(3)    NULL,
  Created                DATETIME2(3)    NULL,
  Updated                DATETIME2(3)    NULL,
  Resolved               DATETIME2(3)    NULL,
  Duedate                DATE            NULL,
  Assignee               NVARCHAR(100)   NULL,
  Reporter               NVARCHAR(100)   NULL,
  RequestParticipants    NVARCHAR(1000)   NULL,
  Resolution             NVARCHAR(100)   NULL,
  InterfaceType          NVARCHAR(50)    NULL,
  InfDivision            NVARCHAR(100)   NULL,
  InfBranch              NVARCHAR(100)   NULL,
  KahuaRef               NVARCHAR(100)   NULL,
  Urgency                NVARCHAR(100)   NULL,
  PONumber               NVARCHAR(100)   NULL,
  InvoiceNumber          NVARCHAR(100)   NULL,
  Labels                 NVARCHAR(1000)   NULL,
  Summary                NVARCHAR(1000)  NULL,
  TimeToCompletion       DECIMAL(18,7)   NULL
"

if (!dbExistsTable(con, target_table)) {
  dbExecute(con, paste0("CREATE TABLE ", full_target, " (", col_defs, ");"))
}

# ---- Comments table setup ----
comments_table <- "JIRA_IMS_Comments"
full_comments  <- paste0(schema_name, ".", comments_table)
temp_comments  <- paste0("#", comments_table, "Temp")

comment_col_defs <- "
  RefreshDate   DATETIME2(3)   NOT NULL,
  CommentID     NVARCHAR(20)   NOT NULL,
  IssueKey      NVARCHAR(20)   NOT NULL,
  Author        NVARCHAR(100)  NULL,
  Created       DATETIME2(3)   NULL,
  Updated       DATETIME2(3)   NULL,
  CommentBody   NVARCHAR(MAX)  NULL
"

if (!dbExistsTable(con, DBI::Id(schema = schema_name, table = comments_table))) {
  dbExecute(con, paste0(
    "CREATE TABLE ", full_comments, " (", comment_col_defs,
    ", CONSTRAINT PK_", comments_table, " PRIMARY KEY (CommentID));"
  ))
}

etl_error <- NULL
dbBegin(con)

tryCatch(
  {
    if (dbExistsTable(con, temp_table)) {
      dbRemoveTable(con, temp_table)
    }

    dbExecute(con, paste0("CREATE TABLE ", temp_table, " (", col_defs, ");"))

    dbWriteTable(con, name = temp_table, value = Issues, append = TRUE, overwrite = FALSE)

    dup_count <- dbGetQuery(con, paste0(
      "SELECT COUNT(*) AS n FROM (SELECT IssueKey FROM ", temp_table,
      " GROUP BY IssueKey HAVING COUNT(*) > 1) dupes;"
    ))$n

    if (dup_count > 0) {
      stop(paste0("Duplicate IssueKey values detected (", dup_count, " keys). Rolling back."))
    }

    non_key_cols <- setdiff(names(Issues), "IssueKey")
    set_clause <- paste0("tgt.", non_key_cols, " = src.", non_key_cols, collapse = ",\n")

    n_updated <- dbExecute(con, paste0(
      "UPDATE tgt SET ", set_clause,
      " FROM ", full_target, " tgt INNER JOIN ", temp_table,
      " src ON tgt.IssueKey = src.IssueKey;"
    ))

    all_cols <- paste(names(Issues), collapse = ", ")
    src_cols <- paste0("src.", names(Issues), collapse = ", ")

    n_inserted <- dbExecute(con, paste0(
      "INSERT INTO ", full_target, " (", all_cols, ") SELECT ", src_cols,
      " FROM ", temp_table, " src LEFT JOIN ", full_target,
      " tgt ON tgt.IssueKey = src.IssueKey WHERE tgt.IssueKey IS NULL;"
    ))

    # ---- Comments (same transaction as Issues) ----
    n_comments_updated <- 0
    n_comments_inserted <- 0

    if (nrow(Comments) > 0) {
      if (dbExistsTable(con, temp_comments)) dbRemoveTable(con, temp_comments)
      dbExecute(con, paste0("CREATE TABLE ", temp_comments, " (", comment_col_defs, ");"))
      dbWriteTable(con, name = temp_comments, value = Comments, append = TRUE, overwrite = FALSE)

      c_cols <- setdiff(names(Comments), "CommentID")
      c_set <- paste0("tgt.", c_cols, " = src.", c_cols, collapse = ",\n")

      n_comments_updated <- dbExecute(con, paste0(
        "UPDATE tgt SET ", c_set, " FROM ", full_comments, " tgt INNER JOIN ",
        temp_comments, " src ON tgt.CommentID = src.CommentID;"
      ))

      n_comments_inserted <- dbExecute(con, paste0(
        "INSERT INTO ", full_comments, " (", paste(names(Comments), collapse = ", "), ") ",
        "SELECT ", paste0("src.", names(Comments), collapse = ", "),
        " FROM ", temp_comments, " src LEFT JOIN ", full_comments,
        " tgt ON tgt.CommentID = src.CommentID WHERE tgt.CommentID IS NULL;"
      ))
    }

    # Complete the transaction
    dbCommit(con)

    # Hoist counts to outer scope for logging
    n_updated <<- n_updated
    n_inserted <<- n_inserted

    cat(
      script_name,
      " ETL complete — updated:", n_updated,
      "| inserted:", n_inserted,
      "| comments updated:", n_comments_updated,
      "| comments inserted:", n_comments_inserted,
      "\n"
    )
    # rollback transaction on fail, completion of error handling
  },
  error = function(e) {
    dbRollback(con)
    etl_error <<- e
  }
)

task_end <- Sys.time()
task_duration <- interval(task_start, task_end) / dseconds()

if (is.null(etl_error)) {
  log_daily_etl_run(
    api_name = api_name,
    script_name = script_name,
    table_name = dashboard_id,
    duration = task_duration,
    status = "SUCCESS",
    n_inserted = n_inserted,
    n_updated = n_updated,
    n_deleted = NA,
    message = "ETL completed successfully"
  )
} else {
  log_daily_etl_run(
    api_name = api_name,
    script_name = script_name,
    table_name = dashboard_id,
    status = "FAILURE",
    message = substr(etl_error$message, 1, 500)
  )
  stop(etl_error)
}
