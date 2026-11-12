# parse_headers.R
# Reads the Roxygen-style (#') header at the top of an ETL script, pulls column
# definitions from its CREATE TABLE statement, and cross-checks the header
# against the code. Base R only (no package dependencies).

REQUIRED_TAGS   <- c("title", "description", "database", "schema", "table",
                     "source", "load_type", "grain", "owner", "last_reviewed")
REPEATABLE_TAGS <- c("source", "filters", "consumers", "depends_on",
                     "column", "notes")
MULTILINE_TAGS  <- c("description", "notes")   # keep line breaks
KNOWN_TAGS      <- union(REQUIRED_TAGS, REPEATABLE_TAGS)

`%||%` <- function(a, b) if (is.null(a) || length(a) == 0L) b else a

# ---- Header --------------------------------------------------------------

parse_header <- function(path) {
  lines <- readLines(path, warn = FALSE, encoding = "UTF-8")

  i <- 1L
  while (i <= length(lines) && !nzchar(trimws(lines[i]))) i <- i + 1L
  start <- i
  while (i <= length(lines) && grepl("^#'", lines[i])) i <- i + 1L
  if (i == start) {
    return(list(found = FALSE, meta = list(), duplicate_tags = character(),
                unknown_tags = character()))
  }

  h <- sub("^#'[ ]?", "", lines[start:(i - 1L)])

  entries <- list()
  for (ln in h) {
    m <- regmatches(ln, regexec("^@([A-Za-z_]+)[ \t]*(.*)$", ln))[[1]]
    if (length(m) == 3L) {
      entries[[length(entries) + 1L]] <- list(tag = tolower(m[2]), lines = m[3])
    } else if (length(entries)) {
      k <- length(entries)
      entries[[k]]$lines <- c(entries[[k]]$lines, trimws(ln))
    }
  }

  meta <- list()
  dup  <- character()
  for (e in entries) {
    if (e$tag %in% MULTILINE_TAGS) {
      val <- trimws(paste(e$lines, collapse = "\n"))
    } else {
      keep <- trimws(e$lines)
      val  <- paste(keep[nzchar(keep)], collapse = " ")
    }
    if (e$tag %in% REPEATABLE_TAGS) {
      meta[[e$tag]] <- c(meta[[e$tag]], val)
    } else {
      if (!is.null(meta[[e$tag]])) dup <- c(dup, e$tag)
      meta[[e$tag]] <- val
    }
  }

  list(found = TRUE, meta = meta, duplicate_tags = unique(dup),
       unknown_tags = setdiff(names(meta), KNOWN_TAGS))
}

# ---- Code inspection -----------------------------------------------------

# Columns from the first CREATE TABLE statement in the script.
parse_create_table <- function(code) {
  start <- grep("CREATE TABLE", code, ignore.case = TRUE)[1]
  if (is.na(start) || start == length(code)) return(NULL)

  pat <- paste0("^\\s*([A-Za-z_][A-Za-z0-9_]*)\\s+",
                "([A-Za-z0-9_]+(\\s*\\([0-9, ]+\\))?)\\s*",
                "(NOT NULL|NULL)?\\s*,?\\s*$")
  name <- type <- nullable <- character()
  for (ln in code[(start + 1L):length(code)]) {
    if (grepl("\\)\\s*;", ln)) break
    m <- regmatches(ln, regexec(pat, ln, ignore.case = TRUE))[[1]]
    if (length(m) == 5L) {
      name     <- c(name, m[2])
      type     <- c(type, toupper(gsub("\\s+", "", m[3])))
      nullable <- c(nullable, if (toupper(m[5]) == "NOT NULL") "NOT NULL" else "NULL")
    }
  }
  if (!length(name)) return(NULL)
  data.frame(name = name, type = type, nullable = nullable,
             stringsAsFactors = FALSE)
}

# Schema.Table names that follow FROM / JOIN in the code (comments ignored).
detect_sources <- function(code) {
  code <- code[!grepl("^\\s*#", code)]
  txt  <- paste(code, collapse = "\n")
  pat  <- "(?i)\\b(?:FROM|JOIN)\\s+([A-Za-z_][A-Za-z0-9_]*\\.[A-Za-z_][A-Za-z0-9_]*)"
  hits <- regmatches(txt, gregexpr(pat, txt, perl = TRUE))[[1]]
  unique(sub("(?i)^(?:FROM|JOIN)\\s+", "", hits, perl = TRUE))
}

# Values of the DB_NAME / SCHEMA_NAME / TABLE_NAME constants.
detect_target <- function(code) {
  get_var <- function(var) {
    pat <- sprintf("^\\s*%s\\s*<-\\s*\"([^\"]+)\"", var)
    m <- regmatches(code, regexec(pat, code))
    m <- m[lengths(m) == 2L]
    if (length(m)) m[[1]][2] else NA_character_
  }
  list(database = get_var("DB_NAME"), schema = get_var("SCHEMA_NAME"),
       table = get_var("TABLE_NAME"))
}

# First whitespace-delimited token of a @source value, and the remaining note.
split_source <- function(x) {
  tok  <- sub("\\s.*$", "", x)
  note <- trimws(sub("^\\S+\\s*", "", x))
  list(table = tok, note = note)
}

is_schema_table <- function(x) grepl("^[A-Za-z_][A-Za-z0-9_]*\\.[A-Za-z_][A-Za-z0-9_]*$", x)

# ---- One script ----------------------------------------------------------

parse_script <- function(path) {
  code <- readLines(path, warn = FALSE, encoding = "UTF-8")
  hdr  <- parse_header(path)
  list(
    path    = path,
    name    = tools::file_path_sans_ext(basename(path)),
    header  = hdr,
    meta    = hdr$meta,
    columns = parse_create_table(code),
    code_sources = detect_sources(code),
    code_target  = detect_target(code)
  )
}

# ---- Validation ----------------------------------------------------------
# Returns a data.frame(script, severity, message); severity is "error" or "warn".

validate_script <- function(s) {
  out <- list()
  add <- function(sev, msg) out[[length(out) + 1L]] <<- list(script = s$name,
                                                               severity = sev,
                                                               message = msg)
  m <- s$meta

  if (!s$header$found) {
    add("error", "No #' header found at the top of the script")
  } else {
    for (tg in REQUIRED_TAGS) {
      v <- m[[tg]]
      if (is.null(v) || !any(nzchar(v))) add("error", sprintf("Missing required tag @%s", tg))
    }
    for (tg in names(m)) {
      if (any(grepl("TODO", m[[tg]], fixed = TRUE))) {
        add("warn", sprintf("@%s still contains TODO", tg))
      }
    }
    for (tg in s$header$duplicate_tags) add("warn", sprintf("@%s given more than once (last one wins)", tg))
    for (tg in s$header$unknown_tags)   add("warn", sprintf("Unknown tag @%s", tg))

    lr <- m$last_reviewed
    if (!is.null(lr) && !grepl("TODO", lr, fixed = TRUE) &&
        !grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", lr)) {
      add("warn", "@last_reviewed should look like YYYY-MM-DD")
    }

    # Header vs. constants in the code
    for (f in c("database", "schema", "table")) {
      hv <- m[[f]]; cv <- s$code_target[[f]]
      if (!is.null(hv) && !is.na(cv) && !identical(tolower(hv), tolower(cv))) {
        add("warn", sprintf("@%s is '%s' but the code sets '%s'", f, hv, cv))
      }
    }

    # Header @source vs. FROM/JOIN in the code (schema.table-style entries only)
    hdr_src <- vapply(m$source %||% character(), function(x) split_source(x)$table, "")
    hdr_tbl <- hdr_src[is_schema_table(hdr_src)]
    target  <- if (!is.null(m$schema) && !is.null(m$table)) paste(m$schema, m$table, sep = ".") else ""
    code_tbl <- setdiff(s$code_sources, target)
    miss_in_hdr  <- code_tbl[!tolower(code_tbl) %in% tolower(hdr_tbl)]
    miss_in_code <- hdr_tbl[!tolower(hdr_tbl) %in% tolower(code_tbl)]
    if (length(miss_in_hdr))  add("warn", paste("Queried in code but not listed in @source:", paste(miss_in_hdr, collapse = ", ")))
    if (length(miss_in_code)) add("warn", paste("Listed in @source but not found in code:", paste(miss_in_code, collapse = ", ")))
  }

  if (is.null(s$columns)) {
    add("warn", "No CREATE TABLE statement found; column list will be empty")
  } else if (!is.null(m$column)) {
    doc_names <- vapply(m$column, function(x) sub("\\s.*$", "", x), "")
    bad <- doc_names[!tolower(doc_names) %in% tolower(s$columns$name)]
    if (length(bad)) add("warn", paste("@column names not in CREATE TABLE:", paste(bad, collapse = ", ")))
  }

  if (!length(out)) {
    return(data.frame(script = character(), severity = character(),
                      message = character(), stringsAsFactors = FALSE))
  }
  do.call(rbind, lapply(out, as.data.frame, stringsAsFactors = FALSE))
}
