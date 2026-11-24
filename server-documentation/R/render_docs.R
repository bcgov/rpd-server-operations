# render_docs.R
# Turns parsed scripts into Quarto (.qmd) text. Base R only.
# Output is kept ASCII-only and free of timestamps so regenerated files only
# show git diffs when the documentation actually changes.

md_cell <- function(x) {
  x <- gsub("\r?\n", " ", x %||% "")
  gsub("|", "\\|", x, fixed = TRUE)
}

md_table <- function(header, rows) {
  if (!length(rows)) return(character())
  c(paste0("| ", paste(header, collapse = " | "), " |"),
    paste0("|", paste(rep("---", length(header)), collapse = "|"), "|"),
    vapply(rows, function(r) paste0("| ", paste(vapply(r, md_cell, ""), collapse = " | "), " |"), ""))
}

target_name <- function(s) {
  paste(c(s$meta$schema, s$meta$table), collapse = ".")
}

# ---- Lineage diagram -----------------------------------------------------

render_lineage <- function(scripts) {
  node_id <- function(x) paste0("n_", gsub("[^A-Za-z0-9]", "_", x))
  targets <- vapply(scripts, target_name, "")
  lines   <- c("```{mermaid}", "flowchart LR")
  declared <- character()

  for (tg in targets) {
    if (!nzchar(tg)) next
    lines <- c(lines, sprintf("  %s[(\"%s\")]", node_id(tg), tg))
    declared <- c(declared, tg)
  }
  for (i in seq_along(scripts)) {
    tg <- targets[i]
    for (src in scripts[[i]]$meta$source %||% character()) {
      tok <- split_source(src)$table
      if (!tok %in% declared) {
        lines <- c(lines, sprintf("  %s[\"%s\"]", node_id(tok), tok))
        declared <- c(declared, tok)
      }
      lines <- c(lines, sprintf("  %s --> %s", node_id(tok), node_id(tg)))
    }
  }
  c(lines, "```", "")
}

# ---- One script ----------------------------------------------------------

render_script <- function(s, rel_path) {
  m <- s$meta
  out <- c(sprintf("## %s", m$title %||% s$name), "")

  target <- paste(c(m$database, m$schema, m$table), collapse = ".")
  props <- list(
    c("Target table", sprintf("`%s`", target)),
    c("Script", sprintf("`%s`", rel_path)),
    c("Load type", m$load_type %||% ""),
    c("Grain", m$grain %||% ""),
    c("Owner", m$owner %||% ""),
    c("Last reviewed", m$last_reviewed %||% "")
  )
  out <- c(out, md_table(c("Property", "Value"), props), "")

  if (!is.null(m$description)) out <- c(out, m$description, "")

  if (!is.null(m$source)) {
    out <- c(out, "### Sources", "")
    for (src in m$source) {
      p <- split_source(src)
      out <- c(out, if (nzchar(p$note)) sprintf("- `%s` - %s", p$table, p$note)
                    else sprintf("- `%s`", p$table))
    }
    out <- c(out, "")
  }
  if (!is.null(m$depends_on)) {
    out <- c(out, "### Depends on", "", paste0("- ", m$depends_on), "")
  }
  if (!is.null(m$filters)) {
    out <- c(out, "### Rules and filters", "", paste0("- ", m$filters), "")
  }
  if (!is.null(m$consumers)) {
    out <- c(out, "### Consumers", "", paste0("- ", m$consumers), "")
  }

  if (!is.null(s$columns)) {
    notes <- list()
    for (cx in m$column %||% character()) {
      nm <- sub("\\s.*$", "", cx)
      notes[[tolower(nm)]] <- trimws(sub("^\\S+\\s*", "", cx))
    }
    rows <- lapply(seq_len(nrow(s$columns)), function(i) {
      c(sprintf("`%s`", s$columns$name[i]), s$columns$type[i],
        s$columns$nullable[i], notes[[tolower(s$columns$name[i])]] %||% "")
    })
    out <- c(out, "### Columns", "",
             md_table(c("Column", "Type", "Nullable", "Description"), rows), "")
  }

  if (!is.null(m$notes)) {
    out <- c(out, "### Notes", "", unlist(lapply(m$notes, function(n) c(n, ""))))
  }
  out
}

# ---- One layer / index ---------------------------------------------------

render_layer <- function(layer, scripts, rel_paths) {
  out <- c("---", sprintf("title: \"%s\"", layer$title %||% layer$name), "---", "")
  if (!is.null(layer$description)) out <- c(out, trimws(layer$description), "")

  rows <- lapply(seq_along(scripts), function(i) {
    m <- scripts[[i]]$meta
    c(as.character(i), sprintf("`%s`", basename(rel_paths[i])),
      sprintf("`%s`", target_name(scripts[[i]])), m$load_type %||% "")
  })
  out <- c(out, "## Run order", "",
           md_table(c("#", "Script", "Target table", "Load type"), rows), "",
           "## Lineage", "", render_lineage(scripts))

  for (i in seq_along(scripts)) out <- c(out, render_script(scripts[[i]], rel_paths[i]))
  out
}

render_index <- function(layers_info) {
  out <- c("---", "title: \"ETL Documentation\"", "---", "",
           "Generated from the `#'` headers in each ETL script. Do not edit these pages by hand; edit the script headers and re-run `build_docs.R`.",
           "")
  rows <- lapply(layers_info, function(l) {
    c(sprintf("[%s](%s.qmd)", l$title, l$name), as.character(l$n), l$description)
  })
  c(out, md_table(c("Layer", "Tables", "Description"), rows), "")
}
