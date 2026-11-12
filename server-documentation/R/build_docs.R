# build_docs.R
# Builds generated/<layer>.qmd (and index.qmd) from the #' headers in the ETL
# scripts listed in layers.yml.
#
# Usage (from anywhere):
#   Rscript Server-Documentation/build_docs.R
#   Rscript Server-Documentation/build_docs.R --layer=InfGold
#   Rscript Server-Documentation/build_docs.R --strict    # exit 1 on any error
#
# Requires: the yaml package. Rendering the .qmd files needs Quarto.
# For an active rendering after triggering via task scheduler, add the --render argument
#  Rscript.exe "C:\Projects\rpd-server-operations\Server-Documentation\R\build_docs.R" --render
args <- commandArgs(trailingOnly = TRUE)
strict <- "--strict" %in% args
only <- sub("^--layer=", "", args[grepl("^--layer=", args)])

find_doc_dir <- function() {
  # Where did this script come from? (first method that works wins)
  start <- NULL
  f <- sub("^--file=", "", grep("^--file=", commandArgs(FALSE), value = TRUE))
  if (length(f)) {
    start <- dirname(f[1])
  } # Rscript / Task Scheduler

  if (is.null(start)) {
    of <- tryCatch(sys.frame(1)$ofile, error = function(e) NULL) # source() / Source button
    if (!is.null(of)) start <- dirname(of)
  }
  if (
    is.null(start) &&
      requireNamespace("rstudioapi", quietly = TRUE) &&
      rstudioapi::isAvailable()
  ) {
    # Ctrl+Enter in RStudio
    p <- rstudioapi::getSourceEditorContext()$path
    if (nzchar(p)) start <- dirname(p)
  }
  if (is.null(start)) {
    start <- getwd()
  } # last resort

  # Walk up until the folder containing layers.yml is found
  d <- normalizePath(start)
  repeat {
    if (file.exists(file.path(d, "layers.yml"))) {
      return(d)
    }
    parent <- dirname(d)
    if (identical(parent, d)) {
      break
    }
    d <- parent
  }

  # Started from the repo root: look one level down
  kids <- list.dirs(normalizePath(start), recursive = FALSE)
  kids <- kids[file.exists(file.path(kids, "layers.yml"))]
  if (length(kids)) {
    return(normalizePath(kids[1]))
  }

  stop(
    "Can't find layers.yml. Run from the repo root or inside Server-Documentation."
  )
}

doc_dir <- find_doc_dir()
root <- dirname(doc_dir)

source(file.path(doc_dir, "R", "parse_headers.R"))
source(file.path(doc_dir, "R", "render_docs.R"))

cfg <- yaml::read_yaml(file.path(doc_dir, "layers.yml"))
out_dir <- file.path(doc_dir, "generated")
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

write_text <- function(lines, path) {
  con <- file(path, open = "wb")
  on.exit(close(con))
  writeLines(enc2utf8(lines), con, sep = "\n", useBytes = TRUE)
}

issues <- list()
layers_info <- list()

for (layer in cfg$layers) {
  if (length(only) && !layer$name %in% only) {
    next
  }

  sdir <- file.path(root, layer$path, layer$scripts_dir %||% "Scripts")
  files <- sort(list.files(sdir, pattern = "\\.[Rr]$", full.names = TRUE))
  names(files) <- tools::file_path_sans_ext(basename(files))

  if (!length(files)) {
    message("[", layer$name, "] no scripts found in ", sdir)
    next
  }

  order <- unlist(layer$run_order) %||% character()
  extra <- setdiff(names(files), order)
  gone <- setdiff(order, names(files))
  for (x in extra) {
    issues[[length(issues) + 1L]] <- data.frame(
      script = x,
      severity = "warn",
      message = sprintf("Not in run_order for %s (appended at end)", layer$name)
    )
  }
  for (x in gone) {
    issues[[length(issues) + 1L]] <- data.frame(
      script = x,
      severity = "warn",
      message = sprintf(
        "Listed in run_order for %s but no such script on disk",
        layer$name
      )
    )
  }
  ordered <- c(intersect(order, names(files)), extra)

  scripts <- lapply(files[ordered], parse_script)
  for (s in scripts) {
    v <- validate_script(s)
    if (nrow(v)) issues[[length(issues) + 1L]] <- v
  }

  root_fwd <- paste0(normalizePath(root, winslash = "/"), "/")
  full <- normalizePath(files[ordered], winslash = "/")
  rel <- ifelse(
    startsWith(full, root_fwd),
    substring(full, nchar(root_fwd) + 1L),
    full
  )
  write_text(
    render_layer(layer, unname(scripts), rel),
    file.path(out_dir, paste0(layer$name, ".qmd"))
  )

  layers_info[[length(layers_info) + 1L]] <- list(
    name = layer$name,
    title = layer$title %||% layer$name,
    n = length(scripts),
    description = trimws(layer$description %||% "")
  )
  message("[", layer$name, "] documented ", length(scripts), " script(s)")
}

if (length(layers_info) && !length(only)) {
  write_text(render_index(layers_info), file.path(out_dir, "index.qmd"))
}

if (length(issues)) {
  iss <- do.call(rbind, issues)
  iss <- iss[order(iss$severity, iss$script), ]
  cat("\nIssues:\n")
  for (i in seq_len(nrow(iss))) {
    cat(sprintf(
      "  [%s] %s: %s\n",
      toupper(iss$severity[i]),
      iss$script[i],
      iss$message[i]
    ))
  }
  n_err <- sum(iss$severity == "error")
  cat(sprintf(
    "\n%d error(s), %d warning(s)\n",
    n_err,
    sum(iss$severity == "warn")
  ))
  if (strict && n_err > 0) quit(status = 1)
} else {
  cat("\nNo issues.\n")
}

if ("--render" %in% args) {
  quarto <- Sys.which("quarto")
  if (!nzchar(quarto)) {
    quarto <- Sys.getenv("QUARTO_PATH")
  } # fallback if not on PATH
  if (!nzchar(quarto)) {
    stop("quarto executable not found; add it to PATH or set QUARTO_PATH")
  }

  status <- system2(quarto, c("render", shQuote(out_dir)))
  if (status != 0) {
    stop("quarto render failed with status ", status)
  }
  message("Rendered HTML from ", out_dir)
}
