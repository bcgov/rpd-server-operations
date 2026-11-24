render_handbook <- function(layers, scripts_by_layer) {
  lines <- c(
    "---",
    'title: "Building Intelligence Documentation"',
    "format:",
    "  html:",
    "    toc: true",
    "    toc-depth: 4",
    "    embed-resources: true",
    "---",
    "",
    sprintf(
      "_Generated: %s_",
      format(Sys.time(), "%Y-%m-%d %H:%M UTC", tz = "UTC")
    ),
    ""
  )

  for (layer in layers) {
    lines <- c(
      lines,
      "",
      paste0("# ", layer$title),
      ""
    )

    scripts <- scripts_by_layer[[layer$name]]

    for (s in scripts) {
      lines <- c(
        lines,
        "",
        paste0("## ", s$meta$title %||% s$name),
        ""
      )

      if (!is.null(s$meta$description)) {
        lines <- c(
          lines,
          s$meta$description,
          ""
        )
      }

      lines <- c(
        lines,
        sprintf(
          "- **Table:** `%s.%s`",
          s$meta$schema,
          s$meta$table
        ),
        sprintf(
          "- **Load Type:** %s",
          s$meta$load_type
        ),
        sprintf(
          "- **Grain:** %s",
          s$meta$grain
        ),
        ""
      )

      if (!is.null(s$meta$source)) {
        lines <- c(
          lines,
          "### Sources",
          ""
        )

        for (src in s$meta$source) {
          lines <- c(lines, paste0("- ", src))
        }

        lines <- c(lines, "")
      }

      if (!is.null(s$meta$column)) {
        lines <- c(
          lines,
          "### Columns",
          "",
          "| Column | Description |",
          "|----------|----------|"
        )

        for (cl in s$meta$column) {
          nm <- sub("\\s.*$", "", cl)

          desc <- trimws(
            sub("^\\S+\\s*", "", cl)
          )

          lines <- c(
            lines,
            sprintf(
              "| %s | %s |",
              nm,
              desc
            )
          )
        }

        lines <- c(lines, "")
      }

      if (!is.null(s$meta$notes)) {
        lines <- c(
          lines,
          "### Notes",
          "",
          s$meta$notes,
          ""
        )
      }

      lines <- c(
        lines,
        sprintf(
          "_Owner: %s | Last Reviewed: %s_",
          s$meta$owner %||% "",
          s$meta$last_reviewed %||% ""
        ),
        ""
      )
    }
  }

  lines
}
