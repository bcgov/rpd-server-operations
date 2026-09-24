# See what versions the lockfile wants
lockfile <- renv::lockfile_read()
wanted <- sapply(lockfile$Packages, function(p) p$Version)
wanted <- data.frame(
  Package = names(wanted),
  Wanted = wanted,
  stringsAsFactors = FALSE
)

# See what versions your local repo has
available <- available.packages(
  repos = "file:///C:/Projects/packagerepo",
  type = "win.binary"
)[, c("Package", "Version")]
available <- as.data.frame(available, stringsAsFactors = FALSE)

# Find mismatches
merged <- merge(wanted, available, by = "Package", all.x = TRUE)
merged[is.na(merged$Version) | merged$Wanted != merged$Version, ]
