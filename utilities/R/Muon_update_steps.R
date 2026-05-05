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
  repos = "file:///E:/Projects/packagerepo",
  type = "win.binary"
)[, c("Package", "Version")]
available <- as.data.frame(available, stringsAsFactors = FALSE)

# Find mismatches
merged <- merge(wanted, available, by = "Package", all.x = TRUE)
merged[is.na(merged$Version) | merged$Wanted != merged$Version, ]

# Look for specific stuff in the packagerepo
list.files(
  "E:/Projects/packagerepo/bin/windows/contrib/4.5",
  pattern = "Bioc",
  full.names = TRUE
)

# If missing anything grab from laptop and copy over.
# Then update PACKAGES to get consistent (doesn't add BiocVersion correctly...)
tools::write_PACKAGES(
  "E:/Projects/packagerepo/bin/windows/contrib/4.5",
  type = "win.binary",
  verbose = TRUE
)

# if individual install needed
# renv::install(packages = "BiocManager", repos = c(LOCAL = "file:///E:/Projects/packagerepo"), type = "win.binary")

# Restore env
options(
        renv.download.trace = TRUE,
        repos = c(LOCAL = "file:///E:/Projects/packagerepo"),
        BioC_mirror = NULL,
        renv.config.repos.override = "file:///E:/Projects/packagerepo")

renv::restore(repos = c(LOCAL = "file:///E:/Projects/packagerepo"), transactional = FALSE)


# for .Rprofile
# options(
#   repos = c(LOCAL = "file:///E:/Projects/packagerepo"),
#   renv.config.repos.override = "file:///E:/Projects/packagerepo"
# )
