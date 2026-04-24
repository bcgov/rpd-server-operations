install.packages(c(
  "arrow",
  "assertthat",
  "AzureAuth",
  "AzureGraph",
  "AzureRMR",
  "base64enc",
  "DBI",
  "devtools",
  "dplyr",
  "fastmap",
  "fs",
  "here",
  "httpuv",
  "httr2",
  "igraph",
  "jsonlite",
  "keyring",
  "lubridate",
  "magrittr",
  "miniCRAN",
  "odbc",
  "openxlsx2",
  "plyr",
  "purrr",
  "readr",
  "renv",
  "stringr",
  "testthat",
  "tibble",
  "tidyr",
  "vroom"
))

library(miniCRAN)
pkgList <- c(
  "igraph",
  "bit64"
)
pkgList <- c(
  "arrow",
  "assertthat",
  "AzureAuth",
  "AzureGraph",
  "AzureRMR",
  "base64enc",
  "DBI",
  "devtools",
  "dplyr",
  "fastmap",
  "fs",
  "here",
  "httpuv",
  "httr2",
  "igraph",
  "jsonlite",
  "keyring",
  "lubridate",
  "magrittr",
  "miniCRAN",
  "odbc",
  "openxlsx2",
  "plyr",
  "purrr",
  "readr",
  "renv",
  "stringr",
  "testthat",
  "tibble",
  "tidyr",
  "vroom"
)

# fullList <- pkgDep(pkgList, type = "source", Rversion = getRversion())
fullList <- pkgDep(pkgList, type = "win.binary", Rversion = getRversion())

# makeRepo(
#   fullList,
#   path = "C:/Projects/packagerepo",
#   download = TRUE,
#   type = "source",
#   Rversion = getRversion()
# )

makeRepo(
  pkgList,
  path = "C:/Projects/packagerepo",
  download = TRUE,
  type = "win.binary",
  Rversion = getRversion()
)

# Attempts to get Muon sorted out
# getOption("repos")
# renv::config$repos.override("file:///E:/Projects/packagerepo")
# options(renv.config.ppm.enabled = FALSE)
# RENV_PATHS_CELLAR =  "E:/Projects/packagerepo/"
# RENV_CONFIG_REPOS_OVERRIDE = "CRAN=file:///E:/Projects/packagerepo"
# options(renv.config.repos.override = c(CRAN = "file:///E:/Projects/packagerepo"))

# Adding the below to .Rprofile after source("renv/activate.R") has worked
options(repos = c(LOCAL = "file:///E:/Projects/packagerepo"))
# may need to call
# renv::restore(repos = getOption("repos"))

install.packages(pkgList, repos = "file:///E:/Projects/packagerepo", type = "win.binary")

options(renv.download.trace = TRUE)
renv::restore()

# Troubleshoot current hanging
# See what versions the lockfile wants
lockfile <- renv::lockfile_read()
wanted <- sapply(lockfile$Packages, function(p) p$Version)
wanted <- data.frame(Package = names(wanted), Wanted = wanted, stringsAsFactors = FALSE)

# See what versions your local repo has
available <- available.packages(
  repos = "file:///E:/Projects/packagerepo",
  type = "win.binary"
)[, c("Package", "Version")]
available <- as.data.frame(available, stringsAsFactors = FALSE)

# Find mismatches
merged <- merge(wanted, available, by = "Package", all.x = TRUE)
merged[is.na(merged$Version) | merged$Wanted != merged$Version, ]

# add to .Rprofile
options(renv.config.install.transactional = FALSE)

# run with
options(renv.download.trace = TRUE)
renv::restore(transactional = FALSE)
