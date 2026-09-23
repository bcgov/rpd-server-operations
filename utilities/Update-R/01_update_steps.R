# In local development environment, on a new branch, download and install R and Rtools if necessary

# Open project with new version of R and run
renv::deactivate(clean = TRUE)

# Start new session then run
renv::init()

# At this point it should install everything needed for the scripts/files in the project
# You may need to resolve a few minor issues/manually install or update a few packages
# but once done, run renv::snapshot() to update the lockfile.

# Now we need to create a package repository to load onto Muon
library(miniCRAN)
library(BiocManager)

# Try and keep this list updated with all the main packages we need, but can be rerun if we are missing things
pkgList <- c(
  "arrow",
  "assertthat",
  "AzureAuth",
  "AzureGraph",
  "AzureRMR",
  "base64enc",
  "BiocManager",
  "blastula",
  "DBI",
  "devtools",
  "dplyr",
  "fastmap",
  "fs",
  "getPass",
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
  "timetk",
  "vroom"
)

# Expand the list to include dependencies
fullList <- pkgDep(
  pkgList,
  repos = unname(getOption("repos")[1]), # see options with getOption('repos')
  type = "win.binary",
  Rversion = getRversion()
)

# make repository of windows binaries for cran packages
makeRepo(
  fullList,
  path = "C:/Projects/packagerepo",
  repos = unname(getOption("repos")[1]),
  download = TRUE,
  type = "win.binary",
  Rversion = getRversion()
)

# Repeat process for Bioconductor
BiocRepos <- BiocManager::repositories()

BiocPkgList <- c(
  "BiocVersion"
)

BiocfullList <- pkgDep(
  BiocPkgList,
  repos = unname(BiocRepos[1]), # can use other repos
  type = "win.binary",
  Rversion = getRversion()
)

makeRepo(
  BiocfullList,
  path = "C:/Projects/packagerepo",
  repos = unname(BiocRepos[1]),
  download = TRUE,
  type = "win.binary",
  Rversion = getRversion()
)

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

# This is what I had in the lockfile that was an older version of what I'd downloaded into packagerepo
install.packages(c(
  "class",
  "curl",
  "httr",
  "KernSmooth",
  "lattice",
  "MASS",
  "Matrix",
  "nlme",
  "nnet",
  "openssl",
  "survival"
))

# after installing, update the lockfile
renv::snapshot()

# Rerun the above script with the lockfile to see remaining differences

# add what is missing to repo
pkgList <- c("odbc", "shinyWidgets", "tinytex", "xfun")

# make repository of windows binaries for cran packages
miniCRAN::makeRepo(
  pkgList,
  path = "C:/Projects/packagerepo",
  repos = unname(getOption("repos")[1]),
  download = TRUE,
  type = "win.binary",
  Rversion = getRversion()
)

# rerun lockfile and repository comparison. should be a complete match now.

# With this part complete on branch test run scripts and make sure everything works prior to pushing changes to main
# Once you've pushed changes to main, pull changes to server

# copy repository over to E:/Projects/packagerepo

# Head over to Muon, and pull the main branch changes

renv::restore(library = "E:/Projects/packagerepo", repos = NULL)
