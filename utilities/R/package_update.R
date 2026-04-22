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

fullList <- pkgDep(pkgList, type = "win.binary", Rversion = getRversion())

makeRepo(
  fullList,
  path = "C:/Projects/packagerepo",
  download = TRUE,
  type = "win.binary",
  Rversion = getRversion()
)
