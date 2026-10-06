library(openxlsx2)
library(dplyr)

Schools <- openxlsx2::read_xlsx(here::here(
  "input/CapitalPlanningSystem/Data Samples/Infrastructure (Schools)_202627_Q1_Capital_Projects - ECC.xlsx"
))

PostSec <- openxlsx2::read_xlsx(here::here(
  "input/CapitalPlanningSystem/Data Samples/Infrastructure (Post-Secondary Institutions)_202627_Q1_Capital_Projects.xlsx"
))

# Health <- openxlsx2::read_xlsx(here::here("input/CapitalPlanningSystem/Data Samples/Infrastructure (Schools)_202627_Q1_Capital_Projects - ECC.xlsx"))

InfraCross <- openxlsx2::read_xlsx(here::here(
  "input/CapitalPlanningSystem/Data Samples/Infrastructure_202627_Q1_Capital_Projects - Cross Gov't.xlsx"
))
