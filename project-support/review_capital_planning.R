library(openxlsx2)
library(dplyr)
library(tidyr)
library(stringr)
library(DBI)
library(odbc)

source("utilities/utilities.R")
ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "InfBronze"
# Connect to SQL database
con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = SQL_SERVER,
  database = DB_NAME,
  Trusted_Connection = "Yes"
)

Schools <- openxlsx2::read_xlsx(here::here(
  "input/CapitalPlanningSystem/Data Samples/Infrastructure (Schools)_202627_Q1_Capital_Projects - ECC.xlsx"
)) |>
  mutate(across(everything(), as.character))

PostSec <- openxlsx2::read_xlsx(here::here(
  "input/CapitalPlanningSystem/Data Samples/Infrastructure (Post-Secondary Institutions)_202627_Q1_Capital_Projects.xlsx"
)) |>
  mutate(across(everything(), as.character))

# Health <- openxlsx2::read_xlsx(here::here("input/CapitalPlanningSystem/Data Samples/Infrastructure (Schools)_202627_Q1_Capital_Projects - ECC.xlsx"))

InfraCross <- openxlsx2::read_xlsx(here::here(
  "input/CapitalPlanningSystem/Data Samples/Infrastructure_202627_Q1_Capital_Projects - Cross Gov't.xlsx"
)) |>
  mutate(across(everything(), as.character))


# Post Sec Notes ####
# Variable1 well used, Variable2 completely empty
# Sector only has two rows saying "Post Secondary"
# Asset Type looks good, values a bit vague. Building or Specialized Equipment the two dominant categories.
# Other asset type only records 4 values. Not sure purpose as we have a dual asset type category in Building and Specialized Equipment
# There is 4 Asset Type rows listed as other, but one is NA, and one other row just has an "X" in it
# Category has a few missing values (90 of 115 rows are "New or expansion")
# cabinet and treasury approval dates are frequently 1900-01-01, should be NA instead?
# Street Address abysmal and postal code/geo lat/geo lon entirely missing
# Location generally good except for 14 rows with the value "Province"
# Admin region empty, Development region has 5 descriptive areas and 14 "Province" rows. Do we have geo info for their regions?
# Start/End dates well filled out, however no delineation between estimated and actual
# What is Dev construct start/end dates?
# Dev Construct Forecast is only 0's
# Maturity level is sparsely filled out and doesn't correspond well to End date (e.g. still in concept plan but we are past end date).
# Overall Percent Complete only records 4 non zero values.
# Procurement section is sparse.
# typo in one of the data column names "34/35 (Provincial" is missing the closing Parenthesis
# Are these straight downloads from CPS or something that has been prepped for us? As all of them have this error
# Because of wide format, 84% of the values are zero.

PostSecDim <- PostSec |>
  select(1:62, 96, 130:150)

PostSecFact <- PostSec |>
  select(2, 5, 64:95, 97:129) |>
  pivot_longer(
    cols = matches("\\((Provincial|Total Project)\\)?$"),
    names_to = c("FiscalYear", "Type"),
    names_pattern = "^(.*?)\\s*\\((Provincial|Total Project)\\)?$",
    values_to = "Value"
  )

SchoolsDim <- Schools |>
  select(1:62, 96, 130:150)

SchoolsFact <- Schools |>
  select(2, 5, 64:95, 97:129) |>
  pivot_longer(
    cols = matches("\\((Provincial|Total Project)\\)?$"),
    names_to = c("FiscalYear", "Type"),
    names_pattern = "^(.*?)\\s*\\((Provincial|Total Project)\\)?$",
    values_to = "Value"
  )

InfraCrossDim <- InfraCross |>
  select(1:62, 96, 130:150)

InfraCrossFact <- InfraCross |>
  select(2, 5, 64:95, 97:129) |>
  pivot_longer(
    cols = matches("\\((Provincial|Total Project)\\)?$"),
    names_to = c("FiscalYear", "Type"),
    names_pattern = "^(.*?)\\s*\\((Provincial|Total Project)\\)?$",
    values_to = "Value"
  )

# Dimension ####
dimension <- bind_rows(SchoolsDim, PostSecDim, InfraCrossDim) |>
  rename_with(.fn = ~ gsub(" ", "", .), .cols = everything())

str(dimension, max.level = 2, vec.len = 0, list.len = Inf)
max_char_lengths(dimension)

TABLE_NAME <- "Dimension_CapitalPlanning"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
PRIMARY_KEY <- "CPSIdentifier"

# dbRemoveTable(con, TARGET_TABLE)
if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- glue::glue(
    "CREATE TABLE
    {SCHEMA_NAME}.{TABLE_NAME}
    (
      [UpdateComplete]                VARCHAR(50)   NULL,
      [CPSIdentifier]                 VARCHAR(50)   NOT NULL,
      [Ministry]                      VARCHAR(200)  NULL,
      [InternalRanking]               VARCHAR(50)   NULL,
      [InternalProjectNumber]         VARCHAR(100)  NULL,
      [Agency]                        VARCHAR(200)  NULL,
      [ProjectName]                   VARCHAR(500)  NULL,
      [ProjectDescription]            VARCHAR(2000) NULL,
      [NotesandConditions]            VARCHAR(1000) NULL,
      [Variable1]                     VARCHAR(200)  NULL,
      [Variable2]                     VARCHAR(100)  NULL,
      [Sector]                        VARCHAR(100)  NULL,
      [AssetType]                     VARCHAR(100)  NULL,
      [OtherAssetType]                VARCHAR(100)  NULL,
      [Category]                      VARCHAR(100)  NULL,
      [StreetAddress]                 VARCHAR(200)  NULL,
      [PostalCode]                    VARCHAR(50)   NULL,
      [GeoCode-Latitude]              VARCHAR(100)  NULL,
      [GeoCode-Longitude]             VARCHAR(100)  NULL,
      [Location]                      VARCHAR(100)  NULL,
      [AdminRegion]                   VARCHAR(50)   NULL,
      [DevelopmentRegion]             VARCHAR(100)  NULL,
      [CabinetApprovalDate]           VARCHAR(30)   NULL,
      [TreasuryBoardApprovalDate]     VARCHAR(30)   NULL,
      [BusinessCaseRequired]          VARCHAR(50)   NULL,
      [BusinessCaseReceivedDate]      VARCHAR(30)   NULL,
      [BusinessCaseApprovedDate]      VARCHAR(30)   NULL,
      [AuditedByOCG]                  VARCHAR(50)   NULL,
      [StartDate]                     VARCHAR(30)   NULL,
      [EndDate]                       VARCHAR(30)   NULL,
      [DevConstructStartDate]         VARCHAR(30)   NULL,
      [DevConstructEndDate]           VARCHAR(30)   NULL,
      [DevConstructForecast]          VARCHAR(50)   NULL,
      [MaturityLevel]                 VARCHAR(100)  NULL,
      [OverallPercentComplete]        VARCHAR(50)   NULL,
      [ProcurementMethod]             VARCHAR(100)  NULL,
      [ProcurementMethodDescription]  VARCHAR(200)  NULL,
      [ProcurementMethodOther]        VARCHAR(200)  NULL,
      [ApprovedAmount]                VARCHAR(50)   NULL,
      [ForecastAmount]                VARCHAR(50)   NULL,
      [Variance]                      VARCHAR(50)   NULL,
      [VarianceSource]                VARCHAR(100)  NULL,
      [OtherVarianceDescription]      VARCHAR(200)  NULL,
      [DebtType]                      VARCHAR(100)  NULL,
      [ApprovedProvincialDebt]        VARCHAR(50)   NULL,
      [ProvincialDebt]                VARCHAR(50)   NULL,
      [ProvincialDebtVariance]        VARCHAR(50)   NULL,
      [Federal]                       VARCHAR(50)   NULL,
      [Municipal/Regional]            VARCHAR(50)   NULL,
      [ThirdParty]                    VARCHAR(50)   NULL,
      [AgencyFunds]                   VARCHAR(50)   NULL,
      [TotalDollarAmount]             VARCHAR(50)   NULL,
      [TotalProjectQ1]                VARCHAR(50)   NULL,
      [TotalProjectQ2]                VARCHAR(50)   NULL,
      [TotalProjectQ3]                VARCHAR(50)   NULL,
      [TotalProjectQ4]                VARCHAR(50)   NULL,
      [ProvincialDebtQ1]              VARCHAR(50)   NULL,
      [ProvincialDebtQ2]              VARCHAR(50)   NULL,
      [ProvincialDebtQ3]              VARCHAR(50)   NULL,
      [ProvincialDebtQ4]              VARCHAR(50)   NULL,
      [ActualYTD(Provincial)]         VARCHAR(50)   NULL,
      [ActualYTD(TotalProject)]       VARCHAR(50)   NULL,
      [TotalCashFlow(Provincial)]     VARCHAR(50)   NULL,
      [TotalCashFlow(TotalProject)]   VARCHAR(50)   NULL,
      [FederalForecastAccelerated]    VARCHAR(50)   NULL,
      [OpCostStartUpDate]             VARCHAR(30)   NULL,
      [OpCostStartUpAmount]           VARCHAR(50)   NULL,
      [OpCostOngoingDate]             VARCHAR(30)   NULL,
      [OpCostOngoingAmount]           VARCHAR(50)   NULL,
      [OpcostAmortizationDate]        VARCHAR(30)   NULL,
      [OpcostAmortizationAmount]      VARCHAR(50)   NULL,
      [Comments]                      VARCHAR(1000) NULL,
      [OperatingCostImplications]     VARCHAR(500)  NULL,
      [5GreatGoals]                   VARCHAR(200)  NULL,
      [StrategicAlignment]            VARCHAR(200)  NULL,
      [GovPublicCommitment]           VARCHAR(50)   NULL,
      [CommitmentType]                VARCHAR(100)  NULL,
      [OtherCommitmentDescription]    VARCHAR(200)  NULL,
      [CommitmentNotes]               VARCHAR(1000) NULL,
      [Cross-GovIntegration]          VARCHAR(50)   NULL,
      [HasDependency]                 VARCHAR(50)   NULL,
      [DependencyDescription]         VARCHAR(500)  NULL,
      [DateCreated]                   VARCHAR(30)   NULL,
      [LastModified]                  VARCHAR(30)   NULL,
      CONSTRAINT PK_bronze_cps PRIMARY KEY ({PRIMARY_KEY})
    );"
  )
  dbExecute(con, sql)
}

DBI::dbAppendTable(con, TARGET_TABLE, dimension)

# Fact ####
fact <- bind_rows(SchoolsDim, PostSecDim, InfraCrossDim) |>
  rename_with(.fn = ~ gsub(" ", "", .), .cols = everything())

str(fact, max.level = 2, vec.len = 0, list.len = Inf)
max_char_lengths(fact)

TABLE_NAME <- "Fact_CapitalPlanning"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
PRIMARY_KEY <- "CPSIdentifier"

# dbRemoveTable(con, TARGET_TABLE)
if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- glue::glue(
    "CREATE TABLE
    {SCHEMA_NAME}.{TABLE_NAME}
    (
      [UpdateComplete]                VARCHAR(50)   NULL,
      [CPSIdentifier]                 VARCHAR(50)   NOT NULL,
      [Ministry]                      VARCHAR(200)  NULL,
      [InternalRanking]               VARCHAR(50)   NULL,
      [InternalProjectNumber]         VARCHAR(100)  NULL,
      [Agency]                        VARCHAR(200)  NULL,
      [ProjectName]                   VARCHAR(500)  NULL,
      [ProjectDescription]            VARCHAR(2000) NULL,
      [NotesandConditions]            VARCHAR(1000) NULL,
      [Variable1]                     VARCHAR(200)  NULL,
      [Variable2]                     VARCHAR(100)  NULL,
      [Sector]                        VARCHAR(100)  NULL,
      [AssetType]                     VARCHAR(100)  NULL,
      [OtherAssetType]                VARCHAR(100)  NULL,
      [Category]                      VARCHAR(100)  NULL,
      [StreetAddress]                 VARCHAR(200)  NULL,
      [PostalCode]                    VARCHAR(50)   NULL,
      [GeoCode-Latitude]              VARCHAR(100)  NULL,
      [GeoCode-Longitude]             VARCHAR(100)  NULL,
      [Location]                      VARCHAR(100)  NULL,
      [AdminRegion]                   VARCHAR(50)   NULL,
      [DevelopmentRegion]             VARCHAR(100)  NULL,
      [CabinetApprovalDate]           VARCHAR(30)   NULL,
      [TreasuryBoardApprovalDate]     VARCHAR(30)   NULL,
      [BusinessCaseRequired]          VARCHAR(50)   NULL,
      [BusinessCaseReceivedDate]      VARCHAR(30)   NULL,
      [BusinessCaseApprovedDate]      VARCHAR(30)   NULL,
      [AuditedByOCG]                  VARCHAR(50)   NULL,
      [StartDate]                     VARCHAR(30)   NULL,
      [EndDate]                       VARCHAR(30)   NULL,
      [DevConstructStartDate]         VARCHAR(30)   NULL,
      [DevConstructEndDate]           VARCHAR(30)   NULL,
      [DevConstructForecast]          VARCHAR(50)   NULL,
      [MaturityLevel]                 VARCHAR(100)  NULL,
      [OverallPercentComplete]        VARCHAR(50)   NULL,
      [ProcurementMethod]             VARCHAR(100)  NULL,
      [ProcurementMethodDescription]  VARCHAR(200)  NULL,
      [ProcurementMethodOther]        VARCHAR(200)  NULL,
      [ApprovedAmount]                VARCHAR(50)   NULL,
      [ForecastAmount]                VARCHAR(50)   NULL,
      [Variance]                      VARCHAR(50)   NULL,
      [VarianceSource]                VARCHAR(100)  NULL,
      [OtherVarianceDescription]      VARCHAR(200)  NULL,
      [DebtType]                      VARCHAR(100)  NULL,
      [ApprovedProvincialDebt]        VARCHAR(50)   NULL,
      [ProvincialDebt]                VARCHAR(50)   NULL,
      [ProvincialDebtVariance]        VARCHAR(50)   NULL,
      [Federal]                       VARCHAR(50)   NULL,
      [Municipal/Regional]            VARCHAR(50)   NULL,
      [ThirdParty]                    VARCHAR(50)   NULL,
      [AgencyFunds]                   VARCHAR(50)   NULL,
      [TotalDollarAmount]             VARCHAR(50)   NULL,
      [TotalProjectQ1]                VARCHAR(50)   NULL,
      [TotalProjectQ2]                VARCHAR(50)   NULL,
      [TotalProjectQ3]                VARCHAR(50)   NULL,
      [TotalProjectQ4]                VARCHAR(50)   NULL,
      [ProvincialDebtQ1]              VARCHAR(50)   NULL,
      [ProvincialDebtQ2]              VARCHAR(50)   NULL,
      [ProvincialDebtQ3]              VARCHAR(50)   NULL,
      [ProvincialDebtQ4]              VARCHAR(50)   NULL,
      [ActualYTD(Provincial)]         VARCHAR(50)   NULL,
      [ActualYTD(TotalProject)]       VARCHAR(50)   NULL,
      [TotalCashFlow(Provincial)]     VARCHAR(50)   NULL,
      [TotalCashFlow(TotalProject)]   VARCHAR(50)   NULL,
      [FederalForecastAccelerated]    VARCHAR(50)   NULL,
      [OpCostStartUpDate]             VARCHAR(30)   NULL,
      [OpCostStartUpAmount]           VARCHAR(50)   NULL,
      [OpCostOngoingDate]             VARCHAR(30)   NULL,
      [OpCostOngoingAmount]           VARCHAR(50)   NULL,
      [OpcostAmortizationDate]        VARCHAR(30)   NULL,
      [OpcostAmortizationAmount]      VARCHAR(50)   NULL,
      [Comments]                      VARCHAR(1000) NULL,
      [OperatingCostImplications]     VARCHAR(500)  NULL,
      [5GreatGoals]                   VARCHAR(200)  NULL,
      [StrategicAlignment]            VARCHAR(200)  NULL,
      [GovPublicCommitment]           VARCHAR(50)   NULL,
      [CommitmentType]                VARCHAR(100)  NULL,
      [OtherCommitmentDescription]    VARCHAR(200)  NULL,
      [CommitmentNotes]               VARCHAR(1000) NULL,
      [Cross-GovIntegration]          VARCHAR(50)   NULL,
      [HasDependency]                 VARCHAR(50)   NULL,
      [DependencyDescription]         VARCHAR(500)  NULL,
      [DateCreated]                   VARCHAR(30)   NULL,
      [LastModified]                  VARCHAR(30)   NULL,
      CONSTRAINT PK_bronze_cps_fact PRIMARY KEY CLUSTERED ({paste(PRIMARY_KEY, collapse = ', ')})
    );"
  )
  dbExecute(con, sql)
}

DBI::dbAppendTable(con, TARGET_TABLE, fact)
