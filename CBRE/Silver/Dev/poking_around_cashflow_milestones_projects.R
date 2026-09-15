query <- dbSendQuery(con, "SELECT * FROM CbreStaging.kahua_cashflow")
CashflowData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.pjm_dim_project")
DimProjData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.dim_project")
DimProjFullData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.pjm_fact_project")
FactProjData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.pjm_fact_milestone")
FactMileStoneData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.dim_property")
DimPropData <- dbFetch(query, n = -1)
dbClearResult(query)

Cashflow <- CashflowData |>
  select(
    ProjectNumber,
    CashflowParentId,
    CashflowId,
    Period,
    LineCategory,
    ActivityCode,
    ActivityCodeDesc,
    ItemAmount,
    AllocationItemId,
    AllocationAmount,
    AllocationDate
  )

DimProj <- DimProjData |>
  select(
    project_skey,
    project_number,
    csf_fundingtype,
    csf_fundingsource,
    csf_ministryparentorg,
    csf_branchchildorg,
    csf_fyclosed,
    dim_source_created = source_created_ts
  ) |>
  mutate(
    csf_fundingsource = stringr::str_to_title(csf_fundingsource) # remove one value of EXPENSE
  )

FactProj <- FactProjData |>
  select(
    project_skey,
    project_status,
    property_skey,
    charter_date,
    fact_source_created = source_created_ts
  )

DimProp <- DimPropData |>
  select(
    property_skey,
    address_line1,
    client_property_name,
    client_property_id
  )

Output <- Cashflow |>
  left_join(DimProj, by = join_by(ProjectNumber == project_number)) |>
  left_join(FactProj, by = join_by(project_skey))

test <- DimProj |>
  group_by(project_number) |>
  mutate(count = n()) |>
  filter(count >= 2) |>
  ungroup() |>
  select(-count) |>
  left_join(FactProj, by = join_by(project_skey)) |>
  arrange(project_number)

test2 <- DimProjFullData |>
  group_by(project_number) |>
  mutate(count = n()) |>
  filter(count >= 2) |>
  ungroup() |>
  select(project_skey, project_number, count)

sum(is.na(DimProjFullData$project_number))

test3 <- DimProjData |>
  select(project_skey, project_number, source_partition_id) |>
  group_by(source_partition_id) |>
  mutate(count = n()) |>
  filter(count >= 2) |>
  ungroup()


pjm_knumber <- DimProjData |>
  select(project_number) |>
  distinct()

dim_knumber <- DimProjFullData |>
  select(project_number) |>
  distinct()

setdiff(pjm_knumber, dim_knumber)
