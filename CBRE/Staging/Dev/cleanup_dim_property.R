ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"

# Connect to SQL database
con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = SQL_SERVER,
  database = DB_NAME,
  Trusted_Connection = "Yes"
)

# Load com_dim_property and clean as much as possible
query <- dbSendQuery(
  con,
  "SELECT * FROM CbreStaging.com_dim_property"
)
ComDimProperty <- dbFetch(query, n = -1)
dbClearResult(query)

ComDimPropertyCleaned <- ComDimProperty |>
  mutate(
    across(
      where(is.character),
      ~ if_else(grepl("^\\s*$", .), NA_character_, .)
    )
  ) |>
  mutate(
    across(
      c(
        property_skey
      ),
      as.character
    )
  ) |>
  select(
    property_skey,
    property_status,
    client_property_name,
    client_property_id,
    alternate_property_id,
    associated_property_id,
    associated_project_number,
    property_master_id,
    property_unique_id,
    reporting_code_1,
    reporting_code_3,
    reporting_code_4,
    reporting_code_5,
    reporting_code_6,
    reporting_code_7,
    address_line1,
    source_unique_id,
    source_system_code,
    edp_update_ts
  ) |>
  mutate(
    Identifier = case_when(
      source_system_code == "Kahua Canada" &
        startsWith(client_property_id, "B") ~ client_property_id,
      source_system_code == "Kahua Canada" &
        startsWith(client_property_id, "N") ~ client_property_id,
      source_system_code == "JDE" &
        startsWith(reporting_code_3, "B") ~ reporting_code_3,
      source_system_code == "JDE" &
        startsWith(reporting_code_3, "N") ~ reporting_code_3,
      source_system_code == "JDE" &
        startsWith(reporting_code_4, "B") ~ reporting_code_4,
      source_system_code == "JDE" &
        startsWith(reporting_code_4, "N") ~ reporting_code_4,
      source_system_code == "JDE" &
        startsWith(reporting_code_5, "B") ~ reporting_code_5,
      source_system_code == "JDE" &
        startsWith(reporting_code_5, "N") ~ reporting_code_5,
      source_system_code == "Envizi" &
        startsWith(alternate_property_id, "B") ~ alternate_property_id,
      source_system_code == "Envizi" &
        startsWith(alternate_property_id, "N") ~ alternate_property_id,
      .default = NA_character_
    ),
    .after = property_skey
  ) |>
  select(
    -c(
      reporting_code_1:reporting_code_7
    )
  )

# sum(is.na(ComDimPropertyCleaned$Identifier))

# load dim_property and clean as much as possible
query <- dbSendQuery(
  con,
  "SELECT * FROM CbreStaging.dim_property"
)
DimProperty <- dbFetch(query, n = -1)
dbClearResult(query)

DimPropertyCleaned <- DimProperty |>
  mutate(
    across(
      where(is.character),
      ~ if_else(grepl("^\\s*$", .), NA_character_, .)
    )
  ) |>
  mutate(
    across(
      c(
        property_skey
      ),
      as.character
    )
  ) |>
  mutate(
    Identifier = case_when(
      source_system_code == "JDE" &
        startsWith(reporting_code_3, "B") ~ reporting_code_3,
      source_system_code == "JDE" &
        startsWith(reporting_code_3, "N") ~ reporting_code_3,
      source_system_code == "JDE" &
        startsWith(reporting_code_4, "B") ~ reporting_code_4,
      source_system_code == "JDE" &
        startsWith(reporting_code_4, "N") ~ reporting_code_4,
      source_system_code == "JDE" &
        startsWith(reporting_code_5, "B") ~ reporting_code_5,
      source_system_code == "JDE" &
        startsWith(reporting_code_5, "N") ~ reporting_code_5,
      source_system_code == "SI7" ~ stringr::str_extract(
        client_additional_attrib,
        '([B-N]\\d*)',
        group = TRUE
      ),
      .default = NA_character_
    ),
    # .keep = "unused",
    .after = client_property_name
  ) |>
  select(
    property_skey,
    property_status,
    source_system_code,
    Identifier,
    client_property_id,
    client_property_name,
    address_line1,
    city_name,
    state_province_code,
    source_unique_id,
    associated_project_number,
    location_id,
    property_desc,
    edp_update_ts
  ) |>
  group_by(client_property_name) |>
  tidyr::fill(Identifier, .direction = "updown") |>
  ungroup()

# Try to merge them together

Output <- ComDimPropertyCleaned |>
  full_join(DimPropertyCleaned, by = join_by(property_skey)) |>
  left_join(
    DimPropertyCleaned,
    by = join_by(alternate_property_id == client_property_id)
  ) |>
  rename(Identifier.z = Identifier) |>
  mutate(
    Identifier = case_when(
      !is.na(Identifier.x) ~ Identifier.x,
      !is.na(Identifier.y) ~ Identifier.y,
      !is.na(Identifier.z) ~ Identifier.z
    ),
    .before = everything()
  ) |>
  relocate(
    Identifier,
    Identifier.x,
    Identifier.y,
    Identifier.z,
    property_skey.x,
    property_skey.y,
    .before = everything()
  ) |>
  pivot_longer(
    cols = c(property_skey.x, property_skey.y),
    names_to = "colname",
    values_to = "property_skey"
  ) |>
  select(
    property_skey,
    Identifier,
    property_status.x,
    property_status.y,
    client_property_name.x,
    client_property_name.y,
    client_property_name.z = client_property_name,
    client_property_id.x,
    client_property_id.y,
    address_line1.x,
    address_line1.y,
    address_line1.z = address_line1,
    city_name.x,
    city_name.y,
    state_province_code.x,
    state_province_code.y,
    alternate_property_id,
    associated_property_id,
    associated_project_number.x,
    associated_project_number.y,
    associated_project_number.z = associated_project_number,
    property_master_id,
    property_unique_id,
    location_id.x,
    location_id.y,
    source_unique_id.x,
    source_unique_id.y,
    source_unique_id.z = source_unique_id,
    source_system_code.x,
    source_system_code.y,
    source_system_code.z = source_system_code,
  ) |>
  filter(!is.na(property_skey)) |>
  mutate(
    property_status = case_when(
      !is.na(property_status.x) ~ property_status.x,
      !is.na(property_status.y) ~ property_status.y,
      .default = NA_character_
    ),
    client_property_name = case_when(
      !is.na(client_property_name.x) ~ client_property_name.x,
      !is.na(client_property_name.y) ~ client_property_name.y,
      !is.na(client_property_name.z) ~ client_property_name.z,
      .default = NA_character_
    ),
    client_property_id = case_when(
      !is.na(client_property_id.x) ~ client_property_id.x,
      !is.na(client_property_id.y) ~ client_property_id.y,
      .default = NA_character_
    ),
    address_line1 = case_when(
      !is.na(address_line1.x) ~ address_line1.x,
      !is.na(address_line1.y) ~ address_line1.y,
      !is.na(address_line1.z) ~ address_line1.z,
      .default = NA_character_
    ),
    city_name = case_when(
      !is.na(city_name.x) ~ city_name.x,
      !is.na(city_name.y) ~ city_name.y,
      .default = NA_character_
    ),
    state_province_code = case_when(
      !is.na(state_province_code.x) ~ state_province_code.x,
      !is.na(state_province_code.y) ~ state_province_code.y,
      .default = NA_character_
    ),
    associated_project_number = case_when(
      !is.na(associated_project_number.x) ~ associated_project_number.x,
      !is.na(associated_project_number.y) ~ associated_project_number.y,
      !is.na(associated_project_number.z) ~ associated_project_number.z,
      .default = NA_character_
    ),
    location_id = case_when(
      !is.na(location_id.x) ~ location_id.x,
      !is.na(location_id.y) ~ location_id.y,
      .default = NA_character_
    ),
    source_unique_id = case_when(
      !is.na(source_unique_id.x) ~ source_unique_id.x,
      !is.na(source_unique_id.y) ~ source_unique_id.y,
      !is.na(source_unique_id.z) ~ source_unique_id.z,
      .default = NA_character_
    ),
    source_system_code = case_when(
      !is.na(source_system_code.x) ~ source_system_code.x,
      !is.na(source_system_code.y) ~ source_system_code.y,
      !is.na(source_system_code.z) ~ source_system_code.z,
      .default = NA_character_
    ),
    .keep = "unused",
    .after = Identifier
  ) |>
  mutate(
    id_group = case_when(
      !is.na(associated_project_number) ~ associated_project_number,
      !is.na(alternate_property_id) ~ alternate_property_id,
      !is.na(client_property_id) ~ client_property_id,
      .default = NA_character_
    )
  ) |>
  group_by(id_group) |>
  tidyr::fill(Identifier, .direction = "updown") |>
  ungroup() |>
  mutate(
    AddressEdit = gsub(address_line1),
    .before = address_line1
  )

sum(is.na(Output$Identifier))

# RPD01849 509594 123_1849

query <- dbSendQuery(
  con,
  "SELECT
   DISTINCT property_skey
   FROM CbreStaging.es_fact_invoice"
)
EsFactInvoice <- dbFetch(query, n = -1)
dbClearResult(query)

Test <- EsFactInvoice |>
  left_join(Output, by = join_by(property_skey))

Test_missing <- Test |>
  filter(is.na(Identifier))

query <- dbSendQuery(
  con,
  "SELECT
   *
   FROM CbreStaging.es_fact_invoice
  WHERE property_skey IN ('20725956', '26083601', '26083602')"
)
EsMissing <- dbFetch(query, n = -1)
dbClearResult(query)


query <- dbSendQuery(
  con,
  "SELECT
   *
   FROM CbreStaging.fm_dim_property_extended_attribute
  WHERE property_skey IN ('20725956', '26083601', '26083602')"
)
fm_dim_extended <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(
  con,
  "SELECT
   *
   FROM CbreStaging.fm_benchmark_property_asset_link
  WHERE property_skey IN ('20725956', '26083601', '26083602')"
)
prop_asset_link <- dbFetch(query, n = -1)
dbClearResult(query)

# all have property_skey
# envizi_property_grouping_vw
# fin_dim_property_reporting_code_vw
# fin_jde_h1_5
# pjm_fact_project_vw
