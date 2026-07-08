query <- dbSendQuery(
  con,
  "SELECT
   property_skey,
   Identifier,
   client_property_name,
   client_property_id,
   address_line1,
   city_name,
   state_province_code,
   associated_project_number,
   location_id
   FROM CbreStaging.dim_property"
)
DimProperty <- dbFetch(query, n = -1)
dbClearResult(query)

DimPropSkey <- DimProperty |>
  select(
    property_skey,
    Identifier,
    address_line1,
    city_name,
    state_province_code,
    associated_project_number,
    location_id
  )

DimPropId <- DimProperty |>
  select(
    client_property_name,
    client_property_id,
    Identifier,
    address_line1,
    city_name,
    state_province_code,
    associated_project_number,
    location_id
  )

query <- dbSendQuery(
  con,
  "SELECT
   property_skey,
   property_status,
   address_line1,
   client_property_name,
   client_property_id,
   property_master_id,
   alternate_property_id,
   property_unique_id,
   cbre_standardized_property_skey,
   associated_property_id,
   associated_project_number,
   reporting_code_1,
   reporting_code_2,
   reporting_code_3,
   reporting_code_4,
   reporting_code_5,
   reporting_code_6,
   reporting_code_7
  FROM CbreStaging.com_dim_property"
)

ComProperty <- dbFetch(query, n = -1)
dbClearResult(query)

Output <- ComProperty |>
  left_join(DimProperty, by = join_by(property_skey)) |>
  left_join(
    DimProperty,
    by = join_by(alternate_property_id == client_property_id)
  )

Final <- Output |>
  mutate(
    Identifier = case_when(
      startsWith(client_property_id.x, "B") ~ client_property_id.x,
      startsWith(client_property_id.x, "N") ~ client_property_id.x, # No Identifier = 14068
      startsWith(Identifier.x, "B") ~ Identifier.x,
      startsWith(Identifier.x, "N") ~ Identifier.x, # No Identifier = 10637
      startsWith(Identifier.y, "B") ~ Identifier.y,
      startsWith(Identifier.y, "N") ~ Identifier.y, # No Identifier = 9901
      startsWith(reporting_code_3, "B") ~ reporting_code_3,
      startsWith(reporting_code_3, "N") ~ reporting_code_3, # No Identifier = 9726
      startsWith(reporting_code_4, "B") ~ reporting_code_4,
      startsWith(reporting_code_4, "N") ~ reporting_code_4, # No Identifier = 8029
      startsWith(reporting_code_5, "B") ~ reporting_code_5,
      startsWith(reporting_code_5, "N") ~ reporting_code_5, # No Identifier = 7701
      .default = "No Identifier"
    ),
    .after = property_skey.x
  )

sum(Final$Identifier == "No Identifier")

Remainder <- Final |>
  filter(Identifier == "No Identifier")

Final2 <- Final |>
  filter(Identifier != "No Identifier") |>
  mutate(
    property_skey_com = property_skey.x,
    property_skey_dim = property_skey.y,
    .after = Identifier,
    .keep = "unused"
  ) |>
  relocate(
    address_line1.x,
    address_line1.y,
    address_line1,
    client_property_name.x,
    client_property_name.y,
    client_property_name,
    client_property_id.x,
    client_property_id.y,
    city_name.x,
    city_name.y,
    state_province_code.x,
    state_province_code.y,
    property_master_id,
    alternate_property_id,
    property_unique_id,
    associated_property_id,
    .after = property_skey_dim
  )
