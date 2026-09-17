# property_skey overlap

# CBRE_TABLE_NAME <- "dim_property_vw"
# CBRE_TABLE_NAME <- "com_dim_property_vw"
CBRE_TABLE_NAME <- "envizi_property_grouping_vw"
# Query API
chunk_1 <- call_cbre_api(
  CBRE_TABLE_NAME,
  start_time = "2010-04-01T00:00:00Z",
  end_time = "2026-06-25T23:59:59Z"
)

raw_data <- chunk_1$data

dim_property <- raw_data |>
  select_if(~ !all(is.na(.))) |>
  select_if(~ !all(. == 0)) |>
  select_if(~ !all(. == '-1')) |>
  select_if(~ !all(. == "N/A")) |>
  select_if(~ !all(. == "-")) |>
  select_if(~ !all(. == "")) |>
  select(
    dim_property_skey = property_skey,
    dim_property_status = property_status,
    dim_address_line1 = address_line1,
    dim_city_name = city_name,
    dim_client_property_id = client_property_id,
    dim_source_system_code = source_system_code
    )

com_dim_property <- raw_data |>
  select_if(~ !all(is.na(.))) |>
  select_if(~ !all(. == 0)) |>
  select_if(~ !all(. == '-1')) |>
  select_if(~ !all(. == "N/A")) |>
  select_if(~ !all(. == "-")) |>
  select_if(~ !all(. == "")) |>
  select(
    com_property_skey = property_skey,
    com_property_status = property_status,
    com_address_line1 = address_line1,
    com_city_name = city_name,
    com_client_property_id = client_property_id,
    com_source_system_code = source_system_code
  )

envizi_property_grouping <- raw_data |>
  select_if(~ !all(is.na(.))) |>
  select_if(~ !all(. == 0)) |>
  select_if(~ !all(. == '-1')) |>
  select_if(~ !all(. == "N/A")) |>
  select_if(~ !all(. == "-")) |>
  select_if(~ !all(. == "")) |>
  select(
    envizi_property_skey = property_skey,
    envizi_source_system_code = source_system_code
  ) |>
  distinct()

test <- dim_property |>
  full_join(com_dim_property, by = join_by(dim_property_skey == com_property_skey)) |>
  full_join(envizi_property_grouping, by = join_by(dim_property_skey == envizi_property_skey))

