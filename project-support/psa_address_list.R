query <- dbSendQuery(con, "SELECT * FROM RealProperty.SpaceAllocation")
SpaceAllocation <- dbFetch(query, n = -1)
dbClearResult(query)


query <- dbSendQuery(con, "SELECT * FROM RealProperty.FacilityDetail")
FacilityDetail <- dbFetch(query, n = -1)
dbClearResult(query)


output <- FacilityDetail |>
  filter(!is.na(BuildingId)) |>
  arrange(GeoFlag, Identifier) |>
  select(
    Identifier,
    BuildingId,
    Name,
    GeoFlag,
    Address,
    City,
    linkAddress,
    linkCity,
    geoAddress,
    geoCity,
    Precision,
    Score,
    FacilityType,
    PrimaryUse,
    lat,
    lon
  )

openxlsx2::write_xlsx(
  output,
  file = here::here("output/FacilityDetailBuildings.xlsx")
)
