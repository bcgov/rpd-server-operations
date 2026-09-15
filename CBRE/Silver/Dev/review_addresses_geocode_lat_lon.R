utils::URLdecode(
  "%C5%A1%78%CA%B7%6D%C9%99%CE%B8%6B%CA%B7%C9%99%79%CC%93%C9%99%6D%61%73%C9%99%6D"
)

# get from data chunk template
rawBuilding <- raw_data

rawProperty <- raw_data

# "12th Ave_Kootenay St N"
# 49.51800839, -115.75982525

test <- rawBuilding |>
  filter(is.na(bl_lat))
