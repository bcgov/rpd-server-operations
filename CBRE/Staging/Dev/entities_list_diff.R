last_list <- openxlsx2::read_xlsx(
  "C:/Users/drattray/Downloads/CBRE_Entities_List.xlsx"
)

lastListCols <- last_list |> select(entity)

currentListCols <- entities |> select(entity)

diff <- setdiff(currentListCols, lastListCols)
