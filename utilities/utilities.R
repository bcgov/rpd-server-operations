list.files(
  here::here("utilities/Scripts/"),
  pattern = "\\.R$",
  full.names = TRUE
) |>
  purrr::walk(source)
