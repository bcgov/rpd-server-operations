CompanyData <- extract_cbre_data("archibus_company")

Company <- CompanyData |>
  select_if(~ !all(is.na(.))) |>
  select_if(~ !all(. == 0)) |>
  select_if(~ !all(. == '-1')) |>
  select_if(~ !all(. == "N/A")) |>
  select_if(~ !all(. == "-")) |>
  rename(
    CompanyId = company_vendor,
    CompanyName = company_name,
    Address = company_address1,
    City = company_city_id,
    CompanyStatus = company_status_pobc
  ) |>
  relocate(CompanyId, CompanyName, Address, City, CompanyStatus) |>
  group_by(CompanyId, CompanyName, Address, City, CompanyStatus) |>
  mutate(count = n()) |>
  arrange(desc(count)) |>
  ungroup()

openxlsx2::write_xlsx(Company, here::here("VendorData.xlsx"))

CompanySet <- Company |>
  select(CompanyId, CompanyName, Address, City, CompanyStatus) |>
  distinct()

ReportVendors <- currentKFile |>
  select(
    `ERP Vendor ID`,
    `ERP Vendor Name`
  ) |>
  distinct() |>
  group_by(`ERP Vendor ID`) |>
  mutate(DuplicationFlag = n()) |>
  ungroup() |>
  arrange(desc(DuplicationFlag))

CbreMissingVendors <- ReportVendors |>
  full_join(CompanySet, by = join_by(`ERP Vendor ID` == CompanyId)) |>
  filter(
    is.na(CompanyName) & is.na(Address) & is.na(City) & is.na(CompanyStatus)
  )
