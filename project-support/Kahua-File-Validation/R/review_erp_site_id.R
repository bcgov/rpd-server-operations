query <- dbSendQuery(con, "SELECT * FROM CbreStaging.archibus_dp")
DepartmentData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.archibus_company")
CompanyData <- dbFetch(query, n = -1)
dbClearResult(query)
# Think these tables have what is needed to fill in blanks for erp vendor and site id
