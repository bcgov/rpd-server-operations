con_test <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = sql_server,
  database = db_name,
  Trusted_Connection = "Yes"
)

con_prod <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = sql_server,
  database = db_name,
  Trusted_Connection = "Yes"
)

# CSR
query <- dbSendQuery(con_test, "SELECT COUNT(*) FROM JIRA.CSR")
CSR_TEST <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con_prod, "SELECT COUNT(*) FROM JIRA.CSR")
CSR_PROD <- dbFetch(query, n = -1)
dbClearResult(query)

CSR_TEST
CSR_PROD

# GPOPR
query <- dbSendQuery(con_test, "SELECT COUNT(*) FROM JIRA.GPOPR")
GPOPR_TEST <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con_prod, "SELECT COUNT(*) FROM JIRA.GPOPR")
GPOPR_PROD <- dbFetch(query, n = -1)
dbClearResult(query)

GPOPR_TEST
GPOPR_PROD

# PAR
query <- dbSendQuery(con_test, "SELECT COUNT(*) FROM JIRA.PAR")
PAR_TEST <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con_prod, "SELECT COUNT(*) FROM JIRA.PAR")
PAR_PROD <- dbFetch(query, n = -1)
dbClearResult(query)

PAR_TEST
PAR_PROD

# PSO
query <- dbSendQuery(con_test, "SELECT COUNT(*) FROM JIRA.PSO")
PSO_TEST <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con_prod, "SELECT COUNT(*) FROM JIRA.PSO")
PSO_PROD <- dbFetch(query, n = -1)
dbClearResult(query)

PSO_TEST
PSO_PROD

# RBAS
query <- dbSendQuery(con_test, "SELECT COUNT(*) FROM JIRA.RBAS")
RBAS_TEST <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con_prod, "SELECT COUNT(*) FROM JIRA.RBAS")
RBAS_PROD <- dbFetch(query, n = -1)
dbClearResult(query)

RBAS_TEST
RBAS_PROD

# RPR
query <- dbSendQuery(con_test, "SELECT COUNT(*) FROM JIRA.RPR")
RPR_TEST <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con_prod, "SELECT COUNT(*) FROM JIRA.RPR")
RPR_PROD <- dbFetch(query, n = -1)
dbClearResult(query)

RPR_TEST
RPR_PROD

# SBP
query <- dbSendQuery(con_test, "SELECT COUNT(*) FROM JIRA.SBP")
SBP_TEST <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con_prod, "SELECT COUNT(*) FROM JIRA.SBP")
SBP_PROD <- dbFetch(query, n = -1)
dbClearResult(query)

SBP_TEST
SBP_PROD

# SBPSB
query <- dbSendQuery(con_test, "SELECT COUNT(*) FROM JIRA.SBPSB")
SBPSB_TEST <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con_prod, "SELECT COUNT(*) FROM JIRA.SBPSB")
SBPSB_PROD <- dbFetch(query, n = -1)
dbClearResult(query)

SBPSB_TEST
SBPSB_PROD
