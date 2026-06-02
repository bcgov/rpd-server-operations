library(AzureAuth)
library(dplyr)
library(httr2)
library(keyring)

appId <- "267fc93d-3fa0-4942-88f9-02f4f7fee693"
tenantId <- "6fdb5200-3d0d-4a8a-b036-d3685e359adc"


credential <- keyring::key_get(
  service = "GraphAPI",
  username = appId
)

token <- get_azure_token(
  resource = "https://graph.microsoft.com",
  tenant = tenantId,
  app = appId,
  password = credential,
  auth_type = "client_credentials"
)
