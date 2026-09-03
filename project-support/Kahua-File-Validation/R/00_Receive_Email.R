source(here::here("utilities/R/utilities.R"))

# Load Libraries
library(dplyr)
library(glue)
library(here)
library(httr2)
library(keyring)
library(lubridate)
library(readr)
library(AzureAuth)

# Graph API ####

appId <- "267fc93d-3fa0-4942-88f9-02f4f7fee693"
tenantId <- "6fdb5200-3d0d-4a8a-b036-d3685e359adc"
mailbox <- "RPD.SpBooking@gov.bc.ca"
credential <- keyring::key_get(service = "GraphAPI", username = appId)

# Set proxy environment variable
# Sys.setenv(HTTPS_PROXY = "142.34.229.249:8080")

token <- get_azure_token(
  resource = "https://graph.microsoft.com",
  tenant = tenantId,
  app = appId,
  password = credential,
  auth_type = "client_credentials",
  use_cache = FALSE
)

access_token <- token$credentials$access_token

# Build the request
resp <- request("https://graph.microsoft.com/v1.0") |>
  req_url_path_append("users", mailbox, "mailFolders", "inbox", "messages") |>
  req_url_query(
    `$top` = 25, # number of messages to return
    `$orderby` = "receivedDateTime desc", # most recent first
    `$select` = "subject,from,receivedDateTime,bodyPreview,isRead"
  ) |>
  req_auth_bearer_token(access_token) |>
  req_perform()

emails <- resp_body_json(resp)


# get full email and check for attachments
resp <- request("https://graph.microsoft.com/v1.0") |>
  req_url_path_append("users", mailbox, "messages", emails$value[[1]]$id) |>
  req_url_query(
    `$select` = "subject,from,receivedDateTime,body,hasAttachments"
  ) |>
  req_auth_bearer_token(access_token) |>
  req_perform()

msg <- resp_body_json(resp)

# Full HTML (or text) body
body_content <- msg$body$content
body_type <- msg$body$contentType # "html" or "text"

# Check before bothering with attachments
msg$hasAttachments

# Has attachments ####
resp_att <- request("https://graph.microsoft.com/v1.0") |>
  req_url_path_append(
    "users",
    mailbox,
    "messages",
    emails$value[[1]]$id,
    "attachments"
  ) |>
  req_auth_bearer_token(access_token) |>
  req_perform()

attachments <- resp_body_json(resp_att)$value

library(base64enc)

att <- attachments[[1]]
raw_bytes <- base64decode(att$contentBytes)
writeBin(
  raw_bytes,
  file.path("input/KahuaPayable", paste0(Sys.Date(), "-RPDKahuaPayable.xlsx"))
)
