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

# Extract the access token string from the AzureAuth token object
access_token <- token$credentials$access_token

# The mailbox you want to read (your service account's UPN or object ID)
mailbox <- "RPD.SpBooking@gov.bc.ca"

# Retrieve the 10 most recent emails from the inbox
response <- request(paste0(
  "https://graph.microsoft.com/v1.0/users/",
  mailbox,
  "/mailFolders/inbox/messages"
)) |>
  req_headers(
    Authorization = paste("Bearer", access_token),
    `Content-Type` = "application/json"
  ) |>
  req_url_query(
    `$top` = 1000,
    `$orderby` = "receivedDateTime desc",
    `$select` = "subject,from,receivedDateTime,bodyPreview,isRead"
  ) |>
  req_perform()

# Parse the JSON response
emails_raw <- response |> resp_body_json()

# Flatten into a tidy data frame
emails <- emails_raw$value |>
  lapply(\(msg) {
    tibble(
      subject = msg$subject %||% NA_character_,
      from = msg$from$emailAddress$address %||% NA_character_,
      received = msg$receivedDateTime,
      body_preview = msg$bodyPreview %||% NA_character_,
      is_read = msg$isRead
    )
  }) |>
  bind_rows()
