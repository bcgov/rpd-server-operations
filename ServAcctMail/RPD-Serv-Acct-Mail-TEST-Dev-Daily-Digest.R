# Load Libraries
library(AzureAuth)
library(blastula)
library(dplyr)
library(glue)
# library(gt)
library(httr2)
library(keyring)
library(readr)

# --- 1. Load & prep today's log ---

log_file <- here::here("logs", paste0("daily_etl_log_", Sys.Date(), ".csv"))

log_raw <- read_csv(log_file, show_col_types = FALSE)

# Keep only the table-level ETL rows (those with real insert/update counts)
etl_rows <- log_raw |>
  filter(!is.na(table_name)) |>
  mutate(
    run_timestamp = as.POSIXct(run_timestamp, tz = "UTC"),
    n_inserted = replace_na(n_inserted, 0L),
    n_updated = replace_na(n_updated, 0L),
    n_deleted = replace_na(n_deleted, 0L)
  )

# Summary stats for the header cards
n_total <- nrow(etl_rows)
n_success <- sum(etl_rows$status == "SUCCESS")
n_failed <- n_total - n_success
total_ins <- sum(etl_rows$n_inserted)
total_upd <- sum(etl_rows$n_updated)
run_date <- format(Sys.Date(), "%B %d, %Y")
host <- unique(etl_rows$host) |> paste(collapse = ", ")

# --- 2. Build the HTML table ---
status_icon <- function(s) ifelse(s == "SUCCESS", "✅", "❌")

table_rows <- etl_rows |>
  mutate(
    status_icon = status_icon(status),
    n_inserted = formatC(n_inserted, format = "d", big.mark = ","),
    n_updated = formatC(n_updated, format = "d", big.mark = ",")
  ) |>
  select(status_icon, api_name, table_name, n_inserted, n_updated, message)

html_rows <- apply(table_rows, 1, \(r) {
  glue(
    "<tr>
     <td style='padding:8px 12px; text-align:center;'>{r['status_icon']}</td>
     <td style='padding:8px 12px; font-family:monospace; font-size:13px;'>{r['api_name']}</td>
     <td style='padding:8px 12px; font-family:monospace; font-size:13px;'>{r['table_name']}</td>
     <td style='padding:8px 12px; text-align:right;'>{r['n_inserted']}</td>
     <td style='padding:8px 12px; text-align:right;'>{r['n_updated']}</td>
   </tr>"
  )
}) |>
  paste(collapse = "\n")

etl_table_html <- glue(
  '
<table style="width:100%; border-collapse:collapse; font-size:14px; font-family:Arial,sans-serif;">
  <thead>
    <tr style="background-color:#2c3e50; color:#ffffff;">
      <th style="padding:10px 12px;">Status</th>
      <th style="padding:10px 12px; text-align:left;">API</th>
      <th style="padding:10px 12px; text-align:left;">Table</th>
      <th style="padding:10px 12px; text-align:right;">Inserted</th>
      <th style="padding:10px 12px; text-align:right;">Updated</th>
    </tr>
  </thead>
  <tbody>
    {html_rows}
  </tbody>
</table>
'
)

# --- 3. Compose the blastula email ---

overall_status_text <- if (n_failed == 0) {
  "All runs completed successfully."
} else {
  glue("{n_failed} run(s) failed — review below.")
}
header_colour <- if (n_failed == 0) "#27ae60" else "#e74c3c"

email <- compose_email(
  body = blocks(
    block_title("ETL Daily Digest"),
    block_text(glue::glue(
      "<p style='color:#666; margin:0;'>{run_date} &nbsp;|&nbsp; Host: <strong>{host}</strong></p>"
    )),
    block_spacer(),

    # Summary cards as a simple inline table
    block_text(glue(
      '
      <table style="width:100%; border-collapse:collapse; margin-bottom:16px;">
        <tr>
          <td style="background:{header_colour}; color:white; text-align:center; padding:16px; border-radius:6px; width:25%;">
            <div style="font-size:28px; font-weight:bold;">{n_total}</div>
            <div style="font-size:12px; margin-top:4px;">Total Runs</div>
          </td>
          <td style="width:2%;"></td>
          <td style="background:#27ae60; color:white; text-align:center; padding:16px; border-radius:6px; width:25%;">
            <div style="font-size:28px; font-weight:bold;">{n_success}</div>
            <div style="font-size:12px; margin-top:4px;">Successful</div>
          </td>
          <td style="width:2%;"></td>
          <td style="background:#2980b9; color:white; text-align:center; padding:16px; border-radius:6px; width:25%;">
            <div style="font-size:28px; font-weight:bold;">{formatC(total_ins, format="d", big.mark=",")}</div>
            <div style="font-size:12px; margin-top:4px;">Rows Inserted</div>
          </td>
          <td style="width:2%;"></td>
          <td style="background:#8e44ad; color:white; text-align:center; padding:16px; border-radius:6px; width:25%;">
            <div style="font-size:28px; font-weight:bold;">{formatC(total_upd, format="d", big.mark=",")}</div>
            <div style="font-size:12px; margin-top:4px;">Rows Updated</div>
          </td>
        </tr>
      </table>
    '
    )),

    block_text(glue(
      "<p style='color:{header_colour}; font-weight:bold;'>{overall_status_text}</p>"
    )),
    block_text(etl_table_html),
    block_spacer(),
    block_text(
      "<p style='color:#aaa; font-size:12px;'>Generated automatically by the ETL event logger.</p>"
    )
  )
)

# --- 4. Extract HTML and send via Graph API ---

# Setup Graph API
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

email_html <- as.character(email$html_str)

recipient <- "david.rattray@gov.bc.ca"

request(
  "https://graph.microsoft.com/v1.0/users/{mailbox}/sendMail" |> glue()
) |>
  req_headers(
    Authorization = paste("Bearer", access_token),
    `Content-Type` = "application/json"
  ) |>
  req_body_json(list(
    message = list(
      subject = glue(
        "ETL Daily Digest — {run_date} [{if(n_failed==0) 'OK' else 'FAILURES'}]"
      ),
      body = list(
        contentType = "HTML",
        content = email_html
      ),
      toRecipients = list(
        list(emailAddress = list(address = recipient))
      )
    ),
    saveToSentItems = FALSE
  )) |>
  req_dry_run()
req_perform()

htmltools::save_html(
  blastula::get_html_str(email),
  file = here("output/digest_preview.html")
)
browseURL(here("output/digest_preview.html"))

str(email, max.level = 1) # see the slot names
class(email$html_str) # should be "character"
nchar(email$html_str) # should be a large number

# A safer extraction that works across blastula versions
email_html <- blastula::get_html_str(email) # returns the string without writing to disk

email_html
