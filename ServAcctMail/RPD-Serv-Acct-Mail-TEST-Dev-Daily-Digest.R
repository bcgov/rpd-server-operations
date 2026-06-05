source(here::here("renv/activate.R"))
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

# --- 1. Load & classify log rows ---

log_file <- here::here("logs", paste0("daily_etl_log_", Sys.Date(), ".csv"))
log_raw <- read_csv(log_file, show_col_types = FALSE) |>
  mutate(run_timestamp = as.POSIXct(run_timestamp, tz = "UTC"))

# Orchestrator summary rows: script_name == api_name
orch_rows <- log_raw |>
  filter(script_name == api_name) |>
  arrange(run_timestamp)

# Individual script rows: everything else
script_rows <- log_raw |>
  filter(script_name != api_name) |>
  mutate(
    n_inserted = replace_na(n_inserted, 0L),
    n_updated = replace_na(n_updated, 0L),
    n_deleted = replace_na(n_deleted, 0L)
  ) |>
  arrange(run_timestamp)

# --- 2. Summary stats ---

run_date <- format(Sys.Date(), "%B %d, %Y")
host <- unique(log_raw$host) |> paste(collapse = ", ")
n_orch <- nrow(orch_rows)
n_orch_ok <- sum(orch_rows$status == "SUCCESS")
n_orch_fail <- sum(orch_rows$status %in% c("ERROR", "PARTIAL_FAILURE"))
n_scripts <- nrow(script_rows)
n_scripts_ok <- sum(script_rows$status == "SUCCESS")
n_scripts_fail <- n_scripts - n_scripts_ok
total_ins <- sum(script_rows$n_inserted, na.rm = TRUE)
total_upd <- sum(script_rows$n_updated, na.rm = TRUE)
any_failure <- (n_orch_fail + n_scripts_fail) > 0
# header_colour <- if (!any_failure) "#27ae60" else "#e74c3c"
header_colour <- "#234075"
overall_text <- if (!any_failure) {
  "All runs completed successfully."
} else {
  glue("{n_orch_fail + n_scripts_fail} failure(s) detected — review below.")
}

# --- 3. HTML helpers ---

status_badge <- function(s) {
  colour <- switch(
    s,
    SUCCESS = "#27ae60",
    PARTIAL_FAILURE = "#e67e22",
    ERROR = "#e74c3c",
    "#95a5a6"
  )
  icon <- if (s == "SUCCESS") "✓" else "✗"
  glue(
    "<span style='background:{colour}; color:white; padding:2px 8px;
        border-radius:3px; font-size:12px; font-weight:bold;'>{icon} {s}</span>"
  )
}

fmt_num <- function(x) formatC(x, format = "d", big.mark = ",")

fmt_dur <- function(secs) {
  if (is.na(secs)) {
    return("—")
  }
  if (secs < 60) {
    return(glue("{round(secs, 1)}s"))
  }
  glue("{floor(secs/60)}m {round(secs %% 60)}s")
}

stat_card <- function(value, label, colour) {
  glue(
    '
    <td style="background:{colour}; color:white; text-align:center;
               padding:16px; border-radius:6px; width:18%;">
      <div style="font-size:26px; font-weight:bold;">{value}</div>
      <div style="font-size:11px; margin-top:4px; opacity:0.9;">{label}</div>
    </td>
    <td style="width:2%;"></td>
  '
  )
}

# --- 4. Orchestrator summary table ---

orch_table_rows <- apply(orch_rows, 1, \(r) {
  dur_raw <- suppressWarnings(as.numeric(r["duration"]))
  glue(
    '
    <tr style="border-bottom:1px solid #ecf0f1;">
      <td style="padding:10px 12px;">{status_badge(r["status"])}</td>
      <td style="padding:10px 12px; font-family:monospace; font-size:13px;">
        {r["api_name"]}</td>
      <td style="padding:10px 12px; color:#555;">{r["message"]}</td>
      <td style="padding:10px 12px; text-align:right; color:#777; font-size:13px;">
        {fmt_dur(dur_raw)}</td>
    </tr>
  '
  )
}) |>
  paste(collapse = "\n")

orch_table <- glue(
  '
  <table style="width:100%; border-collapse:collapse; font-size:14px;
                font-family:Arial,sans-serif; margin-bottom:24px;">
    <thead>
      <tr style="background:#2c3e50; color:white;">
        <th style="padding:10px 12px; text-align:left;">Status</th>
        <th style="padding:10px 12px; text-align:left;">Orchestrator</th>
        <th style="padding:10px 12px; text-align:left;">Summary</th>
        <th style="padding:10px 12px; text-align:right;">Duration</th>
      </tr>
    </thead>
    <tbody>{orch_table_rows}</tbody>
  </table>
'
)

# --- 5. Individual script detail table ---

script_table_rows <- apply(script_rows, 1, \(r) {
  dur_raw <- suppressWarnings(as.numeric(r["duration"]))
  row_bg <- if (r["status"] != "SUCCESS") "#fff5f5" else "white"
  glue(
    '
    <tr style="border-bottom:1px solid #ecf0f1; background:{row_bg};">
      <td style="padding:8px 12px;">{status_badge(r["status"])}</td>
      <td style="padding:8px 12px; font-family:monospace; font-size:12px;">
        {r["api_name"]}</td>
      <td style="padding:8px 12px; font-family:monospace; font-size:12px;">
        {r["table_name"]}</td>
      <td style="padding:8px 12px; text-align:right;">{fmt_num(as.integer(r["n_inserted"]))}</td>
      <td style="padding:8px 12px; text-align:right;">{fmt_num(as.integer(r["n_updated"]))}</td>
      <td style="padding:8px 12px; text-align:right; color:#777; font-size:12px;">
        {fmt_dur(dur_raw)}</td>
    </tr>
  '
  )
}) |>
  paste(collapse = "\n")

script_table <- glue(
  '
  <table style="width:100%; border-collapse:collapse; font-size:14px;
                font-family:Arial,sans-serif; margin-bottom:24px;">
    <thead>
      <tr style="background:#34495e; color:white;">
        <th style="padding:10px 12px; text-align:left;">Status</th>
        <th style="padding:10px 12px; text-align:left;">API</th>
        <th style="padding:10px 12px; text-align:left;">Table</th>
        <th style="padding:10px 12px; text-align:right;">Inserted</th>
        <th style="padding:10px 12px; text-align:right;">Updated</th>
        <th style="padding:10px 12px; text-align:right;">Duration</th>
      </tr>
    </thead>
    <tbody>{script_table_rows}</tbody>
  </table>
'
)

# --- 6. Assemble full HTML body ---

email_html <- glue(
  '
<!DOCTYPE html>
<html>
<body style="margin:0; padding:0; background:#f4f6f8; font-family:Arial,sans-serif;">
<div style="max-width:800px; margin:24px auto; background:white;
            border-radius:8px; overflow:hidden;
            box-shadow:0 2px 8px rgba(0,0,0,0.08);">

  <!-- Header -->
  <div style="background:{header_colour}; padding:24px 32px;">
    <h1 style="margin:0; color:white; font-size:22px;">ETL Daily Digest</h1>
    <p style="margin:6px 0 0; color:rgba(255,255,255,0.85); font-size:14px;">
      {run_date} &nbsp;|&nbsp; Host: {host}
    </p>
  </div>

  <!-- Stat cards -->
  <div style="padding:24px 32px 8px;">
    <table style="width:100%; border-collapse:collapse;">
      <tr>
        {stat_card(n_orch,            "Orchestrators",   header_colour)}
        {stat_card(n_scripts,         "Scripts Run",     header_colour)}
        {stat_card(fmt_num(total_ins),"Rows Inserted",   header_colour)}
        {stat_card(fmt_num(total_upd),"Rows Updated",    header_colour)}
        {stat_card(n_scripts_fail,    "Failures",        header_colour)}
      </tr>
    </table>
  </div>

  <!-- Status line -->
  <div style="padding:8px 32px 16px;">
    <p style="color:{header_colour}; font-weight:bold; margin:0;">{overall_text}</p>
  </div>

  <!-- Orchestrator table -->
  <div style="padding:0 32px 8px;">
    <h2 style="font-size:16px; color:#2c3e50; border-bottom:2px solid #ecf0f1;
               padding-bottom:8px;">Orchestrator Summary</h2>
    {orch_table}
  </div>

  <!-- Script detail table -->
  <div style="padding:0 32px 24px;">
    <h2 style="font-size:16px; color:#2c3e50; border-bottom:2px solid #ecf0f1;
               padding-bottom:8px;">Script Detail</h2>
    {script_table}
  </div>

  <!-- Footer -->
  <div style="background:#f4f6f8; padding:12px 32px; text-align:center;">
    <p style="color:#aaa; font-size:11px; margin:0;">
      Generated automatically by the ETL event logger &nbsp;|&nbsp; {Sys.time()}
    </p>
  </div>

</div>
</body>
</html>
'
)

# --- 7. Send via Graph API ---

appId <- "267fc93d-3fa0-4942-88f9-02f4f7fee693"
tenantId <- "6fdb5200-3d0d-4a8a-b036-d3685e359adc"
mailbox <- "RPD.SpBooking@gov.bc.ca"
recipient <- "david.rattray@gov.bc.ca"

credential <- keyring::key_get(service = "GraphAPI", username = appId)

token <- get_azure_token(
  resource = "https://graph.microsoft.com",
  tenant = tenantId,
  app = appId,
  password = credential,
  auth_type = "client_credentials"
)

access_token <- token$credentials$access_token

subject <- glue(
  "ETL Daily Digest — {run_date} [{if(!any_failure) 'OK' else 'FAILURES'}]"
)

request(glue("https://graph.microsoft.com/v1.0/users/{mailbox}/sendMail")) |>
  req_headers(
    Authorization = paste("Bearer", access_token),
    `Content-Type` = "application/json"
  ) |>
  req_body_json(list(
    message = list(
      subject = subject,
      body = list(contentType = "HTML", content = email_html),
      toRecipients = list(
        list(emailAddress = list(address = recipient))
      )
    ),
    saveToSentItems = FALSE
  )) |>
  req_perform()

# --- 8. Local preview (dev only) ---
# preview_path <- here::here("output", "digest_preview.html")
# writeLines(email_html, preview_path)
# browseURL(preview_path)
