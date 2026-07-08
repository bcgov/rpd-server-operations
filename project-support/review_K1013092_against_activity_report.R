query <- dbSendQuery(
  con,
  "SELECT project_skey, project_number, project_name FROM CbreStaging.pjm_dim_project
  WHERE project_number = 'K1013092'"
)
PjmDimProjectData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(
  con,
  "SELECT * FROM CbreStaging.pjm_dim_project
  WHERE project_number = 'K1013092'"
)
PjmDimProjFullData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(
  con,
  "SELECT
    project_skey,
    project_activity_skey,
    paid,
    retained,
    invoiced,
    awarded_amount,
    budget_estimated_total_value,
    budget_approved_changes_total_value,
    budget_approved_adjustment_total_value,
    budget_approved_total_value,
    cost_original_total_value,
    cost_approved_changes_total_value,
    cost_pending_commitments_total_value,
    cost_pending_changes_total_value,
    cost_projected_changes_total_value,
    payables_remaining_total_value
  FROM CbreStaging.pjm_fact_project_activity
  WHERE project_skey = '9756826'"
)
PjmFactProjectActivityData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(
  con,
  "SELECT
    *
  FROM CbreStaging.pjm_fact_project_activity
  WHERE project_skey = '9756826'"
)
ProjActivityData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(
  con,
  "SELECT * FROM CbreStaging.pjm_fact_project
  WHERE project_skey = '9756826'"
)
PjmFactProjectData <- dbFetch(query, n = -1)
dbClearResult(query)
