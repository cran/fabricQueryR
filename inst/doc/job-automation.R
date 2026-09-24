## ----include = FALSE----------------------------------------------------------
knitr::opts_chunk$set(collapse = TRUE, comment = "#>", eval = FALSE)

## ----eval = FALSE-------------------------------------------------------------
# library(fabricQueryR)
# 
# notebook <- fabric_notebooks("Analytics workspace")[[1]]

## ----eval = FALSE-------------------------------------------------------------
# job <- notebook$run(
#   parameters = list(run_date = Sys.Date(), full_load = FALSE)
# )
# result <- notebook$wait(job, timeout = 900, cancel_on_timeout = TRUE)
# result$status

## ----eval = FALSE-------------------------------------------------------------
# detailed <- notebook$status(
#   job,
#   notebook_details = TRUE,
#   respect_retry_after = FALSE
# )
# detailed$exit_value

## ----eval = FALSE-------------------------------------------------------------
# history <- notebook$instances()
# 
# history[[1]]$invoke_type
# history[[1]]$status
# history[[1]]$start_time
# history[[1]]$failure_reason

## ----eval = FALSE-------------------------------------------------------------
# latest <- notebook$status(history[[1]])

## ----eval = FALSE-------------------------------------------------------------
# daily <- fabric_job_schedule_config(
#   "Daily",
#   start_time = "2026-10-01T00:00:00Z",
#   end_time = "2027-10-01T00:00:00Z",
#   time_zone = "W. Europe Standard Time",
#   times = "08:30"
# )

## ----eval = FALSE-------------------------------------------------------------
# weekly <- fabric_job_schedule_config(
#   "Weekly",
#   start_time = "2026-10-01T00:00:00Z",
#   end_time = "2027-10-01T00:00:00Z",
#   time_zone = "W. Europe Standard Time",
#   times = "07:30",
#   weekdays = c("Monday", "Thursday")
# )

## ----eval = FALSE-------------------------------------------------------------
# schedule <- notebook$schedule_create(weekly, enabled = TRUE)
# schedules <- notebook$schedules()

## ----eval = FALSE-------------------------------------------------------------
# disabled <- notebook$schedule_update(
#   schedule,
#   enabled = FALSE
# )
# 
# restarted <- notebook$schedule_update(
#   schedule,
#   enabled = TRUE
# )

## ----eval = FALSE-------------------------------------------------------------
# notebook$schedule_delete(schedule, confirm = TRUE)

