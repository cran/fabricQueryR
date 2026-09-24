## ----include = FALSE----------------------------------------------------------
knitr::opts_chunk$set(collapse = TRUE, comment = "#>", eval = FALSE)

## ----eval = FALSE-------------------------------------------------------------
# library(fabricQueryR)
# 
# database <- fabric_kql_databases("Telemetry workspace")[[1]]
# database$ingestion_service_uri

## ----eval = FALSE-------------------------------------------------------------
# written <- database$write_table(
#   table = "Events",
#   data = data.frame(
#     id = 1:3,
#     category = c("A", "B", "A"),
#     amount = c(10.5, 20, 30.5)
#   ),
#   create_if_missing = TRUE,
#   ingest_if_not_exists = "r-events-2026-08-14"
# )
# 
# written$status$state
# written$rows
# written$staging_retained

## ----eval = FALSE-------------------------------------------------------------
# source <- paste0(
#   "https://onelake.dfs.fabric.microsoft.com/",
#   "<workspace-id>/<lakehouse-id>",
#   "/Files/events/2026-08-14.csv;impersonate"
# )

## ----eval = FALSE-------------------------------------------------------------
# mapping <- Sys.getenv("FABRIC_KQL_INGESTION_MAPPING", unset = "")
# 
# ingestion <- database$ingest(
#   table = "Events",
#   sources = source,
#   format = "csv",
#   mapping = if (nzchar(mapping)) mapping else NULL,
#   ignore_first_record = TRUE,
#   tags = "source:daily-export",
#   ingest_if_not_exists = "events-2026-08-14"
# )
# 
# ingestion$id
# ingestion$sources$source_id

## ----eval = FALSE-------------------------------------------------------------
# snapshot <- database$ingestion_status(ingestion)
# snapshot$state
# snapshot$counts

## ----eval = FALSE-------------------------------------------------------------
# result <- database$ingestion_wait(
#   ingestion,
#   timeout = 900,
#   poll_interval = 2
# )
# 
# result$state
# result$details

## ----eval = FALSE-------------------------------------------------------------
# result <- database$ingestion_wait(
#   ingestion,
#   error_on_failure = FALSE
# )
# 
# failed <- subset(
#   result$details,
#   status %in% c("Failed", "Canceled")
# )
# failed[c("source_id", "error_code", "failure_status", "message")]

## ----eval = FALSE-------------------------------------------------------------
# loaded <- database$query(
#   query = "Events | where ingestion_time() > ago(1h) | take 100"
# )

## ----eval = FALSE-------------------------------------------------------------
# dataset <- arrow::open_dataset("local-parquet-directory")
# 
# written <- database$write_table(
#   table = "Events",
#   data = dataset,
#   mapping = "EventsParquet"
# )

## ----eval = FALSE-------------------------------------------------------------
# lakehouse <- fabric_lakehouses("Telemetry workspace")[[1]]
# 
# exported <- database$export(
#   query = "Events | where amount > 0",
#   destination = lakehouse,
#   path = "Files/exports/events-positive-amount",
#   format = "parquet",
#   name_prefix = "events",
#   compression_type = "snappy"
# )
# 
# exported$state
# exported$records
# exported$artifacts

