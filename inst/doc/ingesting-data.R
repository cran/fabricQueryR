## ----include = FALSE----------------------------------------------------------
knitr::opts_chunk$set(collapse = TRUE, comment = "#>", eval = FALSE)

## ----eval = FALSE-------------------------------------------------------------
# library(fabricQueryR)
# 
# orders <- data.frame(
#   order_id = 1:3,
#   order_date = as.Date(c("2026-08-12", "2026-08-13", "2026-08-14")),
#   amount = c(10.50, 20, 30.25)
# )
# 
# workspaces <- fabric_workspaces()
# matches <- Filter(
#   \(x) identical(x$displayName, "Analytics workspace"),
#   workspaces
# )
# stopifnot(length(matches) == 1L)
# workspace <- matches[[1L]]
# 
# lakehouse <- workspace$lakehouses()[[1L]]

## ----eval = FALSE-------------------------------------------------------------
# write_result <- lakehouse$write_table(
#   table = "orders_from_r",
#   data = orders,
#   mode = "Overwrite"
# )
# 
# write_result$rows
# write_result$staging_retained

## ----eval = FALSE-------------------------------------------------------------
# check <- lakehouse$read_table(
#   table = "orders_from_r",
#   limit = 10L
# )
# 
# check

## ----eval = FALSE-------------------------------------------------------------
# lakehouse$onelake_write_file(
#   path = "Files/exports/orders.parquet",
#   data = orders
# )

## ----eval = FALSE-------------------------------------------------------------
# lakehouse$onelake_upload(
#   path = "Files/incoming/orders.csv",
#   source = "orders.csv"
# )

## ----eval = FALSE-------------------------------------------------------------
# warehouse <- workspace$warehouses()[[1L]]
# 
# warehouse_result <- warehouse$write_table(
#   table = "orders_from_r",
#   data = orders,
#   staging_lakehouse = lakehouse,
#   schema = "dbo",
#   create_if_missing = TRUE,
#   mode = "Append"
# )

## ----eval = FALSE-------------------------------------------------------------
# kql_database <- workspace$kql_databases()[[1L]]
# 
# kql_result <- kql_database$write_table(
#   table = "OrdersFromR",
#   data = orders,
#   create_if_missing = TRUE
# )
# 
# kql_result$status$state

## ----eval = FALSE-------------------------------------------------------------
# operation <- lakehouse$load_table(
#   table = "orders_from_file",
#   path = "Files/incoming/orders.csv",
#   format = "Csv",
#   header = TRUE,
#   mode = "Overwrite"
# )
# 
# completed <- fabric_operation_wait(operation, timeout = 900)

## ----eval = FALSE-------------------------------------------------------------
# dataset <- arrow::open_dataset("local-parquet-directory")
# 
# lakehouse$write_table(
#   table = "large_orders",
#   data = dataset
# )

