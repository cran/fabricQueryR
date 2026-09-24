## ----include = FALSE----------------------------------------------------------
knitr::opts_chunk$set(collapse = TRUE, comment = "#>", eval = FALSE)

## ----eval = FALSE-------------------------------------------------------------
# library(fabricQueryR)
# 
# workspaces <- fabric_workspaces()
# matches <- Filter(\(x) identical(x$displayName, "Analytics"), workspaces)
# stopifnot(length(matches) == 1L)
# workspace <- matches[[1L]]
# warehouse <- workspace$warehouses()[[1L]]

## ----eval = FALSE-------------------------------------------------------------
# orders <- warehouse$sql_query(
#   "SELECT TOP 10 * FROM dbo.orders"
# )

## ----eval = FALSE-------------------------------------------------------------
# con <- warehouse$sql_connect()
# DBI::dbListTables(con)
# DBI::dbGetQuery(con, "SELECT TOP 10 * FROM dbo.orders")
# DBI::dbDisconnect(con)

## ----eval = FALSE-------------------------------------------------------------
# staging_lakehouse <- workspace$lakehouses()[[1L]]

## ----eval = FALSE-------------------------------------------------------------
# written <- warehouse$write_table(
#   table = "orders",
#   data = data.frame(
#     id = 1:3,
#     label = c("alpha", "beta", "gamma"),
#     amount = c(10.5, NA, 30)
#   ),
#   staging_lakehouse = staging_lakehouse,
#   schema = "dbo",
#   mode = "Append"
# )
# 
# written$rows
# written$file_count
# written$staging_retained

## ----eval = FALSE-------------------------------------------------------------
# created <- warehouse$write_table(
#   table = "orders_from_r",
#   data = orders,
#   staging_lakehouse = staging_lakehouse,
#   create_if_missing = TRUE
# )

## ----eval = FALSE-------------------------------------------------------------
# replacement <- data.frame(
#   id = 4:6,
#   label = c("delta", "epsilon", "zeta"),
#   amount = c(40, 50, 60)
# )
# 
# replaced <- warehouse$write_table(
#   table = "orders",
#   data = replacement,
#   staging_lakehouse = staging_lakehouse,
#   mode = "Overwrite",
#   overwrite_method = "Truncate"
# )

## ----eval = FALSE-------------------------------------------------------------
# recreated <- warehouse$write_table(
#   table = "orders",
#   data = replacement,
#   staging_lakehouse = staging_lakehouse,
#   mode = "Overwrite",
#   overwrite_method = "Drop",
#   create_if_missing = TRUE
# )

## ----eval = FALSE-------------------------------------------------------------
# dataset <- arrow::open_dataset("local-parquet-directory")
# 
# written <- warehouse$write_table(
#   table = "orders",
#   data = dataset,
#   staging_lakehouse = staging_lakehouse
# )

