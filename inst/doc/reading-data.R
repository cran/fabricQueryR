## ----include = FALSE----------------------------------------------------------
knitr::opts_chunk$set(collapse = TRUE, comment = "#>", eval = FALSE)

## ----eval = FALSE-------------------------------------------------------------
# library(fabricQueryR)
# 
# workspaces <- fabric_workspaces()
# matches <- Filter(
#   \(x) identical(x$displayName, "Analytics workspace"),
#   workspaces
# )
# stopifnot(length(matches) == 1L)
# workspace <- matches[[1L]]
# lakehouse <- workspace$lakehouses()[[1L]]
# warehouse <- workspace$warehouses()[[1L]]
# kql_database <- workspace$kql_databases()[[1L]]
# model <- workspace$semantic_models()[[1L]]

## ----eval = FALSE-------------------------------------------------------------
# recent_orders <- warehouse$sql_query(
#   sql = paste(
#     "SELECT TOP 100 order_id, order_date, amount",
#     "FROM dbo.orders",
#     "WHERE order_date >= ?",
#     "ORDER BY order_date DESC"
#   ),
#   params = list(as.Date("2026-01-01"))
# )
# 
# head(recent_orders)

## ----eval = FALSE-------------------------------------------------------------
# orders <- local({
#   con <- warehouse$sql_connect()
#   on.exit(DBI::dbDisconnect(con), add = TRUE)
#   DBI::dbListTables(con)
#   DBI::dbGetQuery(con, "SELECT TOP 100 * FROM dbo.orders")
# })

## ----eval = FALSE-------------------------------------------------------------
# lakehouse_rows <- lakehouse$read_table(
#   table = "orders",
#   columns = c("order_id", "order_date", "amount"),
#   limit = 100L
# )
# 
# warehouse_rows <- warehouse$read_table(
#   table = "orders",
#   schema = "dbo",
#   limit = 100L
# )

## ----eval = FALSE-------------------------------------------------------------
# Sys.setenv(RETICULATE_PYTHON = "managed")
# library(fabricQueryR)
# fabric_delta_config(initialize = TRUE)

## ----eval = FALSE-------------------------------------------------------------
# events <- kql_database$read_table(
#   table = "Events",
#   limit = 100L
# )

## ----eval = FALSE-------------------------------------------------------------
# daily_events <- kql_database$query(
#   query = paste(
#     "Events",
#     "| where observed_at >= ago(7d)",
#     "| summarize event_count = count() by bin(observed_at, 1d)",
#     "| order by observed_at asc"
#   )
# )

## ----eval = FALSE-------------------------------------------------------------
# sales_by_region <- model$dax_query(
#   dax = paste(
#     "EVALUATE",
#     "SUMMARIZECOLUMNS(",
#     "  'Region'[Region],",
#     "  \"Total Sales\", [Total Sales]",
#     ")"
#   )
# )

## ----eval = FALSE-------------------------------------------------------------
# orders_file <- lakehouse$onelake_read_file(
#   path = "Files/exports/orders.parquet"
# )

## ----eval = FALSE-------------------------------------------------------------
# api <- workspace$graphql_apis()[[1L]]
# 
# response <- api$query(
#   query = "{ products { items { id name category } } }"
# )
# products <- response$data$products$items

## ----eval = FALSE-------------------------------------------------------------
# result <- lakehouse$livy_query(
#   kind = "sql",
#   code = "SELECT category, count(*) AS n FROM orders GROUP BY category"
# )
# counts <- result$output$parsed

## ----eval = FALSE-------------------------------------------------------------
# row_count <- local({
#   stream <- lakehouse$read_table(
#     table = "large_orders",
#     result = "arrow_stream"
#   )
#   on.exit(nanoarrow::nanoarrow_pointer_release(stream), add = TRUE)
#   reader <- arrow::as_record_batch_reader(stream)
#   on.exit(reader$Close(), add = TRUE, after = FALSE)
#   count <- 0
#   repeat {
#     batch <- reader$read_next_batch()
#     if (is.null(batch)) break
#     # Process or write this batch before reading the next one.
#     count <- count + batch$num_rows
#   }
#   count
# })

