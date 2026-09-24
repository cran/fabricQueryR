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
# 
# lakehouse$displayName

## ----eval = FALSE-------------------------------------------------------------
# orders <- lakehouse$sql_query(
#   "SELECT TOP 10 * FROM dbo.orders"
# )

## ----eval = FALSE-------------------------------------------------------------
# con <- lakehouse$sql_connect()
# DBI::dbListTables(con)
# DBI::dbGetQuery(con, "SELECT TOP 10 * FROM dbo.orders")
# DBI::dbDisconnect(con)

## ----eval = FALSE-------------------------------------------------------------
# files <- lakehouse$onelake_list(
#   path = "Files/incoming"
# )
# 
# files[c("path", "is_directory", "content_length")]

## ----eval = FALSE-------------------------------------------------------------
# orders <- lakehouse$onelake_read_file(
#   path = "Files/incoming/orders.csv"
# )
# 
# head(orders)

## ----eval = FALSE-------------------------------------------------------------
# lakehouse$onelake_write_file(
#   path = "Files/exports/orders.parquet",
#   data = data.frame(
#     order_id = 1:3,
#     amount = c(10.5, 20, 30.25)
#   )
# )

## ----eval = FALSE-------------------------------------------------------------
# lakehouse$onelake_upload(
#   path = "Files/incoming/logo.png",
#   source = "logo.png"
# )

## ----eval = FALSE-------------------------------------------------------------
# tables <- lakehouse$tables()
# tables[c("schema", "name", "type", "format")]

## ----eval = FALSE-------------------------------------------------------------
# table <- tables[1L, ]
# 
# rows <- lakehouse$read_table(
#   table,
#   columns = c("order_id", "amount"),
#   limit = 100L
# )

## ----eval = FALSE-------------------------------------------------------------
# result <- lakehouse$write_table(
#   table = "orders_from_r",
#   data = data.frame(
#     order_id = 1:3,
#     amount = c(10.5, 20, 30.25)
#   ),
#   mode = "Overwrite"
# )
# 
# result$rows
# result$staging_retained

## ----eval = FALSE-------------------------------------------------------------
# operation <- lakehouse$load_table(
#   table = "orders_from_csv",
#   path = "Files/incoming/orders.csv",
#   format = "Csv",
#   header = TRUE,
#   delimiter = ",",
#   mode = "Overwrite"
# )
# 
# operation <- fabric_operation_wait(operation, timeout = 900)

## ----eval = FALSE-------------------------------------------------------------
# local({
#   stream <- lakehouse$read_table(
#     table = "large_orders",
#     result = "arrow_stream"
#   )
#   on.exit(nanoarrow::nanoarrow_pointer_release(stream), add = TRUE)
#   reader <- arrow::as_record_batch_reader(stream)
#   on.exit(reader$Close(), add = TRUE, after = FALSE)
#   row_count <- 0
#   repeat {
#     batch <- reader$read_next_batch()
#     if (is.null(batch)) break
#     # Process or write this batch before reading the next one.
#     row_count <- row_count + batch$num_rows
#   }
#   row_count
# })

## ----eval = FALSE-------------------------------------------------------------
# older_rows <- lakehouse$read_table(
#   table = "orders",
#   version = 42L,
#   limit = 100L
# )

