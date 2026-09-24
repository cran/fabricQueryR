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

## ----eval = FALSE-------------------------------------------------------------
# livy_scopes <- paste0(
#   paste0("https", "://api.fabric.microsoft.com/"),
#   c(
#     "Lakehouse.Execute.All",
#     "Lakehouse.Read.All",
#     "Code.AccessFabric.All",
#     "Code.AccessStorage.All",
#     "Code.AccessSQL.All"
#   )
# )
# 
# # `$livy_query()` is the object interface to `fabric_livy_query()`
# result <- lakehouse$livy_query(
#   code = "SELECT * FROM external_sql_table",
#   kind = "sql",
#   audience = livy_scopes
# )

## ----eval = FALSE-------------------------------------------------------------
# result <- lakehouse$livy_query(
#   kind = "sql",
#   code = "SELECT 1 AS id, 'hello from Spark' AS message"
# )
# 
# result$output$parsed

## ----eval = FALSE-------------------------------------------------------------
# result <- lakehouse$livy_query(
#   kind = "sparkr",
#   code = paste(
#     "df <- sql('SELECT * FROM orders LIMIT 100')",
#     "printSchema(df)",
#     "showDF(df, numRows = 10)",
#     sep = "\n"
#   )
# )

## ----eval = FALSE-------------------------------------------------------------
# result <- lakehouse$livy_query(
#   kind = "sparkr",
#   code = paste(
#     "library(sparklyr)",
#     "spark_version <- sparkR.version()",
#     "config <- spark_config()",
#     paste0(
#       "sc <- spark_connect(master = 'yarn', version = spark_version, ",
#       "spark_home = '/opt/spark', method = 'synapse', config = config)"
#     ),
#     "orders <- dplyr::tbl(sc, 'orders')",
#     "print(dplyr::collect(head(orders, 10)))",
#     "spark_disconnect(sc)",
#     sep = "\n"
#   )
# )

## ----eval = FALSE-------------------------------------------------------------
# answer <- local({
#   session <- lakehouse$livy_session()
#   on.exit(session$close(), add = TRUE)
# 
#   session$wait()
#   session$run("shared_value = 40", kind = "pyspark")
#   answer <- session$run("print(shared_value + 2)", kind = "pyspark")
#   answer
# })
# answer$output$parsed

## ----eval = FALSE-------------------------------------------------------------
# batch <- lakehouse$livy_batch_submit(
#   file = paste0(
#     "abfss://", workspace$id,
#     "@onelake.dfs.fabric.microsoft.com/",
#     lakehouse$id,
#     "/Files/jobs/daily_transform.py"
#   ),
#   name = "daily-transform",
#   wait = TRUE,
#   timeout = 1800
# )
# 
# batch$result()

## ----eval = FALSE-------------------------------------------------------------
# environment <- workspace$environments()[[1L]]
# 
# result <- lakehouse$livy_query(
#   kind = "pyspark",
#   code = "print(spark.version)",
#   environment_id = environment$id
# )

