## ----include = FALSE----------------------------------------------------------
knitr::opts_chunk$set(collapse = TRUE, comment = "#>", eval = FALSE)

## ----eval = FALSE-------------------------------------------------------------
# library(fabricQueryR)
# 
# model <- fabric_semantic_models("Analytics workspace")[[1]]

## ----eval = FALSE-------------------------------------------------------------
# rows <- model$dax_query(
#   dax = "EVALUATE TOPN(100, 'Customers')"
# )
# 
# head(rows)

## ----eval = FALSE-------------------------------------------------------------
# sales <- model$dax_query(
#   dax = paste(
#     "EVALUATE",
#     "SUMMARIZECOLUMNS(",
#     "  'Region'[Region],",
#     "  \"Total Sales\", [Total Sales]",
#     ")"
#   )
# )

## ----eval = FALSE-------------------------------------------------------------
# refresh <- model$refresh()
# completed <- model$refresh_wait(refresh, timeout = 1800)
# 
# completed$state
# completed$start_time
# completed$end_time

## ----eval = FALSE-------------------------------------------------------------
# load <- lakehouse$write_table(
#   table = "Sales",
#   data = new_sales,
#   schema = "dbo",
#   mode = "overwrite"
# )
# 
# refresh <- model$refresh(mode = "enhanced", type = "Full")
# completed <- model$refresh_wait(
#   refresh,
#   timeout = 1800,
#   cancel_on_timeout = TRUE
# )

## ----eval = FALSE-------------------------------------------------------------
# refresh <- model$refresh(
#   mode = "enhanced",
#   type = "Full",
#   objects = list(
#     list(table = "Sales", partition = "2026"),
#     list(table = "Calendar")
#   ),
#   commit_mode = "Transactional",
#   max_parallelism = 4L,
#   retry_count = 1L,
#   timeout = "02:00:00"
# )
# 
# completed <- model$refresh_wait(refresh, timeout = 5 * 60 * 60)

## ----eval = FALSE-------------------------------------------------------------
# status <- model$refresh_status(refresh)
# 
# status$state
# status$attempts
# status$messages
# status$service_error
# status$objects
# status$details_url

## ----eval = FALSE-------------------------------------------------------------
# result <- model$refresh_wait(
#   refresh,
#   error_on_failure = FALSE
# )
# 
# if (result$state != "Completed") result$details_url

## ----eval = FALSE-------------------------------------------------------------
# history <- model$refresh_history(top = 10L)
# 
# history[[1]]$refresh_type
# history[[1]]$state
# history[[1]]$attempts
# 
# # Refresh an old history entry from its request ID and stored model context
# latest <- model$refresh_status(history[[1]])

