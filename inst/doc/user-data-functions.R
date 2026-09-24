## ----include = FALSE----------------------------------------------------------
knitr::opts_chunk$set(collapse = TRUE, comment = "#>", eval = FALSE)

## ----eval = FALSE-------------------------------------------------------------
# function_url <- Sys.getenv("FABRIC_FUNCTION_URL")

## ----eval = FALSE-------------------------------------------------------------
# result <- fabric_function_invoke(
#   function_url,
#   parameters = list(customerName = "Ada", priority = 2L),
#   audience = paste0(
#     "https://analysis.windows.net/powerbi/api/",
#     "Item.Execute.All"
#   )
# )

## ----eval = FALSE-------------------------------------------------------------
# result <- fabric_function_invoke(
#   function_url,
#   parameters = list(
#     customerName = "Ada",
#     priority = 2L
#   )
# )
# 
# result$status
# result$output

## ----eval = FALSE-------------------------------------------------------------
# structured_result <- fabric_function_invoke(
#   function_url,
#   parameters = list(
#     customerName = "Ada",
#     priority = 2L,
#     lineIds = I(c(101L, 102L)),
#     metadata = list(source = "R", approved = TRUE, note = NULL)
#   )
# )

## ----eval = FALSE-------------------------------------------------------------
# structured_result$function_name
# structured_result$invocation_id
# structured_result$status
# structured_result$output
# structured_result$errors
# structured_result$http_status

## ----eval = FALSE-------------------------------------------------------------
# result <- fabric_function_invoke(
#   function_url,
#   parameters = list(requestId = "stable-business-key"),
#   idempotent = TRUE
# )

