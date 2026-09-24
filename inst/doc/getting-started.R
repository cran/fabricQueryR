## ----include = FALSE----------------------------------------------------------
knitr::opts_chunk$set(collapse = TRUE, comment = "#>", eval = FALSE)

## ----eval = FALSE-------------------------------------------------------------
# install.packages("fabricQueryR")
# 
# library(fabricQueryR)
# Sys.setenv(FABRICQUERYR_TENANT_ID = "<your-tenant-id>")

## ----eval = FALSE-------------------------------------------------------------
# # Run this only when your administrator supplies a client ID:
# Sys.setenv(FABRICQUERYR_CLIENT_ID = "<your-app-client-id>")

## ----tutorial-test-list-workspaces, eval = FALSE------------------------------
# # List all workspaces you can access
# workspaces <- fabric_workspaces()

## ----tutorial-test-select-first, eval = FALSE---------------------------------
# # Select the first workspace in the list
# workspace <- workspaces[[1L]]
# workspace$displayName

## ----tutorial-test-select-by-name, eval = FALSE-------------------------------
# # Select a workspace by name
# workspaces <- fabric_workspaces()
# matches <- Filter(
#   \(x) identical(x$displayName, "Analytics workspace"),
#   workspaces
# )
# stopifnot(length(matches) == 1L)
# workspace <- matches[[1L]]

## ----tutorial-test-list-items, eval = FALSE-----------------------------------
# # List all items in the workspace
# items <- workspace$items()
# items
# 
# # The generic interface also filters types without a typed convenience method
# reports <- workspace$items(type = "Report")
# 
# # List only Lakehouses in the workspace
# lakehouses <- workspace$lakehouses()
# lakehouse <- lakehouses[[1L]]
# lakehouse$displayName

## ----tutorial-test-read-lakehouse, eval = FALSE-------------------------------
# # List the tables in the Lakehouse
# tables <- lakehouse$tables()
# tables[c("schema", "name", "type")]
# 
# # Select the first table and read a small number of rows
# first_table <- tables[1L, ]
# rows <- lakehouse$read_table(
#   first_table,
#   limit = 100L
# )
# 
# # Show the first few rows
# head(rows)

