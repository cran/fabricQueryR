## ----include = FALSE----------------------------------------------------------
knitr::opts_chunk$set(collapse = TRUE, comment = "#>")

## ----eval = FALSE-------------------------------------------------------------
# Sys.setenv(FABRICQUERYR_TENANT_ID = "<your-tenant-id>")
# 
# library(fabricQueryR)

## ----eval = FALSE-------------------------------------------------------------
# Sys.setenv(FABRICQUERYR_CLIENT_ID = "<your-client-id>")

## ----eval = FALSE-------------------------------------------------------------
# workspaces <- fabric_workspaces()
# 
# workspaces
# workspace <- workspaces[[1L]]
# workspace$displayName
# workspace$id

## ----eval = FALSE-------------------------------------------------------------
# items <- workspace$items()
# items
# item <- items[[1L]]
# item$displayName
# item$type
# item$id

## ----eval = FALSE-------------------------------------------------------------
# file.edit("~/.Renviron")

## ----eval = FALSE-------------------------------------------------------------
# Sys.getenv("FABRICQUERYR_TENANT_ID")
# Sys.getenv("FABRICQUERYR_CLIENT_ID")

## ----eval = FALSE-------------------------------------------------------------
# workspaces <- fabric_workspaces(
#   auth_args = list(use_cache = FALSE)
# )

## ----eval = FALSE-------------------------------------------------------------
# workspaces <- fabric_workspaces(
#   auth_args = list(auth_type = "device_code")
# )

## ----eval = FALSE-------------------------------------------------------------
# workspaces <- fabric_workspaces(
#   tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
#   client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID"),
#   auth_args = list(
#     password = Sys.getenv("FABRIC_CLIENT_SECRET"),
#     auth_type = "client_credentials"
#   )
# )

