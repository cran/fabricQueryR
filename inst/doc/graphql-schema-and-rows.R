## ----include = FALSE----------------------------------------------------------
knitr::opts_chunk$set(collapse = TRUE, comment = "#>", eval = FALSE)

## ----eval = FALSE-------------------------------------------------------------
# library(fabricQueryR)
# 
# api <- fabric_graphql_apis("Analytics workspace")[[1]]

## ----eval = FALSE-------------------------------------------------------------
# response <- api$query(
#   query = "{ products { items { id name } } }"
# )
# 
# response$data$products$items
# response$errors

## ----eval = FALSE-------------------------------------------------------------
# schema <- api$schema()
# 
# schema$queryType$name
# vapply(schema$types, `[[`, character(1), "name")

## ----eval = FALSE-------------------------------------------------------------
# pages <- api$paginate(
#   query = paste(
#     "query Products($first: Int!, $after: String) {",
#     "  products(first: $first, after: $after, orderBy: {id: ASC}) {",
#     "    items {",
#     "      id",
#     "      name",
#     "      category { id name }",
#     "      tags",
#     "    }",
#     "    hasNextPage",
#     "    endCursor",
#     "  }",
#     "}"
#   ),
#   variables = list(first = 100L, after = NULL),
#   operation_name = "Products",
#   next_cursor = fabric_graphql_cursor("products"),
#   idempotent = TRUE
# )

## ----eval = FALSE-------------------------------------------------------------
# products <- fabric_graphql_collect(pages, c("products", "items"))
# 
# products
# attr(products, "complete")
# attr(products, "page_count")
# attr(products, "errors")

## ----eval = FALSE-------------------------------------------------------------
# products$category[[1]]
# products$tags[[1]]

## ----eval = FALSE-------------------------------------------------------------
# incomplete_pages <- tryCatch(
#   api$paginate(
#     query = paste(
#       "query Products($first: Int!, $after: String) {",
#       "  products(first: $first, after: $after, orderBy: {id: ASC}) {",
#       "    items { id name } hasNextPage endCursor",
#       "  }",
#       "}"
#     ),
#     variables = list(first = 100L, after = NULL),
#     operation_name = "Products",
#     next_cursor = fabric_graphql_cursor("products"),
#     max_pages = 1L,
#     idempotent = TRUE
#   ),
#   fabric_graphql_pagination_error = function(error) error$pages
# )
# 
# tryCatch(
#   fabric_graphql_collect(incomplete_pages, c("products", "items")),
#   fabric_graphql_collection_error = function(error) {
#     partial <- error$partial_data
#     attr(partial, "complete")
#     partial
#   }
# )

