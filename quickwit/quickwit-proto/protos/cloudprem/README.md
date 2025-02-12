file in this directory are shared with other ddog project

- cloudprem.proto:
    owned by cloudprem, used for communication between event platform and cloudprem
    url: https://github.com/DataDog/dd-source/blob/main/domains/event-platform/shared/libs/cloudprem-proto/cloudprem.proto

- queryparser.proto:
    owned by event platform, used for the query AST
    url: https://github.com/DataDog/logs-backend/blob/prod/libs/grpc/queryparser-proto/src/main/resources/queryparser.proto

- calc_fields.proto:
    owned by event platform, used for expression AST inside aggregations
    url: https://github.com/DataDog/logs-backend/blob/prod/libs/grpc/exprparser-proto/src/main/resources/calc_fields.proto

eventually, we should investigate a simple way to automate keeping them in sync (submodule?)
