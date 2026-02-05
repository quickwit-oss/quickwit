// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import { openapiSchemaToJsonSchema } from "@openapi-contrib/openapi-schema-to-json-schema";
import React from "react";

/**
 * return the json schema for the given component
 * based on the openapi schema at /openapi.json
 *
 * @param ref is a path to the component, usually starting with #/components/schemas/...
 */
export const useJsonSchema = (ref: string) => {
  const [openApiSchema, setOpenApiSchema] = React.useState<any>(null);

  console.log(openApiSchema);

  React.useEffect(() => {
    schemaPromise = schemaPromise || fetchOpenApiSchema();
    schemaPromise.then(setOpenApiSchema);
  }, []);

  const jsonShema = React.useMemo(() => {
    if (!openApiSchema) return null;
    return openapiSchemaToJsonSchema({
      ...openApiSchema,
      $ref: ref,
    });
  }, [openApiSchema, ref]);

  return jsonShema;
};

let schemaPromise: Promise<any> | null = null;
export const fetchOpenApiSchema = async () => {
  const response = await fetch("/openapi.json");
  if (!response.ok) {
    throw new Error(`Failed to fetch OpenAPI schema: ${response.statusText}`);
  }
  return await response.json();
};
