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

import { Box, styled, Table, TableBody, TableContainer } from "@mui/material";
import { Field, getAllFields, Index, SearchResponse } from "../../utils/models";
import { Row } from "./Row";

const TableBox = styled(Box)`
display: flex;
flex-direction: column;
overflow: auto;
flex: 1 1 100%;
height: 100%;
`;

export function ResultTable({
  searchResponse,
  index,
}: {
  searchResponse: SearchResponse;
  index: Index;
}) {
  const timestampField = getTimestampField(index);
  return (
    <TableBox>
      <TableContainer>
        <Table size="small">
          <TableBody>
            {searchResponse.hits.map((hit, idx) => (
              <Row key={idx} row={hit} timestampField={timestampField} />
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </TableBox>
  );
}

function getTimestampField(index: Index): Field | null {
  const fields = getAllFields(
    index.metadata.index_config.doc_mapping.field_mappings,
  );
  const timestamp_field_name =
    index.metadata.index_config.doc_mapping.timestamp_field;
  const timestamp_field = fields.filter(
    (field) => field.field_mapping.name === timestamp_field_name,
  )[0];
  return timestamp_field ?? null;
}
