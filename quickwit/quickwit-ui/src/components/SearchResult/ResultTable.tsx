// Copyright (C) 2022 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

import { Table, TableBody, TableContainer, Box, styled } from "@mui/material";
import { Field as Field, getAllFields, Index, SearchResponse} from "../../utils/models";
import { Row } from "./Row";

const TableBox = styled(Box)`
display: flex;
flex-direction: column;
overflow: auto;
flex: 1 1 100%;
height: 100%;
`

export function ResultTable({searchResponse, index}: {searchResponse: SearchResponse, index: Index}) {
  const timestampField = getTimestampField(index);
  return (
    <TableBox>
      <TableContainer>
        <Table size="small" >
          <TableBody>
            { searchResponse.hits.map((hit, idx) =>
                <Row
                  key={idx}
                  row={hit}
                  timestampField={timestampField}
                />
            )}
          </TableBody>
        </Table>
      </TableContainer>
    </TableBox>
  );
}

function getTimestampField(index: Index): Field | null {
  const fields = getAllFields(index.metadata.index_config.doc_mapping.field_mappings);
  const timestamp_field_name = index.metadata.index_config.doc_mapping.timestamp_field;
  const timestamp_field = fields.filter(field => field.field_mapping.name === timestamp_field_name)[0];
  return timestamp_field ?? null;
}
