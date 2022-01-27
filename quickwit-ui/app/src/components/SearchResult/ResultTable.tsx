// Copyright (C) 2021 Quickwit, Inc.
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

import { Paper, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Box } from "@mui/material";
import { IndexMetadata, SearchResponse } from "../../utils/models";
import { Row } from "./Row";

export function ResultTable({searchResponse, indexMetadata}: {searchResponse: SearchResponse, indexMetadata: IndexMetadata}) {
  return (
    <Box sx={{display: 'flex', flexDirection: 'column', overflow: 'auto', flex: '1 1 100%', height: '100%'}}>
      <TableContainer component={Paper} sx={{ overflow: "initial" }}>
        <Table size="small" sx={{width: '100%', maxWidth: '100%', border: 'none', borderCollapse: 'collapse', borderSpacing: 0 }}>
          <TableHead>
            <TableRow>
            <TableCell sx={{ width: '24px' }} />
            <TableCell>Document</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            { searchResponse.hits.map((hit, index) =>
                <Row key={index} row={hit} timestampField={indexMetadata.indexing_settings.timestamp_field} />
            )}
          </TableBody>
        </Table>
      </TableContainer> 
    </Box>
  );
}