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

import { Paper, Table, TableBody, TableCell, TableContainer, TableHead, TableRow } from "@mui/material";
import dayjs from "dayjs";
import { IndexMetadata } from "../utils/models";
import { useNavigate } from "react-router-dom";

const IndexesTable = ({ indexesMetadata }: Readonly<{indexesMetadata: IndexMetadata[]}>) => {
  const navigate = useNavigate();
  const handleClick = function(indexId: string) {
    navigate(`/indexes/${indexId}`);
  }

  return (
    <TableContainer component={Paper}>
      <Table sx={{ minWidth: 650 }} aria-label="Indexes">
        <TableHead>
          <TableRow>
            <TableCell align="left">ID</TableCell>
            <TableCell align="left">URI</TableCell>
            <TableCell align="left">Created on</TableCell>
            <TableCell align="left">Updated on</TableCell>
            <TableCell align="left">Sources</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {indexesMetadata.map((indexMetadata) => (
            <TableRow
              key={indexMetadata.index_id}
              sx={{ '&:last-child td, &:last-child th': { border: 0 }, cursor: "pointer"}}
              hover={true}
              onClick={() => handleClick(indexMetadata.index_id)}
            >
              <TableCell component="th" scope="row">
                {indexMetadata.index_id}
              </TableCell>
              <TableCell align="left">{indexMetadata.index_uri}</TableCell>
              <TableCell align="left">{ dayjs.unix(indexMetadata.create_timestamp).format("YYYY/MM/DD HH:MM") }</TableCell>
              <TableCell align="left">{ dayjs.unix(indexMetadata.update_timestamp).format("YYYY/MM/DD HH:MM") }</TableCell>
              <TableCell align="left">{ indexMetadata.sources?.length || 'None'}</TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </TableContainer>
  );
};

export default IndexesTable;