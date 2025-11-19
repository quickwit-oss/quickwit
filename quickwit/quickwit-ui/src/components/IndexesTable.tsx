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

import {
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
} from "@mui/material";
import dayjs from "dayjs";
import utc from "dayjs/plugin/utc";
import { useNavigate } from "react-router";
import { IndexMetadata } from "../utils/models";

dayjs.extend(utc);

const IndexesTable = ({
  indexesMetadata,
}: Readonly<{ indexesMetadata: IndexMetadata[] }>) => {
  const navigate = useNavigate();
  const handleClick = (indexId: string) => {
    navigate(`/indexes/${indexId}`);
  };

  return (
    <TableContainer component={Paper}>
      <Table sx={{ minWidth: 650 }} aria-label="Indexes">
        <TableHead>
          <TableRow>
            <TableCell align="left">ID</TableCell>
            <TableCell align="left">URI</TableCell>
            <TableCell align="left">Created on</TableCell>
            <TableCell align="left">Sources</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {indexesMetadata.map((indexMetadata) => (
            <TableRow
              key={indexMetadata.index_config.index_id}
              sx={{
                "&:last-child td, &:last-child th": { border: 0 },
                cursor: "pointer",
              }}
              hover={true}
              onClick={() => handleClick(indexMetadata.index_config.index_id)}
            >
              <TableCell component="th" scope="row">
                {indexMetadata.index_config.index_id}
              </TableCell>
              <TableCell align="left">
                {indexMetadata.index_config.index_uri}
              </TableCell>
              <TableCell align="left">
                {dayjs
                  .unix(indexMetadata.create_timestamp)
                  .utc()
                  .format("YYYY/MM/DD HH:mm")}
              </TableCell>
              <TableCell align="left">
                {indexMetadata.sources?.length || "None"}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </TableContainer>
  );
};

export default IndexesTable;
