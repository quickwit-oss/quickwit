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

import { KeyboardArrowDown } from "@mui/icons-material";
import ChevronRight from "@mui/icons-material/ChevronRight";
import { Box, IconButton, TableCell, TableRow } from "@mui/material";
import { styled } from "@mui/system";
import { useState } from "react";

interface RowProps {
  timestampField: null | string;
  row: any;
}

const BreakWordBox = styled(Box)({
  verticalAlign: 'top',
  display: 'inline-block',
  color: '#464646',
  wordBreak: 'break-all',
  wordWrap: 'break-word',
  whiteSpace: 'pre-wrap',
  margin: 1,
  overflow: 'hidden',
});

export function Row(props: RowProps) {
  const [open, setOpen] = useState(false);
  return (
    <>
      <TableRow>
        <TableCell sx={{ px: 0, py: 0, verticalAlign: 'top' }}>
          <IconButton
            aria-label="expand row"
            size="small"
            onClick={() => setOpen(!open)}
          >
            {open ? <KeyboardArrowDown /> : <ChevronRight />}
          </IconButton>
        </TableCell>
        <TableCell sx={{ m: 1 }}>
          <BreakWordBox sx={{ maxHeight: '100px' }}>
            {JSON.stringify(props.row, null, 0)}
          </BreakWordBox>
        </TableCell>
      </TableRow>
      <TableRow sx={{ margin: 0, padding: 0 }}>
        {open && <>
            <TableCell />
            <TableCell sx={{ m: 1 }}>
                <BreakWordBox>
                  {JSON.stringify(props.row, null, 2)}
                </BreakWordBox>
            </TableCell>
          </>
        }
      </TableRow>
    </>
  );
}