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
import React, { useState } from "react";
import { DocMapping, Entry, flattenEntries, RawDoc } from "../../utils/models";
import { QUICKWIT_GREY, QUICKWIT_INTERMEDIATE_GREY } from "../../utils/theme";
import { JsonEditor } from "../JsonEditor";

interface RowProps {
  timestampField: null | string;
  row: RawDoc;
  doc_mapping: DocMapping;
}

const EntryName = styled('dt')`
display: inline;
background-color: ${QUICKWIT_INTERMEDIATE_GREY};
color: #343741;
padding: 2px 1px 2px 4px;
margin-right: 4px;
word-break: normal;
border-radius: 3px;
`;

const EntryValue = styled('dd')`
display: inline;
margin: 0;
padding: 0;
margin-inline-end: 5px;
`;

function EntryFormatter(entry: Entry) {
  return (
    <>
      <EntryName>{entry.key}:</EntryName>
      <EntryValue>{entry.value}</EntryValue>
    </>
  )
}

const BreakWordBox = styled('dl')({
  verticalAlign: 'top',
  display: 'inline-block',
  color: '#464646',
  wordBreak: 'break-all',
  wordWrap: 'break-word',
  margin: 1,
  overflow: 'hidden',
  lineHeight: '1.8em',
});

export function Row(props: RowProps) {
  const [open, setOpen] = useState(false);
  const flatten_entries = flattenEntries(props.doc_mapping, props.row);
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
        <TableCell>
          {!open && <BreakWordBox sx={{ maxHeight: '100px' }}>
              { flatten_entries.map((entry) => <React.Fragment key={entry.key}>{EntryFormatter(entry)}</React.Fragment>) }
            </BreakWordBox>
          }
          {open && 
              <JsonEditor content={props.row} resizeOnMount={true} />
          }
        </TableCell>
      </TableRow>
    </>
  );
}