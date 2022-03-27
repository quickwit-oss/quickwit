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

import { List, ListItem, Paper } from "@mui/material";
import { styled } from "@mui/system";
import { Box } from "@mui/system";
import dayjs from "dayjs";
import NumberFormat from "react-number-format";
import { IndexMetadata } from "../utils/models";

const InlineBox = styled(Box)`
display: inline;
padding-right: 10px;
`

const CustomListItem = styled(ListItem)`
width: inherit;
`

export function IndexSummary({indexMetadata}: {indexMetadata: IndexMetadata}) {
  return (
    <Paper variant="outlined" >
      <List sx={{ display: 'flex', flexDirection: 'row', 'padding': 0 }}>
        <CustomListItem>
          <InlineBox>Created at:</InlineBox>
          <InlineBox>
            { dayjs.unix(indexMetadata.create_timestamp).format("YYYY/MM/DD HH:MM") }
          </InlineBox>
        </CustomListItem>
        <CustomListItem>
          <InlineBox>URI:</InlineBox>
          <InlineBox>
            { indexMetadata.index_uri }
          </InlineBox>
        </CustomListItem>
        <CustomListItem>
          <InlineBox>Size:</InlineBox>
          <InlineBox>
            { indexMetadata.num_bytes / 1000000 } MB 
          </InlineBox>
        </CustomListItem>
        <CustomListItem>
          <InlineBox>Documents:</InlineBox>
          <InlineBox>
            <NumberFormat value={indexMetadata.num_docs} displayType={'text'} thousandSeparator={true} />
          </InlineBox>
        </CustomListItem>
        <CustomListItem>
          <InlineBox>Splits:</InlineBox>
          <InlineBox>
            { indexMetadata.num_splits }
          </InlineBox>
        </CustomListItem>
      </List>
      <List sx={{ display: 'flex', flexDirection: 'row', 'padding': 0 }}>
        <CustomListItem>
          <InlineBox>Updated at:</InlineBox>
          <InlineBox>
            { dayjs.unix(indexMetadata.update_timestamp).format("YYYY/MM/DD HH:MM") }
          </InlineBox>
        </CustomListItem>
      </List>
    </Paper>
  )
}