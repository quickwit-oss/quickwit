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

import styled from "@emotion/styled";
import { Paper } from "@mui/material";
import dayjs from "dayjs";
import utc from "dayjs/plugin/utc"
import NumberFormat from "react-number-format";
import { Index } from "../utils/models";
dayjs.extend(utc);

const ItemContainer = styled.div`
padding: 10px;
display: flex;
flex-direction: column;
`
const Row = styled.div`
padding: 5px;
display: flex;
flex-direction: row;
`
const RowKey = styled.div`
width: 100px;
`

export function IndexSummary({index}: {index: Index}) {
  const all_splits = index.splits;
  const published_splits = all_splits.filter(split => split.split_state == "Published");
  const num_of_staged_splits = all_splits.filter(split => split.split_state == "Staged").length;
  const num_of_marked_for_delete_splits = all_splits.filter(split => split.split_state == "MarkedForDeletion").length;
  const total_num_docs = published_splits
    .map(split => split.num_docs)
    .reduce((sum, current) => sum + current, 0);
  const total_num_bytes = published_splits
    .map(split => {
      return split.footer_offsets.end 
    })
    .reduce((sum, current) => sum + current, 0);
  return (
    <Paper variant="outlined" >
      <ItemContainer>
        <Row>
          <RowKey>Created at:</RowKey>
          <div>
            { dayjs.unix(index.metadata.create_timestamp).utc().format("YYYY/MM/DD HH:MM") }
          </div>
        </Row>
        <Row>
          <RowKey>Updated at:</RowKey>
          <div>
            { dayjs.unix(index.metadata.update_timestamp).utc().format("YYYY/MM/DD HH:MM") }
          </div>
        </Row>
        <Row>
          <RowKey>URI:</RowKey>
          <div>
            { index.metadata.index_uri }
          </div>
        </Row>
        <Row>
          <RowKey>Size of pusblished splits:</RowKey>
          <div>
            <NumberFormat value={total_num_bytes / 1000000} displayType={'text'} thousandSeparator={true} suffix='MB' decimalScale={2}/>
          </div>
        </Row>
        <Row>
          <RowKey>Documents:</RowKey>
          <div>
            <NumberFormat value={total_num_docs} displayType={'text'} thousandSeparator={true} />
          </div>
        </Row>
        <Row>
          <RowKey>Number of published splits:</RowKey>
          <div>
            { published_splits.length }
          </div>
        </Row>
        <Row>
          <RowKey>Number of staged splits:</RowKey>
          <div>
            { num_of_staged_splits }
          </div>
        </Row>
        <Row>
          <RowKey>Number of splits marked for delete:</RowKey>
          <div>
            { num_of_marked_for_delete_splits }
          </div>
        </Row>
      </ItemContainer>
    </Paper>
  )
}
