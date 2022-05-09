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

import styled from "@emotion/styled";
import { Paper } from "@mui/material";
import dayjs from "dayjs";
import NumberFormat from "react-number-format";
import { Index } from "../utils/models";

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
  const total_num_docs = index.splits
    .map(split => split.num_docs)
    .reduce((sum, current) => sum + current, 0);
  const total_num_bytes = index.splits
    .map(split => split.size_in_bytes)
    .reduce((sum, current) => sum + current, 0);
  return (
    <Paper variant="outlined" >
      <ItemContainer>
        <Row>
          <RowKey>Created at:</RowKey>
          <div>
            { dayjs.unix(index.metadata.create_timestamp).format("YYYY/MM/DD HH:MM") }
          </div>
        </Row>
        <Row>
          <RowKey>Updated at:</RowKey>
          <div>
            { dayjs.unix(index.metadata.update_timestamp).format("YYYY/MM/DD HH:MM") }
          </div>
        </Row>
        <Row>
          <RowKey>URI:</RowKey>
          <div>
            { index.metadata.index_uri }
          </div>
        </Row>
        <Row>
          <RowKey>Size:</RowKey>
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
          <RowKey>Splits:</RowKey>
          <div>
            { index.splits.length }
          </div>
        </Row>
      </ItemContainer>
    </Paper>
  )
}