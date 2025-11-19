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

import styled from "@emotion/styled";
import { Paper } from "@mui/material";
import dayjs from "dayjs";
import utc from "dayjs/plugin/utc";
import { FC, ReactNode } from "react";
import { NumericFormat } from "react-number-format";
import { Index } from "../utils/models";

dayjs.extend(utc);

const ItemContainer = styled.div`
  padding: 10px;
  display: flex;
  flex-direction: column;
`;
const Row = styled.div`
  padding: 5px;
  display: flex;
  flex-direction: row;
  &:nth-of-type(odd) {
    background: rgba(0, 0, 0, 0.05);
  }
`;
const RowKey = styled.div`
  width: 350px;
`;
const IndexRow: FC<{ title: string; children: ReactNode }> = ({
  title,
  children,
}) => (
  <Row>
    <RowKey>{title}</RowKey>
    <div>{children}</div>
  </Row>
);

export function IndexSummary({ index }: { index: Index }) {
  const all_splits = index.splits;
  const published_splits = all_splits.filter(
    (split) => split.split_state === "Published",
  );
  const num_of_staged_splits = all_splits.filter(
    (split) => split.split_state === "Staged",
  ).length;
  const num_of_marked_for_delete_splits = all_splits.filter(
    (split) => split.split_state === "MarkedForDeletion",
  ).length;
  const total_num_docs = published_splits
    .map((split) => split.num_docs)
    .reduce((sum, current) => sum + current, 0);
  const total_num_bytes = published_splits
    .map((split) => {
      return split.footer_offsets.end;
    })
    .reduce((sum, current) => sum + current, 0);
  const total_uncompressed_num_bytes = published_splits
    .map((split) => {
      return split.uncompressed_docs_size_in_bytes;
    })
    .reduce((sum, current) => sum + current, 0);
  return (
    <Paper variant="outlined">
      <ItemContainer>
        <IndexRow title="Created at:">
          {dayjs
            .unix(index.metadata.create_timestamp)
            .utc()
            .format("YYYY/MM/DD HH:mm")}
        </IndexRow>
        <IndexRow title="URI:">
          {index.metadata.index_config.index_uri}
        </IndexRow>
        <IndexRow title="Number of published documents:">
          <NumericFormat
            value={total_num_docs}
            displayType={"text"}
            thousandSeparator={true}
          />
        </IndexRow>
        <IndexRow title="Size of published documents (uncompressed):">
          <NumericFormat
            value={total_uncompressed_num_bytes / 1000000}
            displayType={"text"}
            thousandSeparator={true}
            suffix=" MB"
            decimalScale={2}
          />
        </IndexRow>
        <IndexRow title="Number of published splits:">
          {published_splits.length}
        </IndexRow>
        <IndexRow title="Size of published splits:">
          <NumericFormat
            value={total_num_bytes / 1000000}
            displayType={"text"}
            thousandSeparator={true}
            suffix=" MB"
            decimalScale={2}
          />
        </IndexRow>
        <IndexRow title="Number of staged splits:">
          {num_of_staged_splits}
        </IndexRow>
        <IndexRow title="Number of splits marked for deletion:">
          {num_of_marked_for_delete_splits}
        </IndexRow>
      </ItemContainer>
    </Paper>
  );
}
