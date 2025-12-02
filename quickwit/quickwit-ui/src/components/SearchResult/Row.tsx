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

import { KeyboardArrowDown } from "@mui/icons-material";
import ChevronRight from "@mui/icons-material/ChevronRight";
import { Box, IconButton, styled, TableCell, TableRow } from "@mui/material";
import dayjs from "dayjs";
import relativeTime from "dayjs/plugin/relativeTime";
import utc from "dayjs/plugin/utc";
import React, { useState } from "react";
import {
  DATE_TIME_WITH_SECONDS_FORMAT as DATE_TIME_WITH_MILLISECONDS_FORMAT,
  DATE_TIME_WITH_SECONDS_FORMAT,
  Entry,
  Field,
  RawDoc,
} from "../../utils/models";
import { QUICKWIT_INTERMEDIATE_GREY } from "../../utils/theme";
import { JsonEditor } from "../JsonEditor";

dayjs.extend(relativeTime);
dayjs.extend(utc);

interface RowProps {
  timestampField: Field | null;
  row: RawDoc;
}

const EntryName = styled("dt")`
  display: inline;
  background-color: ${QUICKWIT_INTERMEDIATE_GREY};
  color: #343741;
  padding: 2px 1px 2px 4px;
  margin-right: 4px;
  word-break: normal;
  border-radius: 3px;
`;

const EntryValue = styled("dd")`
  display: inline;
  margin: 0;
  padding: 0;
  margin-inline-end: 5px;
`;

function EntryFormatter(entry: Entry) {
  // Some field can contains objects, stringify them to render them otherwise React will crash.
  const value =
    typeof entry.value === "object" ? JSON.stringify(entry.value) : entry.value;
  return (
    <>
      <EntryName>{entry.key}:</EntryName>
      <EntryValue>{value}</EntryValue>
    </>
  );
}

// Display the timestamp value if found in a `TableCell`.
function DisplayTimestampValue(row: RawDoc, timestampField: Field | null) {
  if (
    timestampField === null ||
    timestampField.field_mapping.output_format === null
  ) {
    return <></>;
  }
  let field_value = row;
  for (const path_segment of timestampField.path_segments) {
    field_value = field_value[path_segment];
  }
  if (!field_value) {
    return <></>;
  }
  return (
    <TableCell sx={{ verticalAlign: "top", padding: "4px" }}>
      <Box
        sx={{
          maxHeight: "115px",
          width: "90px",
          display: "inline-block",
          wordBreak: "break-word",
        }}
      >
        {formatDateTime(
          field_value,
          timestampField.field_mapping.output_format,
        )}
      </Box>
    </TableCell>
  );
}

function formatDateTime(field_value: any, timestampOutputFormat: string): any {
  // A unix timestamp can be in secs/millis/micros/nanos and need to be converted properly.
  if (
    timestampOutputFormat === "unix_timestamp_secs" &&
    typeof field_value === "number"
  ) {
    return dayjs(field_value * 1000)
      .utc()
      .format(DATE_TIME_WITH_SECONDS_FORMAT);
  } else if (
    timestampOutputFormat === "unix_timestamp_millis" &&
    typeof field_value === "number"
  ) {
    return dayjs(field_value).utc().format(DATE_TIME_WITH_MILLISECONDS_FORMAT);
  } else if (
    timestampOutputFormat === "unix_timestamp_micros" &&
    typeof field_value === "number"
  ) {
    return dayjs(field_value / 1000)
      .utc()
      .format(DATE_TIME_WITH_MILLISECONDS_FORMAT);
  } else if (
    timestampOutputFormat === "unix_timestamp_nanos" &&
    typeof field_value === "number"
  ) {
    return dayjs(field_value / 1000000)
      .utc()
      .format(DATE_TIME_WITH_MILLISECONDS_FORMAT);
  } else {
    // Other formats are string values and we can just display it as is.
    return field_value;
  }
}

const BreakWordBox = styled("dl")({
  verticalAlign: "top",
  display: "inline-block",
  color: "#464646",
  wordBreak: "break-all",
  wordWrap: "break-word",
  margin: 1,
  overflow: "hidden",
  lineHeight: "1.8em",
});

export function Row(props: RowProps) {
  const [open, setOpen] = useState(false);
  const entries: Entry[] = [];
  for (const [key, value] of Object.entries(props.row)) {
    entries.push({ key: key, value: value });
  }
  return (
    <>
      <TableRow>
        <TableCell
          sx={{ px: 0, py: 0, verticalAlign: "top", padding: "0  px" }}
        >
          <IconButton
            aria-label="expand row"
            size="small"
            onClick={() => setOpen(!open)}
          >
            {open ? <KeyboardArrowDown /> : <ChevronRight />}
          </IconButton>
        </TableCell>
        {DisplayTimestampValue(props.row, props.timestampField)}
        <TableCell sx={{ padding: "4px" }}>
          {!open && (
            <BreakWordBox sx={{ maxHeight: "100px" }}>
              {entries.map((entry) => (
                <React.Fragment key={entry.key}>
                  {EntryFormatter(entry)}
                </React.Fragment>
              ))}
            </BreakWordBox>
          )}
          {open && <JsonEditor content={props.row} resizeOnMount={true} />}
        </TableCell>
      </TableRow>
    </>
  );
}
