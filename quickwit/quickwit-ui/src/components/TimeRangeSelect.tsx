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

import { AccessTime, ChevronRight, DateRange } from "@mui/icons-material";
import {
  Box,
  Button,
  Divider,
  List,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  Popover,
} from "@mui/material";
import { DateTimePicker, LocalizationProvider } from "@mui/x-date-pickers";
import { AdapterDayjs } from "@mui/x-date-pickers/AdapterDayjs";
import { Dayjs, default as dayjs } from "dayjs";
import relativeTime from "dayjs/plugin/relativeTime";
import utc from "dayjs/plugin/utc";
import React, { JSX, useEffect, useMemo, useState } from "react";
import { DATE_TIME_WITH_SECONDS_FORMAT } from "../utils/models";

dayjs.extend(relativeTime);
dayjs.extend(utc);

const TIME_RANGE_CHOICES = [
  ["Last 15 min", 15 * 60],
  ["Last 30 min", 30 * 60],
  ["Last 1 hour", 60 * 60],
  ["Last 7 days", 7 * 24 * 60 * 60],
  ["Last 30 days", 30 * 24 * 60 * 60],
  ["Last 3 months", 90 * 24 * 60 * 60],
  ["Last year", 365 * 24 * 60 * 60],
];

type TimeRange = {
  startTimestamp: number | null;
  endTimestamp: number | null;
};

export interface TimeRangeSelectProps {
  timeRange: TimeRange;
  disabled?: boolean;
  onUpdate(newTimeRange: TimeRange): void;
}

interface TimeRangeSelectState {
  anchor: HTMLElement | null;
  customDatesPanelOpen: boolean;
  width: number;
}

export function TimeRangeSelect(props: TimeRangeSelectProps): JSX.Element {
  const getInitialState = () => {
    return { width: 220, anchor: null, customDatesPanelOpen: false };
  };
  const initialState = useMemo(() => {
    return getInitialState();
  }, []);
  const [state, setState] = useState<TimeRangeSelectState>(initialState);

  const handleOpenClick = (event: React.MouseEvent<HTMLButtonElement>) => {
    setState((prevState) => {
      return { ...prevState, anchor: event.currentTarget };
    });
  };

  const handleOpenCustomDatesPanelClick = () => {
    setState((prevState) => {
      return { ...prevState, customDatesPanelOpen: true, width: 500 };
    });
  };

  useEffect(() => {
    setState(initialState);
  }, [props.disabled, initialState]);

  const handleClose = () => {
    setState(initialState);
  };

  const handleTimeRangeChoiceClick = (
    secondsBeforeNow: number | string | undefined,
  ) => {
    if (secondsBeforeNow === undefined) {
      return;
    }
    // Ensures that we have a number.
    secondsBeforeNow = +secondsBeforeNow;
    setState(initialState);
    const startTimestamp = Math.trunc(Date.now() / 1000) - secondsBeforeNow;
    props.onUpdate({ startTimestamp, endTimestamp: null });
  };

  const handleReset = () => {
    props.onUpdate({ startTimestamp: null, endTimestamp: null });
  };

  const open = Boolean(state.anchor);
  const id = open ? "time-range-select-popover" : undefined;

  return (
    <Box sx={{ padding: "10px" }}>
      <Button
        variant="contained"
        disableElevation
        onClick={handleOpenClick}
        startIcon={<AccessTime />}
        disabled={props.disabled}
      >
        <DateTimeRangeLabel
          startTimestamp={props.timeRange.startTimestamp}
          endTimestamp={props.timeRange.endTimestamp}
        />
      </Button>
      <Popover
        id={id}
        open={open}
        anchorEl={state.anchor}
        onClose={handleClose}
        anchorOrigin={{
          vertical: "bottom",
          horizontal: "center",
        }}
        transformOrigin={{
          vertical: "top",
          horizontal: "center",
        }}
        PaperProps={{
          style: { width: state.width },
        }}
      >
        <Box display="flex" flexDirection="column">
          <Box p={1.5}>
            <b>Select a period</b>
          </Box>
          <Divider />
          <Box display="flex" flexDirection="row">
            <Box flexGrow={1} borderRight={1} borderColor="grey.300">
              <List disablePadding>
                {TIME_RANGE_CHOICES.map((value, idx) => {
                  return (
                    <ListItemButton
                      key={idx}
                      onClick={() => handleTimeRangeChoiceClick(value[1])}
                    >
                      <ListItemText primary={value[0]} />
                    </ListItemButton>
                  );
                })}
                <ListItemButton onClick={handleReset}>
                  <ListItemText primary="Reset" />
                </ListItemButton>
                <ListItemButton onClick={handleOpenCustomDatesPanelClick}>
                  <ListItemIcon
                    sx={{
                      alignItems: "left",
                      minWidth: "inherit",
                      paddingRight: "8px",
                    }}
                  >
                    <DateRange />
                  </ListItemIcon>
                  <ListItemText
                    primary="Custom dates"
                    sx={{ paddingRight: "16px" }}
                  />
                  <ListItemIcon sx={{ minWidth: "inherit" }}>
                    <ChevronRight />
                  </ListItemIcon>
                </ListItemButton>
              </List>
            </Box>
            {state.anchor !== null && state.customDatesPanelOpen && (
              <CustomDatesPanel {...props} />
            )}
          </Box>
        </Box>
      </Popover>
    </Box>
  );
}

function CustomDatesPanel(props: TimeRangeSelectProps): JSX.Element {
  const [startDate, setStartDate] = useState<Dayjs | null>(null);
  const [endDate, setEndDate] = useState<Dayjs | null>(null);

  useEffect(() => {
    setStartDate(
      props.timeRange.startTimestamp
        ? convertTimestampSecsIntoDateUtc(props.timeRange.startTimestamp)
        : null,
    );
    setEndDate(
      props.timeRange.endTimestamp
        ? convertTimestampSecsIntoDateUtc(props.timeRange.endTimestamp)
        : null,
    );
  }, [props.timeRange.startTimestamp, props.timeRange.endTimestamp]);
  const handleReset = (event: React.MouseEvent<HTMLButtonElement>) => {
    event.preventDefault();
    setStartDate(null);
    setEndDate(null);
    props.onUpdate({ startTimestamp: null, endTimestamp: null });
  };
  const handleApply = (event: React.MouseEvent<HTMLButtonElement>) => {
    event.preventDefault();
    const startTimestamp = startDate ? startDate.valueOf() / 1000 : null;
    const endTimestamp = endDate ? endDate.valueOf() / 1000 : null;
    props.onUpdate({ startTimestamp, endTimestamp });
  };

  return (
    <LocalizationProvider dateAdapter={AdapterDayjs}>
      <Box
        display="flex"
        flexDirection="column"
        p={2}
        sx={{ minWidth: "300px" }}
      >
        <Box flexGrow={1}>
          <Box pb={1.5}>
            <DateTimePicker
              label="Start Date"
              value={startDate}
              format={DATE_TIME_WITH_SECONDS_FORMAT}
              onChange={(newValue: null | Dayjs) => {
                // By default, newValue is a datetime defined on the local time zone and for now we consider
                // input/output only in UTC.
                setStartDate(
                  newValue
                    ? dayjs(
                        newValue.valueOf() + newValue.utcOffset() * 60 * 1000,
                      ).utc()
                    : null,
                );
              }}
              slotProps={{ textField: { sx: { width: "100%" } } }}
            />
          </Box>
          <Box>
            <DateTimePicker
              label="End Date"
              value={endDate}
              format={DATE_TIME_WITH_SECONDS_FORMAT}
              onChange={(newValue: null | Dayjs) => {
                // By default, newValue is a datetime defined on the local time zone and for now we consider
                // input/output only in UTC.
                setEndDate(
                  newValue
                    ? dayjs(
                        newValue.valueOf() + newValue.utcOffset() * 60 * 1000,
                      ).utc()
                    : null,
                );
              }}
              slotProps={{ textField: { sx: { width: "100%" } } }}
            />
          </Box>
        </Box>
        <Box display="flex">
          <Button
            variant="outlined"
            color="primary"
            onClick={handleReset}
            disableElevation
            style={{ marginRight: 10 }}
          >
            Reset
          </Button>
          <Button
            variant="contained"
            color="primary"
            onClick={handleApply}
            disableElevation
          >
            Apply
          </Button>
        </Box>
      </Box>
    </LocalizationProvider>
  );
}

interface DateTimeRangeLabelProps {
  startTimestamp: number | null;
  endTimestamp: number | null;
}

function DateTimeRangeLabel(props: DateTimeRangeLabelProps): JSX.Element {
  function Label() {
    if (props.startTimestamp !== null && props.endTimestamp !== null) {
      return (
        <>
          {convertTimestampSecsIntoDateUtc(props.startTimestamp).format(
            DATE_TIME_WITH_SECONDS_FORMAT,
          )}{" "}
          -{" "}
          {convertTimestampSecsIntoDateUtc(props.endTimestamp).format(
            DATE_TIME_WITH_SECONDS_FORMAT,
          )}
        </>
      );
    } else if (props.startTimestamp !== null && props.endTimestamp === null) {
      return (
        <>
          Since{" "}
          {convertTimestampSecsIntoDateUtc(props.startTimestamp).fromNow(true)}
        </>
      );
    } else if (props.startTimestamp == null && props.endTimestamp != null) {
      return (
        <>
          Before{" "}
          {convertTimestampSecsIntoDateUtc(props.endTimestamp).format(
            DATE_TIME_WITH_SECONDS_FORMAT,
          )}
        </>
      );
    }
    return <>No date range</>;
  }

  return (
    <span style={{ textTransform: "none" }}>
      <Label />
    </span>
  );
}

function convertTimestampSecsIntoDateUtc(timestamp_secs: number): Dayjs {
  return dayjs(timestamp_secs * 1000).utc();
}
