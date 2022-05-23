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

import React, { useEffect, useMemo, useState } from "react";
import {
  Box,
  Button,
  Divider,
  List,
  ListItem,
  ListItemIcon,
  ListItemText,
  Popover,
  TextField,
} from "@mui/material";
import { AccessTime, ChevronRight, DateRange } from "@mui/icons-material";
import { default as dayjs } from 'dayjs';
import relativeTime from "dayjs/plugin/relativeTime"
import utc from "dayjs/plugin/utc"
import { DateTimePicker } from "@mui/lab";
import { getDateTimeFormat, guessTimeUnit, TimeUnit } from "../utils/models";
import { SearchComponentProps } from "../utils/SearchComponentProps";
import DateAdapter from '@mui/lab/AdapterDayjs';
import LocalizationProvider from '@mui/lab/LocalizationProvider';

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

interface TimeRangeSelectState {
  anchor: HTMLElement | null;
  customDatesPanelOpen: boolean;
  width: number;
}

function convertFromMilliSecond(value: number | null, targetTimeUnit: TimeUnit): number | null {
  if (value === null) {
    return null;
  } 
  if (targetTimeUnit === TimeUnit.MILLI_SECOND) {
    return value;
  } else if (targetTimeUnit === TimeUnit.SECOND) {
    return Math.round(value / 1000);
  }
  // `TimeRangeSelect` should not be displayed with a unknown/unsupported TimeUnit.
  throw new Error(`Cannot convertFromMilliSecond with a time unit ${targetTimeUnit}.`);
}

function convertToMilliSecond(value: number | null, valueTimeUnit: TimeUnit): number | null {
  if (value === null) {
    return null;
  } 
  if (valueTimeUnit === TimeUnit.MILLI_SECOND) {
    return value;
  } else if (valueTimeUnit === TimeUnit.SECOND) {
    return value * 1e3;
  }
  // `TimeRangeSelect` should not be displayed with a unknown/unsupported TimeUnit.
  throw new Error(`Cannot convertToMilliSecond with a time unit ${valueTimeUnit}.`);
}

export function TimeRangeSelect(props: SearchComponentProps): JSX.Element {
  const getInitialState = () => {return {width: 220, anchor: null, customDatesPanelOpen: false}};
  const initialState = useMemo(() => {return getInitialState(); }, []);
  const [state, setState] = useState<TimeRangeSelectState>(initialState);
  const timeUnit = props.index === null ? TimeUnit.MILLI_SECOND : guessTimeUnit(props.index);

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
  }, [props.queryRunning, initialState])

  const handleClose = () => {
    setState(initialState);
  };

  const handleTimeRangeChoiceClick = (secondsBeforeNow: number | string | undefined) => {
    if (secondsBeforeNow === undefined) {
      return;
    }
    secondsBeforeNow = +secondsBeforeNow;
    setState(initialState);
    const startTimestampInMilliSec = Date.now() - secondsBeforeNow * 1000;
    const startTimestamp = convertFromMilliSecond(startTimestampInMilliSec, timeUnit);
    props.runSearch({...props.searchRequest, startTimestamp: startTimestamp, endTimestamp: null});
  };

  const handleReset = () => {
    props.runSearch({...props.searchRequest, startTimestamp: null, endTimestamp: null});
  };

  const open = Boolean(state.anchor);
  const id = open ? "time-range-select-popover" : undefined;

  return (
    <div>
      <Button
        variant="contained"
        disableElevation
        onClick={handleOpenClick}
        startIcon={<AccessTime />}
        disabled={props.queryRunning || props.searchRequest.indexId == null}
      >
        <DateTimeRangeLabel timeUnit={timeUnit} startTimestamp={props.searchRequest.startTimestamp} endTimestamp={props.searchRequest.endTimestamp} />
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
                  return  <ListItem
                    key={idx}
                    button
                    onClick={() => handleTimeRangeChoiceClick(value[1])}
                    >
                    <ListItemText primary={value[0]} />
                  </ListItem>
                })}
                <ListItem button onClick={handleReset}>
                  <ListItemText primary="Reset" />
                </ListItem>
                <ListItem button onClick={handleOpenCustomDatesPanelClick}>
                  <ListItemIcon sx={{alignItems: "left", minWidth: 'inherit', paddingRight: '8px'}}>
                    <DateRange />
                  </ListItemIcon>
                  <ListItemText primary="Custom dates" sx={{ paddingRight: '16px' }} />
                  <ListItemIcon sx={{ minWidth: 'inherit' }}>
                    <ChevronRight />
                  </ListItemIcon>
                </ListItem>
              </List>
            </Box>
            {state.anchor !== null && state.customDatesPanelOpen && (
              <CustomDatesPanel
                { ...props }
              />
            )}
          </Box>
        </Box>
      </Popover>
    </div>
  );
}

function CustomDatesPanel(props: SearchComponentProps): JSX.Element {
  const [startDate, setStartDate] = useState<dayjs.Dayjs | null>(null);
  const [endDate, setEndDate] = useState<dayjs.Dayjs | null>(null);
  const timeUnit = props.index === null ? TimeUnit.MILLI_SECOND : guessTimeUnit(props.index);
  const dateTimeFormat = getDateTimeFormat(timeUnit);

  useEffect(() => {
    const initStartTimestamp = convertToMilliSecond(props.searchRequest.startTimestamp, timeUnit);
    const initEndTimeStamp = convertToMilliSecond(props.searchRequest.endTimestamp, timeUnit);
    setStartDate(initStartTimestamp ? dayjs(initStartTimestamp).utc() : null);
    setEndDate(initEndTimeStamp ? dayjs(initEndTimeStamp).utc() : null);
  }, [props.searchRequest, timeUnit]);
  const handleReset = (event: React.MouseEvent<HTMLButtonElement>) => {
    event.preventDefault();
    setStartDate(null);
    setEndDate(null)
    props.onSearchRequestUpdate({...props.searchRequest, startTimestamp: null, endTimestamp: null});
  };
  const handleApply = (event: React.MouseEvent<HTMLButtonElement>) => {
    event.preventDefault();
    const startTimestamp = convertFromMilliSecond(startDate ? startDate.valueOf() : null, timeUnit);
    const endTimestamp = convertFromMilliSecond(endDate ? endDate.valueOf() : null, timeUnit);
    props.runSearch({...props.searchRequest, startTimestamp: startTimestamp, endTimestamp: endTimestamp});
  };

  return (
    <LocalizationProvider dateAdapter={DateAdapter}>
      <Box display="flex" flexDirection="column" p={2} sx={{ minWidth: '300px'}}>
        <Box flexGrow={1}>
          <Box pb={1.5}>
            <DateTimePicker
              label="Start Date"
              value={startDate}
              inputFormat={dateTimeFormat}
              onChange={(newValue) => {
                // By default, newValue is a datetime defined on the local time zone and for now we consider
                // input/output only in UTC.
                setStartDate(newValue ? dayjs(newValue.valueOf() + newValue.utcOffset() * 60 * 1000).utc() : null);
              }}
              renderInput={(params) => <TextField {...params} sx={{width: '100%'}} />}
            />
          </Box>
          <Box>
            <DateTimePicker
              label="End Date"
              value={endDate}
              inputFormat={dateTimeFormat}
              onChange={(newValue) => {
                // By default, newValue is a datetime defined on the local time zone and for now we consider
                // input/output only in UTC.
                setEndDate(newValue ? dayjs(newValue.valueOf() + newValue.utcOffset() * 60 * 1000).utc() : null);
              }}
              renderInput={(params) => <TextField {...params} sx={{width: '100%'}} />}
            />
          </Box>
        </Box>
        <Box display="flex">
          <Button
            variant="outlined"
            color="primary"
            onClick={handleReset}
            disableElevation
            style={{marginRight: 10}}
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
  timeUnit: TimeUnit;
}

function DateTimeRangeLabel(props: DateTimeRangeLabelProps): JSX.Element {
  const [startTimestamp, setStartTimestamp] = useState(convertToMilliSecond(props.startTimestamp, props.timeUnit));
  const [endTimestamp, setEndTimestamp] = useState(convertToMilliSecond(props.endTimestamp, props.timeUnit));
  const dateTimeFormat = getDateTimeFormat(props.timeUnit);

  useEffect(() => {
    setStartTimestamp(convertToMilliSecond(props.startTimestamp, props.timeUnit));
    setEndTimestamp(convertToMilliSecond(props.endTimestamp, props.timeUnit));
  }, [props.startTimestamp, props.endTimestamp, props.timeUnit])

  function Label() {
    if (startTimestamp !== null && endTimestamp !== null) {
      return <>
        {dayjs(startTimestamp).utc().format(dateTimeFormat)} -{" "}
        {dayjs(endTimestamp).utc().format(dateTimeFormat)}
      </>
    } else if (startTimestamp !== null && endTimestamp === null) {
      return <>Since {dayjs(startTimestamp).utc().fromNow(true)}</>
    } else if (startTimestamp == null && endTimestamp != null) {
      return <>Before {dayjs(endTimestamp).utc().format(dateTimeFormat)}</>
    }
    return <>No date range</>
  }

  return (
    <span style={{textTransform: "none"}}>
      <Label />
    </span>
  );
}
