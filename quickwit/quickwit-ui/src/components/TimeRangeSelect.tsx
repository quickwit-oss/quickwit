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
import { Dayjs, default as dayjs } from 'dayjs';
import relativeTime from "dayjs/plugin/relativeTime"
import utc from "dayjs/plugin/utc"
import { DateTimePicker } from "@mui/lab";
import { SearchComponentProps } from "../utils/SearchComponentProps";
import DateAdapter from '@mui/lab/AdapterDayjs';
import LocalizationProvider from '@mui/lab/LocalizationProvider';
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

interface TimeRangeSelectState {
  anchor: HTMLElement | null;
  customDatesPanelOpen: boolean;
  width: number;
}

export function TimeRangeSelect(props: SearchComponentProps): JSX.Element {
  const getInitialState = () => {return {width: 220, anchor: null, customDatesPanelOpen: false}};
  const initialState = useMemo(() => {return getInitialState(); }, []);
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
  }, [props.queryRunning, initialState])

  const handleClose = () => {
    setState(initialState);
  };

  const handleTimeRangeChoiceClick = (secondsBeforeNow: number | string | undefined) => {
    if (secondsBeforeNow === undefined) {
      return;
    }
    // Ensures that we have a number.
    secondsBeforeNow = +secondsBeforeNow;
    setState(initialState);
    const startTimestamp = Math.trunc(Date.now() / 1000) - secondsBeforeNow;
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
        <DateTimeRangeLabel startTimestamp={props.searchRequest.startTimestamp} endTimestamp={props.searchRequest.endTimestamp} />
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
  const [startDate, setStartDate] = useState<Dayjs | null>(null);
  const [endDate, setEndDate] = useState<Dayjs | null>(null);

  useEffect(() => {
    setStartDate(props.searchRequest.startTimestamp ? convertTimestampSecsIntoDateUtc(props.searchRequest.startTimestamp) : null);
    setEndDate(props.searchRequest.endTimestamp ? convertTimestampSecsIntoDateUtc(props.searchRequest.endTimestamp) : null);
  }, [props.searchRequest.startTimestamp, props.searchRequest.endTimestamp]);
  const handleReset = (event: React.MouseEvent<HTMLButtonElement>) => {
    event.preventDefault();
    setStartDate(null);
    setEndDate(null)
    props.onSearchRequestUpdate({...props.searchRequest, startTimestamp: null, endTimestamp: null});
  };
  const handleApply = (event: React.MouseEvent<HTMLButtonElement>) => {
    event.preventDefault();
    const startTimestamp = startDate ? startDate.valueOf() / 1000 : null;
    const endTimestamp = endDate ? endDate.valueOf() / 1000 : null;
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
              inputFormat={DATE_TIME_WITH_SECONDS_FORMAT}
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
              inputFormat={DATE_TIME_WITH_SECONDS_FORMAT}
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
}

function DateTimeRangeLabel(props: DateTimeRangeLabelProps): JSX.Element {
  function Label() {
    if (props.startTimestamp !== null && props.endTimestamp !== null) {
      return <>
        {convertTimestampSecsIntoDateUtc(props.startTimestamp).format(DATE_TIME_WITH_SECONDS_FORMAT)} -{" "}
        {convertTimestampSecsIntoDateUtc(props.startTimestamp).format(DATE_TIME_WITH_SECONDS_FORMAT)}
      </>
    } else if (props.startTimestamp !== null && props.endTimestamp === null) {
      return <>Since {convertTimestampSecsIntoDateUtc(props.startTimestamp).fromNow(true)}</>
    } else if (props.startTimestamp == null && props.endTimestamp != null) {
      return <>Before {convertTimestampSecsIntoDateUtc(props.endTimestamp).format(DATE_TIME_WITH_SECONDS_FORMAT)}</>
    }
    return <>No date range</>
  }

  return (
    <span style={{textTransform: "none"}}>
      <Label />
    </span>
  );
}

function convertTimestampSecsIntoDateUtc(timestamp_secs: number): Dayjs {
  return dayjs(timestamp_secs * 1000).utc();
}
