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

import React, { useEffect, useState } from "react";
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
import { DateTimePicker } from "@mui/lab";
import { SearchRequest } from "../utils/models";
import { SearchComponentProps } from "../utils/SearchComponentProps";

dayjs.extend(relativeTime)

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
  const initialState = {width: 310, anchor: null, customDatesPanelOpen: false};
  const [state, setState] = useState<TimeRangeSelectState>(initialState);
  const [searchRequest, ] = useState<SearchRequest>(props.searchRequest);

  const handleOpenClick = (event: React.MouseEvent<HTMLButtonElement>) => {
    setState((prevState) => {
      return { ...prevState, anchor: event.currentTarget };
    });
  };

  /*const handleApplyCustomDates = (request: SearchRequest) => {
    setState(initialState);
  };*/

  const handleOpenCustomDatesPanelClick = () => {
    setState((prevState) => {
      return { ...prevState, customDatesPanelOpen: true, width: 620 };
    });
  };

  const handleClose = () => {
    setState(initialState);
  };

  const handleTimeRangeChoiceClick = (secondsBeforeNow: number) => {
    setState(initialState);
    //const startTimestamp = Math.round(Date.now() / 1000) - secondsBeforeNow;
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
        disabled={props.queryRunning || searchRequest.indexId == null}
      >
        <DateTimeRangeLabel startTimestamp={searchRequest.startTimestamp} endTimestamp={searchRequest.endTimestamp} />
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
                    onClick={() => handleTimeRangeChoiceClick(+value[1])}
                    >
                    <ListItemText primary={value[0]} />
                  </ListItem>
                })}
                <ListItem button onClick={handleOpenCustomDatesPanelClick}>
                  <ListItemIcon style={{alignItems: "center"}}>
                    <DateRange />
                  </ListItemIcon>
                  <ListItemText primary="Customize dates" />
                  <ListItemIcon style={{alignItems: "center"}}>
                    <ChevronRight />
                  </ListItemIcon>
                </ListItem>
              </List>
            </Box>
            {state.customDatesPanelOpen && (
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
  const [startDate, setStartDate] = useState<Date | null>(null);
  const [endDate, setEndDate] = useState<Date | null>(null);
  useEffect(() => {
    const initStartTimestamp = props.searchRequest.startTimestamp,
    initEndTimeStamp = props.searchRequest.endTimestamp;
    setStartDate(initStartTimestamp ? new Date(initStartTimestamp * 1000) : null);
    setEndDate(initEndTimeStamp ? new Date(initEndTimeStamp * 1000) : null);
  }, [props.searchRequest]);
  const handleReset = (event: React.MouseEvent<HTMLButtonElement>) => {
    event.preventDefault();
    setStartDate(null);
    setEndDate(null)
    props.onSearchRequestUpdate({...props.searchRequest, startTimestamp: null, endTimestamp: null});
  };
  const handleApply = (event: React.MouseEvent<HTMLButtonElement>) => {
    event.preventDefault();
    const startTimestamp = startDate ? startDate.getTime() / 1000 : null;
    const endTimestamp = endDate ? endDate.getTime() / 1000 : null;
    props.onSearchRequestUpdate({...props.searchRequest, startTimestamp: startTimestamp, endTimestamp: endTimestamp});
  };

  return (
    <Box display="flex" flexDirection="column" width="50%" p={2}>
      <Box flexGrow={1}>
        <Box pb={1.5}>
          <DateTimePicker
            label="Start Date"
            value={startDate}
            onChange={(newValue) => setStartDate(newValue ? new Date(newValue) : null)}
            renderInput={(params) => <TextField {...params} />}
          />
        </Box>
        <Box>
          <DateTimePicker
            label="End Date"
            value={endDate}
            onChange={(newValue) => setEndDate(newValue ? new Date(newValue) : null)}
            renderInput={(params) => <TextField {...params} />}
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
  );
}

interface DateTimeRangeLabelProps {
  startTimestamp: number | null;
  endTimestamp: number | null;
}

function DateTimeRangeLabel(props: DateTimeRangeLabelProps): JSX.Element {
  const [startTimestamp, setStartTimestamp] = useState(props.startTimestamp);
  const [endTimestamp, setEndTimestamp] = useState(props.endTimestamp);
  
  useEffect(() => {
    setStartTimestamp(props.startTimestamp);
    setEndTimestamp(props.endTimestamp);
  }, [props.startTimestamp, props.endTimestamp])


  function Label() {
    if (startTimestamp != null && endTimestamp != null) {
      return <>
        {dayjs.unix(startTimestamp).format("YYYY/MM/DD HH:mm:ss")} -{" "}
        {dayjs.unix(endTimestamp).format("YYYY/MM/DD HH:mm:ss")}
      </>
    } else if (startTimestamp != null && endTimestamp == null) {
      return <>Since {dayjs.unix(startTimestamp).fromNow(true)}</>
    } else if (startTimestamp == null && endTimestamp != null) {
      return <>Before {dayjs.unix(endTimestamp).format("YYYY/MM/DD HH:mm:ss")}</>
    }
    return <>No date range</>
  }

  return (
    <span style={{textTransform: "none"}}>
      <Label />
    </span>
  );
}
