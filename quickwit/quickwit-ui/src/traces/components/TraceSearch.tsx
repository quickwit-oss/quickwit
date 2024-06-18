import React, { useCallback, useEffect } from "react";
import styled from "@emotion/styled";
import { Autocomplete, Box, Button, Divider, TextField, Typography } from '@mui/material';

import { TimeRange, TimeRangeSelect } from '@/components/TimeRangeSelect';
import { JaegerAPIClient, ServiceList, ServiceOperationsList, TraceRequestParams } from "@/services/jaeger_api.client";
import { ONE_SECOND } from "@/utils/date";

const DEFAULT_RESULT_LIMIT = 20;

const TraceSearchBarWrapper = styled('div')({
  display: 'flex',
  height: '100%',
  flex: '0 0 260px',
  maxWidth: '260px',
  flexDirection: 'column',
  borderRight: '1px solid rgba(0, 0, 0, 0.12)',
  overflow: 'auto',
});

function QAutocomplete<T>(props:{
    options:T[],
    value?:T,
    onUpdate(value?:T):void
  }) {
  const [value, setValue] = React.useState<T|null>(props.value||null)
  useEffect(()=>{
    setValue(props.value||null);
  }, [props.value])
  return (
    <Autocomplete
      size="small"
      sx={{ width: '100%' }}
      options={props.options}
      value={value}
      onChange={(_, updatedValue) => {
        if (updatedValue == null) {
          setValue(null);
          props.onUpdate();
        } else {
          setValue(updatedValue);
          props.onUpdate(updatedValue);
        }
      }}
      renderInput={(params)=>(
        <TextField {...params}></TextField>
      )}
      />
  )
}


export function TraceSearchSidebar(props:{
    jaegerClient:JaegerAPIClient,
    onFetchTraces:(traceParams:TraceRequestParams)=>void
  }) {
  const [services, setServices] = React.useState<ServiceList>([])
  const [service, setService] = React.useState<string>("")
  const [operations, setOperations] = React.useState<ServiceOperationsList>([])
  const [operation, setOperation] = React.useState<string>("all")
  // const [limit, setLimit] = React.useState<number>(DEFAULT_RESULT_LIMIT)
  const limit = DEFAULT_RESULT_LIMIT;
  const [timeRange, setTimeRange] = React.useState<TimeRange>({startTimestamp:null, endTimestamp:null})


 const fetchServices = useCallback(()=>{
    props.jaegerClient.getServices().then(
      (fetchedServices) => { setServices(fetchedServices); }
    )
  }, [props.jaegerClient])

  const fetchServiceOperations = useCallback((service)=>{
    if (service) {
      props.jaegerClient.getServiceOperations(service).then(
        (fetchedOperations) => { setOperations(fetchedOperations); }
      );
    }
    else {
      setOperations([])
    }
  }, [props.jaegerClient, service] );

  useEffect(()=> {
    fetchServices();
  }, [fetchServices])

  useEffect(()=>{
    if (!service && services.length ) {
      setService(services[0]!)
    }
  }, [services])

  // Get operations for service
  useEffect(()=>{
    fetchServiceOperations(service);
  }, [service])

  // Render sidebar
  return (
    <TraceSearchBarWrapper>
      <Box sx={{ px: 2, py: 2}}>
        <Typography variant='body1' mb={1}>
        Service:
        </Typography>
        <QAutocomplete
          options={services}
          value={service||""}
          onUpdate={(updatedService)=>{
            if (updatedService) setService(updatedService);
          }}/>
      </Box>
      {service && ( <React.Fragment>
        <Divider/>
        <Box sx={{ flexGrow:1, padding: 2}}>
          <Box sx={{mb:'1rem'}}>
            <Typography variant='body1' mb={1}> Operation: </Typography>
            <QAutocomplete
              options={['all', ...operations]}
              value={operation}
              onUpdate={
                (updatedOperation)=>{
                  if (!updatedOperation)
                    setOperation("all");
                  else
                    setOperation(updatedOperation);
                  }
              }/>
          </Box>

          <Box sx={{mb:'1rem'}}>
            <Typography variant='body1' mb={1}> Tags: </Typography>
            <TextField size="small" sx={{width:'100%'}}></TextField>
          </Box>

        </Box>

        <Box sx={{padding:2}}>
          <Box sx={{mb:'1rem'}}>
            <TimeRangeSelect
                sx={{width:'100%'}}
                timeRange={timeRange}
                onUpdate={ (newTimeRange)=>{ setTimeRange(newTimeRange) } }
                variant="outlined"
              />
          </Box>
          <Button variant="contained" sx={{width:'100%'}} onClick={
            ()=>{
              const params : TraceRequestParams = { service };
              if (operation !== "all") { params.operation = operation; }
              if (timeRange.startTimestamp) { params.start = timeRange.startTimestamp*ONE_SECOND; }
              if (timeRange.endTimestamp) { params.end = timeRange.endTimestamp*ONE_SECOND; }
              if (limit) { params.limit = limit; }
              // if (tags) { params.tags = tags; }
              
              props.onFetchTraces(params) }
          }>Find Traces</Button>
        </Box>
      </React.Fragment>)}
    </TraceSearchBarWrapper>
  )
}