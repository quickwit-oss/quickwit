import { Box, Card, Chip, Divider, LinearProgress, Typography, useTheme } from '@mui/material';
import { FullBoxContainer, ViewUnderAppBarBox } from '@/components/LayoutUtils';
import { Trace, TraceRequestParams } from '../../services/jaeger_api.client';
import { useJaegerAPIClient } from '../client';
import { TraceSearchSidebar } from '../components/TraceSearch';
import React, { useCallback, useState, useEffect, useRef } from "react";

import TraceTimelineChart from '../components/charts/TraceTimelineChart';
import { Link } from 'react-router-dom';
import { formatDuration } from '@/utils/date';
import { useSize } from '@/utils/hooks';

type TraceDatum = {
  id:string,
  startTime:Date,
  duration:number,
  spanCount:number,
  service: string,
  operation: string,
  body: Trace,
}

const clamp = (num:number, min:number, max:number)=>(Math.min(Math.max(num, min), max));
// NOTE : log10(v**2) gives an idea of the magniture of the count.
// floored, scaled 4x, clamped between 2 and 20 pixels
const countToSize = (v:number):number => (clamp(Math.floor(Math.log10(v**2)*4), 2, 20));

function TracesTimeline(props: {data: TraceDatum[]}) {
  const elRef = useRef<HTMLDivElement>(null)
  const size = useSize(elRef);
  const theme = useTheme();

  const chartProps = {
    accessors:{
      // XXX : see https://github.com/airbnb/visx/issues/1247
      xAccessor:(d:TraceDatum)=>(d?.startTime.getTime()),
      yAccessor:(d:TraceDatum)=>(d?.duration),
      size:(d:TraceDatum)=>(d && countToSize(d.spanCount)),
      color:()=>(theme.palette.primary.main),
    },
    ...size
  };
  return (
    <Box sx={{minWidth:0, height:'200px', flexShrink:1, flexGrow:0}} ref={elRef}>
      {props.data && <TraceTimelineChart data={props.data} {...chartProps}></TraceTimelineChart>}
    </Box>
  )
}

function TraceSnippet(props:{trace:TraceDatum, refDuration?:number}) {
  const [durationPercent, setDurationPercent] = useState(0)

  useEffect(()=>{
    if (props.refDuration) {
      setDurationPercent((props.trace.duration/props.refDuration)*100)
    }
  }, [props.trace, props.refDuration])
  
  return (
    <Link to={`/traces/${props.trace.id}`} style={{textDecoration:"none"}}>
      <Card variant="outlined" sx={{overflowY:"visible", background:"#eee", mb:1}}>
        <Box sx={{ position:"relative", padding:1, display:"flex", flexDirection:"row", gap:1, alignItems:"baseline"}}>
          <Box sx={{flexShrink:1, flexGrow:1, minWidth:0, overflow:"hidden", textOverflow:"ellipsis"}}>
            {/* {dayjs(props.trace.date).fromNow()} */}
            {props.trace.service}:{props.trace.operation}
            <Typography component="span" sx={{fontSize:"0.6rem", color:"#888", lineHeight:"0.6rem"}}> {props.trace.id}</Typography>
          </Box>
          <Chip label={`${props.trace.spanCount} spans / ${formatDuration(props.trace.duration)} `} size="small"/>
        </Box>
        {props.refDuration && <LinearProgress variant="determinate" value={durationPercent}/>}
      </Card>
    </Link>
  )
}


export function TracesView() {
  const jaegerClient = useJaegerAPIClient();
  const [traces, setTraces] = useState<TraceDatum[]>([])
  const [maxDuration, setMaxDuration] = useState(0);

  const fetchTraces = useCallback((traceParams:TraceRequestParams)=>{
    jaegerClient.getTraces(traceParams)
      .then((rawTraces:Trace[])=>{
        const data = rawTraces.map((trace)=>{
          const longestSpan = trace.spans.sort((t1, t2)=>t2.duration-t1.duration)[0]!;
          return {
            id: trace.traceID,
            spanCount: trace.spans.length,
            // Times are returned in ns, convert to µs
            startTime: longestSpan.startTime,
            duration: longestSpan.duration,
            // Keep the source info
            body:trace,
            service: trace.processes[longestSpan.processID]!.service,
            operation: longestSpan.operation,
          }
        }).sort((t1,t2)=>(t2.startTime.getTime() - t1.startTime.getTime()));
        setTraces(data as TraceDatum[]);
      })
  },[jaegerClient])

  useEffect(()=>{
    const durations: number[] = traces.map((trace)=>(trace.duration as number));
    setMaxDuration(Math.max(...durations));

  }, [traces])

  return(
    <ViewUnderAppBarBox sx={{ flexDirection: 'row'}}>
      <TraceSearchSidebar jaegerClient={jaegerClient}
                          onFetchTraces={fetchTraces}>
      </TraceSearchSidebar>
      <FullBoxContainer sx={{padding:0, flexGrow:0, minWidth:0}} >
        <TracesTimeline data={traces}/>
        <Divider/>
        <FullBoxContainer sx={{flexGrow:1, overflow:'scroll'}}>
          <Box sx={{minHeight:0}}>
            { traces && traces.map((trace)=>(
              <React.Fragment key={`trace-snippet-${trace.id}`}>
                <TraceSnippet key={`trace-snippet-${trace.id}`} trace={trace} refDuration={maxDuration}/>
                </React.Fragment>
            ))}
          </Box>
        </FullBoxContainer>
      </FullBoxContainer>
    </ViewUnderAppBarBox>
  )
}