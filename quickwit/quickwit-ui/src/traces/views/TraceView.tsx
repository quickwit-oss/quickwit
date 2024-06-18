import { useCallback, useEffect, useState } from "react";
import { useParams } from "react-router-dom"

import { ViewUnderAppBarBox } from "@/components/LayoutUtils";
import { JsonEditor } from "@/components/JsonEditor";
import { Trace } from "@/services/jaeger_api.client";

import { useJaegerAPIClient } from '../client';

export function TraceView(){
  const {traceId} = useParams();
  const jaegerClient = useJaegerAPIClient();
  const [trace, setTrace] = useState<Trace>()
  const fetchTrace = useCallback((traceID)=>{
      jaegerClient.getTrace(traceID).then(
        (fetchedTrace)=>{ setTrace(fetchedTrace); }
      )
  }, [jaegerClient])

  useEffect(()=>{
    if (traceId) fetchTrace(traceId);
  }, [fetchTrace, traceId])

  return (
    <ViewUnderAppBarBox>
      <JsonEditor content={trace} resizeOnMount></JsonEditor>
    </ViewUnderAppBarBox>
  )
}
