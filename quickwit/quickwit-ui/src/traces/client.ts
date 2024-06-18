import { useMemo } from "react";
import { JaegerAPIClient } from "../services/jaeger_api.client";
 
export function useJaegerAPIClient(): JaegerAPIClient {
  const jaegerClient = useMemo(()=>new JaegerAPIClient('otel-traces-v0_6'), []);
  return jaegerClient;
}
