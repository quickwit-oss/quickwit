import { BaseApiClient } from "./base";

import { z } from "zod";

export type ServiceList = string[];
export type ServiceOperationsList = string[];
export interface TraceRequestParams {
  service: string,
  operation?: string,
  tags?: string,
  start?: number,
  end?: number,
  limit?: number,
}

//  Validators

const HexString = z.string().regex(/^[0-9A-Fa-f]*$/).brand<"HexString">();

const Span = z.object({
  spanID: HexString,
  processID: z.string(),
  references: z.array(z.object({
    traceID: HexString,
    spanID: HexString,
    refType: z.string(),
  })),
  operationName: z.string(),
  startTime: z.number().transform((v)=>Math.floor(v/1000)).pipe(z.coerce.date()),
  duration: z.number().transform((v)=>Math.ceil(v/1000)),
}).transform(({operationName, ...rest})=>({
  operation:operationName,
  ...rest
}))
export type Span = z.infer<typeof Span>

const Trace = z.object({
  traceID: HexString,
  // XXX : naive sorting 
  spans: z.array(Span),
  processes: z.object({}).catchall(
    z.object({ serviceName: z.string() }).transform(({serviceName})=>({service:serviceName}))
  )
})
export type Trace = z.infer<typeof Trace>


const JaegerAPIResponse = z.object({
  data:z.union([z.array(z.string()), z.array(Trace), Trace])
})
export type JaegerAPIResponse = z.infer<typeof JaegerAPIResponse>


export class JaegerAPIClient extends BaseApiClient {
  private readonly _source: string
  constructor(source:string, host?:string, apiRoot?:string) {
    super(host, apiRoot)
    this._source = source;
  }

  get baseURL(): string {
    return `${super.baseURL}/${ this._source}/jaeger/api/`;
  }

  // NOTE : client boilerplate, should be abstracted out
  // Endpoint handlers

  async getServices(): Promise<ServiceList> {
    return await this.fetch(`./services`, this.getDefaultGetRequestParams())
    .then(
      (response)=>{return (response as JaegerAPIResponse).data as ServiceList}
    )
  }

  async getServiceOperations(service:string): Promise<ServiceOperationsList> {
    return await this.fetch(`./services/${service}/operations`, this.getDefaultGetRequestParams())
    .then(
      (response)=>{return (response as JaegerAPIResponse).data as ServiceOperationsList}
    )
  }

  async getTraces(traceParams:TraceRequestParams): Promise<Trace[]> {
    const url = this.getURL(`./traces?`);
    if (traceParams.service) url.searchParams.append("service", traceParams.service)
    if (traceParams.operation) url.searchParams.append("operation", traceParams.operation)
    if (traceParams.start) url.searchParams.append("start", traceParams.start.toString())
    if (traceParams.end) url.searchParams.append("end", traceParams.end.toString())
    if (traceParams.tags) url.searchParams.append("tags", traceParams.tags.toString())
    if (traceParams.limit) url.searchParams.append("limit", traceParams.limit.toString())

    return await this.fetch(url, this.getDefaultGetRequestParams())
    .then(
      (response)=>{return JaegerAPIResponse.parse((response as JaegerAPIResponse)).data as Trace[]}
    )
  }

  async getTrace(traceID:string): Promise<Trace> {
    return await this.fetch(`./traces/${traceID}`, this.getDefaultGetRequestParams())
    .then((response)=>((response as JaegerAPIResponse).data) as Trace)

  }
}
