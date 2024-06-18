import { Route } from "react-router-dom";
import { TracesView } from "./TracesView";
import { TraceView }  from "./TraceView";

export const routes = (
    <Route path="traces">
        <Route index element={<TracesView/>} />
        <Route path=":traceId" element={<TraceView/>} />
    </Route>
);