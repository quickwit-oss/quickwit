import { GlyphDot } from '@visx/glyph';
import { Orientation } from '@visx/axis';
import { XYChart, Axis, Grid, GlyphSeries } from '@visx/xychart';
import { GlyphProps } from '@visx/xychart/lib/types';
import { formatDuration } from '@/utils/date';

export type ChartProps<T> = {
  data: T[],
  accessors:{
    xAccessor: (d:T)=>number|Date,
    yAccessor: (d:T)=>number,
    size: (d:T)=>number,
    color: (d?:T)=>string,
  }
  width: number,
  height: number,
};


export default function TraceTimelineChart<T extends object>(props : ChartProps<T>) {
  const glyphProps = {
    dataKey:"L1",
    xAccessor: props.accessors.xAccessor,
    yAccessor: props.accessors.yAccessor,
    colorAccessor: props.accessors.color, 
    size: props.accessors.size, 
    renderGlyph: ({x,y,size,color}:GlyphProps<T>)=>(
      <GlyphDot left={x} top={y} r={size} fill={color} stroke={color} fillOpacity={0.5} strokeOpacity={0.8} />
    )
  }

  return (
      <XYChart
          margin={{top:25, right:25, bottom:25, left:55}}
          width={props.width} height={props.height}
          xScale={{type:'time'}} yScale={{type:'linear'}}>
        <Axis key={`scale-x`} orientation={Orientation.bottom} />
        <Axis key={`scale-y`} orientation={Orientation.left}
              numTicks={3}
              tickFormat={(v:number)=>(formatDuration(v))} />
        <Grid rows={false} strokeDasharray='3 3'/>
        <GlyphSeries {...glyphProps} data={props.data}/>
      </XYChart>
  );
}
