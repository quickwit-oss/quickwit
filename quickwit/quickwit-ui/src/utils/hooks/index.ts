import { useState, useLayoutEffect } from "react";

export function useSize(elRef:React.MutableRefObject<HTMLElement|null>) {
  const [width, setWidth] = useState<number>(0);
  const [height, setHeight] = useState<number>(0);

  useLayoutEffect(()=>{
    const updateSize = ()=>{
      if (elRef.current){
        setWidth(elRef.current?.offsetWidth);
        setHeight(elRef.current?.offsetHeight);
      }
    };
    window.addEventListener('resize', updateSize);
    updateSize();
    return () => window.removeEventListener('resize', updateSize);
  }, [])

  return { width, height }
}