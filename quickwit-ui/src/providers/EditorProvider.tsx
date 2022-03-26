import { Monaco } from "@monaco-editor/react"
import { createContext, MutableRefObject, PropsWithChildren, useContext, useRef } from "react"

type ContextProps = {
  editorRef: MutableRefObject<any | null> | null
  monacoRef: MutableRefObject<Monaco | null> | null
}

const defaultValues = {
  editorRef: null,
  monacoRef: null,
}

const EditorContext = createContext<ContextProps>(defaultValues);

export const EditorProvider = ({ children }: PropsWithChildren<{}>) => {
  const editorRef = useRef<any | null>(null)
  const monacoRef = useRef<Monaco | null>(null)

  return (
    <EditorContext.Provider
      value={{
        editorRef,
        monacoRef,
      }}
    >
      {children}
    </EditorContext.Provider>
  )
}

export const useEditor = () => {
  return useContext(EditorContext)
}