// Copyright (C) 2022 Quickwit, Inc.
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

import * as monacoEditor from 'monaco-editor/esm/vs/editor/editor.api';
import { createContext, MutableRefObject, PropsWithChildren, useContext, useRef } from "react"

type ContextProps = {
  editorRef: MutableRefObject<any | null> | null
  monacoRef: MutableRefObject<typeof monacoEditor | null> | null
}

const defaultValues = {
  editorRef: null,
  monacoRef: null,
}

const EditorContext = createContext<ContextProps>(defaultValues);

export const EditorProvider = ({ children }: PropsWithChildren<{}>) => {
  const editorRef = useRef<any | null>(null)
  const monacoRef = useRef<typeof monacoEditor | null>(null)

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
