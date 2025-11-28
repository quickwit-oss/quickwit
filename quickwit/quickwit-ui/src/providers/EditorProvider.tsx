// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import * as monacoEditor from "monaco-editor/esm/vs/editor/editor.api";
import {
  createContext,
  MutableRefObject,
  PropsWithChildren,
  useContext,
  useRef,
} from "react";

type ContextProps = {
  editorRef: MutableRefObject<unknown | null> | null;
  monacoRef: MutableRefObject<typeof monacoEditor | null> | null;
};

const defaultValues = {
  editorRef: null,
  monacoRef: null,
};

const EditorContext = createContext<ContextProps>(defaultValues);

export const EditorProvider = ({ children }: PropsWithChildren<unknown>) => {
  const editorRef = useRef<unknown | null>(null);
  const monacoRef = useRef<typeof monacoEditor | null>(null);

  return (
    <EditorContext.Provider
      value={{
        editorRef,
        monacoRef,
      }}
    >
      {children}
    </EditorContext.Provider>
  );
};

export const useEditor = () => {
  return useContext(EditorContext);
};
