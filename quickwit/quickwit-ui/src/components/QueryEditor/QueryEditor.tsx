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

import { Editor } from "@monaco-editor/react";
import { Box } from "@mui/material";
import * as monacoEditor from "monaco-editor/esm/vs/editor/editor.api";
import React, { useEffect, useRef, useState } from "react";
import { SearchComponentProps } from "../../utils/SearchComponentProps";
import { EDITOR_THEME } from "../../utils/theme";
import {
  createIndexCompletionProvider,
  LANGUAGE_CONFIG,
  LanguageFeatures,
} from "./config";

const QUICKWIT_EDITOR_THEME_ID = "quickwit-light";

function getLanguageId(indexId: string | null): string {
  if (indexId === null) {
    return "";
  }
  return `${indexId}-query-language`;
}

export function QueryEditor(props: SearchComponentProps) {
  const monacoRef = useRef<null | typeof monacoEditor>(null);
  const [languageId, setLanguageId] = useState<string>("");
  const runSearchRef = useRef(props.runSearch);
  const searchRequestRef = useRef(props.searchRequest);
  const defaultValue =
    props.searchRequest.query === null
      ? `// Select an index and type your query. Example: field_name:"phrase query"`
      : props.searchRequest.query;
  let resize: () => void;

  function handleEditorDidMount(editor: any, monaco: any) {
    monacoRef.current = monaco;
    editor.addAction({
      id: "SEARCH",
      label: "Run search",
      keybindings: [
        monaco.KeyCode.F9,
        monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter,
      ],
      run: () => {
        runSearchRef.current(searchRequestRef.current);
      },
    });
    resize = () => {
      editor.layout({
        width: Math.max(window.innerWidth - (260 + 180 + 2 * 24), 200),
        height: 84,
      });
    };
    window.addEventListener("resize", resize);
  }

  React.useEffect(() => {
    return () => window.removeEventListener("resize", resize);
  });

  useEffect(() => {
    const updatedLanguageId = getLanguageId(props.searchRequest.indexId);
    if (
      monacoRef.current !== null &&
      updatedLanguageId !== "" &&
      props.index !== null
    ) {
      const monaco = monacoRef.current;
      if (
        !monaco.languages
          .getLanguages()
          .some(({ id }: { id: string }) => id === updatedLanguageId)
      ) {
        console.log("register language", updatedLanguageId);
        monaco.languages.register({ id: updatedLanguageId });
        monaco.languages.setMonarchTokensProvider(
          updatedLanguageId,
          LanguageFeatures(),
        );
        if (props.index != null) {
          monaco.languages.registerCompletionItemProvider(
            updatedLanguageId,
            createIndexCompletionProvider(props.index.metadata),
          );
          monaco.languages.setLanguageConfiguration(
            updatedLanguageId,
            LANGUAGE_CONFIG,
          );
        }
      }
      setLanguageId(updatedLanguageId);
    }
  }, [monacoRef, props.index]);

  useEffect(() => {
    if (monacoRef.current !== null) {
      runSearchRef.current = props.runSearch;
    }
  }, [monacoRef, props.runSearch]);

  function handleEditorChange(value: any) {
    const updatedSearchRequest = Object.assign({}, props.searchRequest, {
      query: value,
    });
    searchRequestRef.current = updatedSearchRequest;
    props.onSearchRequestUpdate(updatedSearchRequest);
  }

  function handleEditorWillMount(monaco: any) {
    monaco.editor.defineTheme(QUICKWIT_EDITOR_THEME_ID, EDITOR_THEME);
  }

  return (
    <Box sx={{ height: "100px", py: 1 }}>
      <Editor
        beforeMount={handleEditorWillMount}
        onMount={handleEditorDidMount}
        onChange={handleEditorChange}
        language={languageId}
        value={defaultValue}
        options={{
          fontFamily: "monospace",
          minimap: {
            enabled: false,
          },
          renderLineHighlight: "gutter",
          fontSize: 14,
          fixedOverflowWidgets: true,
          scrollBeyondLastLine: false,
        }}
        theme={QUICKWIT_EDITOR_THEME_ID}
      />
    </Box>
  );
}
