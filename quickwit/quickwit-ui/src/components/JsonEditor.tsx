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

import { BeforeMount, Editor, OnMount } from "@monaco-editor/react";
import { useCallback } from "react";
import { EDITOR_THEME } from "../utils/theme";

export function JsonEditor({
  content,
  resizeOnMount,
}: {
  content: unknown;
  resizeOnMount: boolean;
}) {
  // Setting editor height based on lines height and count to stretch and fit its content.
  const onMount: OnMount = useCallback(
    (editor) => {
      if (!resizeOnMount) {
        return;
      }
      const editorElement = editor.getDomNode();

      if (!editorElement) {
        return;
      }

      // Weirdly enough, we have to wait a few ms to get the right height
      // from `editor.getContentHeight()`. If not, we sometimes end up with
      // a height > 7000px... and I don't know why.
      setTimeout(() => {
        const height = Math.min(800, editor.getContentHeight());
        editorElement.style.height = `${height}px`;
        editor.layout();
      }, 10);
    },
    [resizeOnMount],
  );

  const beforeMount: BeforeMount = (monaco) => {
    monaco.editor.defineTheme("quickwit-light", EDITOR_THEME);
  };

  return (
    <Editor
      language="json"
      value={JSON.stringify(content, null, 2)}
      beforeMount={beforeMount}
      onMount={onMount}
      options={{
        readOnly: true,
        fontFamily: "monospace",
        overviewRulerBorder: false,
        overviewRulerLanes: 0,
        minimap: {
          enabled: false,
        },
        scrollbar: {
          alwaysConsumeMouseWheel: false,
        },
        renderLineHighlight: "gutter",
        fontSize: 12,
        fixedOverflowWidgets: true,
        scrollBeyondLastLine: false,
        automaticLayout: true,
        wordWrap: "on",
        wrappingIndent: "deepIndent",
      }}
      theme="quickwit-light"
    />
  );
}
