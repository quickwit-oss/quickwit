// Copyright (C) 2021 Quickwit, Inc.
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

import { Box } from '@mui/system';
import { useEffect, useRef } from 'react';
import Editor, { useMonaco } from "@monaco-editor/react";
import { LANGUAGE_CONFIG, LanguageFeatures, createIndexCompletionProvider } from './config';
import { SearchComponentProps } from '../../utils/SearchComponentProps';
import { get_all_fields } from '../../utils/models';
import { QUICKWIT_BLUE } from '../../utils/theme';

export function QueryEditor(props: SearchComponentProps) {
  const monaco = useMonaco();
  const editorRef = useRef(null);
  const runSearchRef = useRef(props.runSearch);

  function handleEditorDidMount(editor: any, monaco: any) {
    editorRef.current = editor; 
    editor.addAction({
      id: 'EXECUTE',
      label: "Execute command",
      keybindings: [
        monaco.KeyCode.F9,
        monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter,
      ],
      run: () => {
        runSearchRef.current();
      },
    })
  }

  useEffect(() => {
    if (monaco && props.searchRequest.indexId !== '' && props.indexMetadata !== null) {
      // let's update the language
      const languageId = props.searchRequest.indexId + '-query-language';
      if (!monaco.languages.getLanguages().some(({ id }: {id :string }) => id === languageId)) {
        console.log('register language', languageId);
        monaco.languages.register({'id': languageId});
        monaco.languages.setMonarchTokensProvider(languageId, LanguageFeatures())
        monaco.languages.setLanguageConfiguration(
          languageId,
          LANGUAGE_CONFIG,
        );
        if (props.indexMetadata != null) {
          const fields = get_all_fields(props.indexMetadata.doc_mapping);
          monaco.languages.registerCompletionItemProvider(languageId, createIndexCompletionProvider(fields));
        }
      }
    }
  }, [monaco, props.searchRequest, props.indexMetadata]);

  useEffect(() => {
    if (monaco) {
      runSearchRef.current = props.runSearch;
    }
  }, [monaco, props.runSearch]);

  function handleEditorWillMount(monaco: any) {
    monaco.editor.defineTheme('quickwit-light', {
      base: 'vs',
      inherit: true,
      rules: [
        { token: 'comment', foreground: '#1F232A', fontStyle: 'italic' },
        { token: 'keyword', foreground: QUICKWIT_BLUE }
      ],
      colors: {
        'editor.comment.foreground': '#CBD1DE',
        'editor.foreground': '#000000',
        'editor.background': '#F8F9FB',
        'editorLineNumber.foreground': 'black',
        'editor.lineHighlightBackground': '#DFE0E1',
      },
    });
  }
  const defaultValue = `// Select an index and type your query. Example: field_name:"phrase query"
`;
  return (
    <Box sx={{ height: '100px', py: 1}} >
      <Editor
        beforeMount={handleEditorWillMount}
        onMount={handleEditorDidMount}
        language={props.searchRequest.indexId + '-query-language'}
        value={defaultValue}
        options={{
          fontFamily: 'monospace',
          minimap: {
            enabled: false,
          },
          renderLineHighlight: "gutter",
          fontSize: 14,
          fixedOverflowWidgets: true,
          scrollBeyondLastLine: false,
      }}
      theme='quickwit-light'
      />
    </Box>
  );
}
