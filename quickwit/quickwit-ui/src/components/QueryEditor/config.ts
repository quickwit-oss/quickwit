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

import { getAllFields, IndexMetadata } from "../../utils/models";

export enum CompletionItemKind {
  Field = 3,
  Operator = 11,
}

const BRACES: [string, string] = ["{", "}"];
const BRACKETS: [string, string] = ["[", "]"];
const PARENTHESES: [string, string] = ["(", ")"];

export const LANGUAGE_CONFIG = {
  comments: {
    lineComment: "//",
  },
  brackets: [BRACES, BRACKETS, PARENTHESES],
  autoClosingPairs: [
    { open: "{", close: "}" },
    { open: "[", close: "]" },
    { open: "(", close: ")" },
    { open: '"', close: '"' },
    { open: "'", close: "'" },
  ],
  surroundingPairs: [
    { open: "{", close: "}" },
    { open: "[", close: "]" },
    { open: "(", close: ")" },
    { open: '"', close: '"' },
    { open: "'", close: "'" },
  ],
};

// TODO: clean language features as I (fmassot) did not dig into it yet.
export function LanguageFeatures(): any {
  return {
    defaultToken: "invalid",
    //wordDefinition: /(-?\d*\.\d\w*)|([^\`\~\!\#\%\^\&\*\(\)\-\=\+\[\{\]\}\\\|\;\:\'\"\,\.\<\>\/\?\s]+)/g,
    operators: ["+", "-"],
    brackets: [{ open: "(", close: ")", token: "delimiter.parenthesis" }],
    keywords: ["AND", "OR"],
    symbols: /[=><!~?:&|+\-*/^%]+/,
    escapes:
      /\\(?:[abfnrtv\\"']|x[0-9A-Fa-f]{1,4}|u[0-9A-Fa-f]{4}|U[0-9A-Fa-f]{8})/,
    tokenizer: {
      root: [
        // identifiers and keywords
        [
          /[a-z_$][\w$]*/,
          {
            cases: {
              "@keywords": "keyword",
              "@default": "identifier",
            },
          },
        ],
        [/[A-Z][\w$]*/, "type.identifier"], // to show class names nicely

        // whitespace
        { include: "@whitespace" },

        // delimiters and operators
        [/[{}()[]]/, "@brackets"],
        [/[<>](?!@symbols)/, "@brackets"],
        [/@symbols/, { cases: { "@operators": "operator", "@default": "" } }],

        // @ annotations.
        // As an example, we emit a debugging log message on these tokens.
        // Note: message are suppressed during the first load -- change some lines to see them.
        [
          /@\s*[a-zA-Z_$][\w$]*/,
          { token: "annotation", log: "annotation token: $0" },
        ],

        // numbers
        [/\d*\.\d+([eE][-+]?\d+)?/, "number.float"],
        [/0[xX][0-9a-fA-F]+/, "number.hex"],
        [/\d+/, "number"],

        // delimiter: after number because of .\d floats
        [/[;,.]/, "delimiter"],

        // strings
        [/"([^"\\]|\\.)*$/, "string.invalid"], // non-terminated string
        [/"/, { token: "string.quote", bracket: "@open", next: "@string" }],

        // characters
        [/'[^\\']'/, "string"],
        [/(')(@escapes)(')/, ["string", "string.escape", "string"]],
        [/'/, "string.invalid"],
      ],
      comment: [
        [/[^/*]+/, "comment"],
        [/\/\*/, "comment", "@push"], // nested comment
        ["\\*/", "comment", "@pop"],
        [/[/*]/, "comment"],
      ],
      string: [
        [/[^\\"]+/, "string"],
        [/@escapes/, "string.escape"],
        [/\\./, "string.escape.invalid"],
        [/"/, { token: "string.quote", bracket: "@close", next: "@pop" }],
      ],

      whitespace: [
        [/[ \t\r\n]+/, "white"],
        [/\/\*/, "comment", "@comment"],
        [/\/\/.*$/, "comment"],
      ],
    },
  };
}

export const createIndexCompletionProvider = (indexMetadata: IndexMetadata) => {
  const fields = getAllFields(
    indexMetadata.index_config.doc_mapping.field_mappings,
  );
  const completionProvider = {
    provideCompletionItems(model: any, position: any) {
      const word = model.getWordUntilPosition(position);

      const range = {
        startLineNumber: position.lineNumber,
        endLineNumber: position.lineNumber,
        startColumn: word.startColumn,
        endColumn: word.endColumn,
      };

      // We want to auto complete all fields except timestamp that is handled with `TimeRangeSelect` component.
      const fieldSuggestions = fields
        .filter(
          (field) =>
            field.json_path !==
            indexMetadata.index_config.doc_mapping.timestamp_field,
        )
        .map((field) => {
          return {
            label: field.json_path,
            kind: CompletionItemKind.Field,
            insertText:
              field.field_mapping.type === "json"
                ? field.json_path + "."
                : field.json_path + ":",
            range: range,
          };
        });

      return {
        suggestions: fieldSuggestions.concat([
          {
            label: "OR",
            kind: CompletionItemKind.Operator,
            insertText: "OR ",
            range: range,
          },
          {
            label: "AND",
            kind: CompletionItemKind.Operator,
            insertText: "AND ",
            range: range,
          },
        ]),
      };
    },
  };

  return completionProvider;
};

export const setErrorMarker = (
  monaco: any,
  editor: any,
  startlineNumber: number,
  startColumnNumber: number,
  message: string,
) => {
  const model = editor.getModel();

  if (model) {
    monaco.editor.setModelMarkers(model, "QuestDBLanguageName", [
      {
        message,
        severity: monaco.MarkerSeverity.Error,
        startLineNumber: startlineNumber,
        endLineNumber: startlineNumber,
        startColumn: startColumnNumber,
        endColumn: startColumnNumber,
      },
    ]);
  }
};
