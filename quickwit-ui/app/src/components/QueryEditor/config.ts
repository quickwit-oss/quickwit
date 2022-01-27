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

import { FieldMapping } from "../../utils/models";

export const LANGUAGE_CONFIG = {
  comments: {
    lineComment: "//",
    blockComment: ["/*", "*/"],
  },
  brackets: [
    ["{", "}"],
    ["[", "]"],
    ["(", ")"],
  ],
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

export function LanguageFeatures(): any {
  return {
    defaultToken: "",
    brackets: [
      { open: "[", close: "]", token: "delimiter.square" },
      { open: "(", close: ")", token: "delimiter.parenthesis" },
    ],
    keywords: [
      'AND', 'OR',
    ],
    tokenizer: {
      root: [
        [
          /\w+/,
          { 
            cases: {   
              '@keywords': 'directive', 
              '@default': 'identifier'
            }
          }
        ]
      ],
    },
  };
}

export const createIndexCompletionProvider = (fields: FieldMapping[]) => {
  const completionProvider = {
    provideCompletionItems(model: any, position: any) {
      const word = model.getWordUntilPosition(position)

      const range = {
        startLineNumber: position.lineNumber,
        endLineNumber: position.lineNumber,
        startColumn: word.startColumn,
        endColumn: word.endColumn,
      }

      const fieldSuggestions = fields.map(field => {
        return {
          label: field.name,
          kind: 3,
          insertText: field.name + ':',
          range: range,
        }
      });

      return {
        suggestions: fieldSuggestions.concat([
          {
            label: 'OR',
            kind: 11,
            insertText: 'OR ',
            range: range,
          },
          {
            label: 'AND',
            kind: 11,
            insertText: 'AND ',
            range: range,
          }
        ]),
      }
    },
  }

  return completionProvider
}