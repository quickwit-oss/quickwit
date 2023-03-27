// Copyright (C) 2023 Quickwit, Inc.
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

use tantivy::tokenizer::Tokenizer;

#[derive(Clone)]
struct MultiLanguageTokenizer {
    chinese_tokenizer: ChineseTokenizer,
    japanese_tokenizer: JapaneseTokenizer,
}

impl MultiLanguageTokenizer {
    pub new() -> Self {
        let japanese_tokenizer = LinderaTokenizer::new(dictionary, None, Mode::Normal);

        Self {
            chinese_tokenizer: ChineseTokenizer::new(),
            japanese_tokenizer: JapaneseTokenizer::new(),
        }
    }
}

impl Tokenizer for MultiLanguageTokenizer {
    type TokenStream<'a> = ChineseTokenStream<'a>;
   
}
