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

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use quickwit_query::CodeTokenizer;
use tantivy::tokenizer::{RegexTokenizer, TextAnalyzer, Token, TokenStream};

// A random ascii string of length 100 chars.
static CODE_TEXT: &str = r#"
# Camel case variables
firstName = "John"
lastName = "Doe"
ageOfPerson = 30
isEmployed = True

# Snake case variables
first_name = "Jane"
last_name = "Smith"
age_of_person = 25
is_employed = False

# Mixed case variables
fullName = firstName + " " + lastName
isPersonEmployed = isEmployed and is_employed

# Code logic
if isEmployed and is_employed:
    print(f"{firstName} {first_name} is currently employed.")
else:
    print(f"{lastName} {last_name} is not employed at the moment.")

totalAge = ageOfPerson + age_of_person
print(f"The combined age is: {totalAge}")

# Longer word examples
longCamelCaseWord = "LongCamelCase"
longSnakeCaseWord = "long_snake_case"
mixedCaseWord = "ThisIsAMixedCaseWord"
longCamelCaseWord = "LongCamelCase"
longSnakeCaseWord = "long_snake_case"
mixedCaseWord = "ThisIsAMixedCaseWord"

# Words with consecutive uppercase letters
WORDWITHConsecutiveUppercase1 = "1"
WORDWITHCONSECUTIVEUppercase2 = "2"
WORDWITHCONSECUTIVEUPPERCASE2 = "3"
=======
use quickwit_query::create_default_quickwit_tokenizer_manager;
use tantivy::tokenizer::{TextAnalyzer, Token, TokenStream};

// A random ascii string of length 100 chars.
const ASCII_SHORT: &str = "It is a long established fact";
static ASCII_LONG: &str = r#"It is a long established fact that a reader will be distracted by the readable content of a
     page when looking at its layout. The point of using Lorem Ipsum is that it has a
     more-or-less normal distribution of letters, as opposed to using 'Content here, content
     here', making it look like readable English. Many desktop publishing packages and web page
     editors now use Lorem Ipsum as their default model text, and a search for 'lorem ipsum' will
     uncover many web sites still in their infancy. Various versions have evolved over the years,
     sometimes by accident, sometimes on purpose (injected humour and the like)."#;
const JPN_SHORT: &str = "日本ごです。　とても素敵な言葉ですね";
const JPN_LONG: &str = r#"日本ごです。　和名の由来は、
                         太陽の動きにつれてその方向を追うように花が回るといわれたことから。
                         ただしこの動きは生長に伴うものであるため、
                         実際に太陽を追って動くのは生長が盛んな若い時期だけである。
                         若いヒマワリの茎の上部の葉は太陽に正対になるように動き、
                         朝には東を向いていたのが夕方には西を向く。日没後はまもなく起きあがり、
                         夜明け前にはふたたび東に向く。この運動はつぼみを付ける頃まで続くが、
                         つぼみが大きくなり花が開く素敵な言葉ですね."#;
const CMN_SHORT: &str = "滚滚长江东逝水，浪花淘尽英雄。";
const CMN_LONG: &str = r#"滚滚长江东逝水，浪花淘尽英雄。是非成败转头空，青山依旧在，几度夕阳红。
                         白发渔樵江渚上，惯看秋月春风。一壶浊酒喜相逢，古今多少事，都付笑谈中。
                         是非成败转头空，青山依旧在，惯看秋月春风。一壶浊酒喜相逢，古今多少事，
                         滚滚长江东逝水，浪花淘尽英雄。 几度夕阳红。白发渔樵江渚上，都付笑谈中。"#;
const KOR_SHORT: &str = "안녕하세요. 반갑습니다.";
const KOR_LONG: &str = r#"
포근히 내려오는 눈밭속에서는
낯이 붉은 處女아이들도 깃들이어 오는 소리…
울고
웃고
수구리고
새파라니 얼어서
運命들이 모두다 안끼어 드는 소리…
큰놈에겐 큰 눈물자국, 작은놈에겐 작은 웃음 흔적
큰이얘기 작은이얘기들이 오부록이 도란 그리며 안끼어 오는 소리
끊임없이 내리는 눈발 속에서는
山도 山도 靑山도 안끼어 드는 소리
>>>>>>> ec00bafdc (Add tokenizers bench.)
"#;

fn process_tokens(analyzer: &mut TextAnalyzer, text: &str) -> Vec<Token> {
    let mut token_stream = analyzer.token_stream(text);
    let mut tokens: Vec<Token> = vec![];
    token_stream.process(&mut |token: &Token| tokens.push(token.clone()));
    tokens
}
