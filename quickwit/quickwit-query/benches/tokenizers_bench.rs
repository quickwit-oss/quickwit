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
"#;

fn process_tokens(analyzer: &mut TextAnalyzer, text: &str) -> Vec<Token> {
    let mut token_stream = analyzer.token_stream(text);
    let mut tokens: Vec<Token> = vec![];
    token_stream.process(&mut |token: &Token| tokens.push(token.clone()));
    tokens
}

pub fn tokenizers_throughput_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("code_tokenizer");
    let mut regex_tokenizer = TextAnalyzer::from(
        RegexTokenizer::new("(\\p{Ll}+|\\p{Lu}\\p{Ll}+|\\p{Lu}+|\\d+)").unwrap(),
    );
    let mut code_tokenizer = TextAnalyzer::from(CodeTokenizer::default());

    group
        .throughput(Throughput::Bytes(CODE_TEXT.len() as u64))
        .bench_with_input("regex-tokenize", CODE_TEXT, |b, text| {
            b.iter(|| process_tokens(&mut regex_tokenizer, black_box(text)));
        });
    group
        .throughput(Throughput::Bytes(CODE_TEXT.len() as u64))
        .bench_with_input("code-tokenize", CODE_TEXT, |b, text| {
            b.iter(|| process_tokens(&mut code_tokenizer, black_box(text)));
        });
}

criterion_group!(
    tokenizers_throughput_benches,
    tokenizers_throughput_benchmark
);
criterion_main!(tokenizers_throughput_benches);
