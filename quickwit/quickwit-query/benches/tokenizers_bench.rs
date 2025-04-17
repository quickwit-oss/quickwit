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

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
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
    let mut tokens: Vec<Token> = Vec::new();
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
