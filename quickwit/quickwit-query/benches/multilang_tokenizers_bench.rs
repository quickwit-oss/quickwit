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
"#;

fn process_tokens(analyzer: &mut TextAnalyzer, text: &str) -> Vec<Token> {
    let mut token_stream = analyzer.token_stream(text);
    let mut tokens: Vec<Token> = Vec::new();
    token_stream.process(&mut |token: &Token| tokens.push(token.clone()));
    tokens
}

pub fn tokenizers_throughput_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("multilang");
    let tokenizer_manager = create_default_quickwit_tokenizer_manager();
    let mut default_tokenizer = tokenizer_manager.get_tokenizer("default").unwrap();
    let mut multilang_tokenizer = tokenizer_manager.get_tokenizer("multilang").unwrap();
    let mut chinese_tokenizer = tokenizer_manager
        .get_tokenizer("chinese_compatible")
        .unwrap();

    group
        .throughput(Throughput::Bytes(ASCII_SHORT.len() as u64))
        .bench_with_input("default-tokenize-short", ASCII_SHORT, |b, text| {
            b.iter(|| process_tokens(&mut default_tokenizer, black_box(text)));
        });
    group
        .throughput(Throughput::Bytes(ASCII_LONG.len() as u64))
        .bench_with_input("default-tokenize-long", ASCII_LONG, |b, text| {
            b.iter(|| process_tokens(&mut default_tokenizer, black_box(text)));
        });
    group
        .throughput(Throughput::Bytes(ASCII_SHORT.len() as u64))
        .bench_with_input("multilang-eng-tokenize-short", ASCII_SHORT, |b, text| {
            b.iter(|| process_tokens(&mut multilang_tokenizer, black_box(text)));
        });
    group
        .throughput(Throughput::Bytes(ASCII_LONG.len() as u64))
        .bench_with_input("multilang-eng-tokenize-long", ASCII_LONG, |b, text| {
            b.iter(|| process_tokens(&mut multilang_tokenizer, black_box(text)));
        });
    let short_with_prefix = "ENG:".to_string() + ASCII_SHORT;
    group
        .throughput(Throughput::Bytes(ASCII_SHORT.len() as u64))
        .bench_with_input(
            "multilang-tokenize-short-with-prefix",
            &short_with_prefix,
            |b, text| {
                b.iter(|| process_tokens(&mut multilang_tokenizer, black_box(text)));
            },
        );
    let long_with_prefix = "ENG:".to_string() + ASCII_LONG;
    group
        .throughput(Throughput::Bytes(ASCII_LONG.len() as u64))
        .bench_with_input(
            "multilang-tokenize-long-with-prefix",
            &long_with_prefix,
            |b, text| {
                b.iter(|| process_tokens(&mut multilang_tokenizer, black_box(text)));
            },
        );
    group
        .throughput(Throughput::Bytes(JPN_SHORT.len() as u64))
        .bench_with_input("multilang-tokenize-jpn-short", JPN_SHORT, |b, text| {
            b.iter(|| process_tokens(&mut multilang_tokenizer, black_box(text)));
        });
    group
        .throughput(Throughput::Bytes(JPN_LONG.len() as u64))
        .bench_with_input("multilang-tokenize-jpn-long", JPN_LONG, |b, text| {
            b.iter(|| process_tokens(&mut multilang_tokenizer, black_box(text)));
        });
    group
        .throughput(Throughput::Bytes(CMN_SHORT.len() as u64))
        .bench_with_input("multilang-tokenize-cmn-short", CMN_SHORT, |b, text| {
            b.iter(|| process_tokens(&mut multilang_tokenizer, black_box(text)));
        });
    group
        .throughput(Throughput::Bytes(CMN_LONG.len() as u64))
        .bench_with_input("multilang-tokenize-cmn-long", CMN_LONG, |b, text| {
            b.iter(|| process_tokens(&mut multilang_tokenizer, black_box(text)));
        });
    group
        .throughput(Throughput::Bytes(KOR_SHORT.len() as u64))
        .bench_with_input("multilang-tokenize-kor-short", KOR_SHORT, |b, text| {
            b.iter(|| process_tokens(&mut multilang_tokenizer, black_box(text)));
        });
    group
        .throughput(Throughput::Bytes(KOR_LONG.len() as u64))
        .bench_with_input("multilang-tokenize-kor-long", KOR_LONG, |b, text| {
            b.iter(|| process_tokens(&mut multilang_tokenizer, black_box(text)));
        });
    group
        .throughput(Throughput::Bytes(CMN_SHORT.len() as u64))
        .bench_with_input(
            "chinese-compatible-tokenize-cmn-short",
            CMN_SHORT,
            |b, text| {
                b.iter(|| process_tokens(&mut chinese_tokenizer, black_box(text)));
            },
        );
    group
        .throughput(Throughput::Bytes(CMN_LONG.len() as u64))
        .bench_with_input(
            "chinese-compatible-tokenize-cmn-long",
            CMN_LONG,
            |b, text| {
                b.iter(|| process_tokens(&mut chinese_tokenizer, black_box(text)));
            },
        );
}

criterion_group!(
    tokenizers_throughput_benches,
    tokenizers_throughput_benchmark
);
criterion_main!(tokenizers_throughput_benches);
