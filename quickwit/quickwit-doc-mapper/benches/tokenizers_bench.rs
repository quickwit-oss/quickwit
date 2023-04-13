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
use quickwit_doc_mapper::QUICKWIT_TOKENIZER_MANAGER;
use tantivy::tokenizer::{TextAnalyzer, Token};

// A random ascii string of length 100 chars.
const ASCII_SHORT: &str = "It is a long established fact";
const ASCII_MEDIUM: &str =
    "It is a long established fact that a reader will be distracted by the readable content of a \
     page when looking at its layout. The point of using Lorem Ipsum is that it has a \
     more-or-less normal distribution of letters, as opposed to using 'Content here, content \
     here', making it look like readable English. Many desktop publishing packages and web page \
     editors now use Lorem Ipsum as their default model text, and a search for 'lorem ipsum' will \
     uncover many web sites still in their infancy. Various versions have evolved over the years, \
     sometimes by accident, sometimes on purpose (injected humour and the like).";
const ASCII_WITH_LANG_PREFIX_SHORT: &str = "ENG:it is a long established fact";
const ASCII_WITH_LANG_PREFIX_MEDIUM: &str =
    "ENG:It is a long established fact that a reader will be distracted by the readable content \
     of a page when looking at its layout. The point of using Lorem Ipsum is that it has a \
     more-or-less normal distribution of letters, as opposed to using 'Content here, content \
     here', making it look like readable English. Many desktop publishing packages and web page \
     editors now use Lorem Ipsum as their default model text, and a search for 'lorem ipsum' will \
     uncover many web sites still in their infancy. Various versions have evolved over the years, \
     sometimes by accident, sometimes on purpose (injected humour and the like).";
const JP_SHORT: &str = "日本ごです。　とても素敵な言葉ですね";
const JP_MEDIUM: &str = "日本ごです。　和名の由来は、\
                         太陽の動きにつれてその方向を追うように花が回るといわれたことから。\
                         ただしこの動きは生長に伴うものであるため、\
                         実際に太陽を追って動くのは生長が盛んな若い時期だけである。\
                         若いヒマワリの茎の上部の葉は太陽に正対になるように動き、\
                         朝には東を向いていたのが夕方には西を向く。日没後はまもなく起きあがり、\
                         夜明け前にはふたたび東に向く。この運動はつぼみを付ける頃まで続くが、\
                         つぼみが大きくなり花が開く素敵な言葉ですね.";
const CN_SHORT: &str = "滚滚长江东逝水，浪花淘尽英雄。";
const CN_MEDIUM: &str = "滚滚长江东逝水，浪花淘尽英雄。是非成败转头空，青山依旧在，几度夕阳红。\
                         白发渔樵江渚上，惯看秋月春风。一壶浊酒喜相逢，古今多少事，都付笑谈中。\
                         是非成败转头空，青山依旧在，惯看秋月春风。一壶浊酒喜相逢，古今多少事，\
                         滚滚长江东逝水，浪花淘尽英雄。 几度夕阳红。白发渔樵江渚上，都付笑谈中。";

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("multilanguage");
    let default_tokenizer = QUICKWIT_TOKENIZER_MANAGER.get("default").unwrap();
    let multilanguage_tokenizer = QUICKWIT_TOKENIZER_MANAGER.get("multilanguage").unwrap();
    let chinese_tokenizer = QUICKWIT_TOKENIZER_MANAGER
        .get("chinese_compatible")
        .unwrap();
    fn process_tokens(analyzer: &TextAnalyzer, text: &str) -> Vec<Token> {
        let mut token_stream = analyzer.token_stream(text);
        let mut tokens: Vec<Token> = vec![];
        token_stream.process(&mut |token: &Token| tokens.push(token.clone()));
        tokens
    }
    group
        .throughput(Throughput::Bytes(ASCII_SHORT.len() as u64))
        .bench_with_input("default-tokenize-short", ASCII_SHORT, |b, text| {
            b.iter(|| process_tokens(&default_tokenizer, black_box(text)));
        });
    group
        .throughput(Throughput::Bytes(ASCII_MEDIUM.len() as u64))
        .bench_with_input("default-tokenize-long", ASCII_MEDIUM, |b, text| {
            b.iter(|| process_tokens(&default_tokenizer, black_box(text)));
        });
    group
        .throughput(Throughput::Bytes(ASCII_SHORT.len() as u64))
        .bench_with_input("multilanguage-tokenize-short", ASCII_SHORT, |b, text| {
            b.iter(|| process_tokens(&multilanguage_tokenizer, black_box(text)));
        });
    group
        .throughput(Throughput::Bytes(ASCII_MEDIUM.len() as u64))
        .bench_with_input("multilanguage-tokenize-long", ASCII_MEDIUM, |b, text| {
            b.iter(|| process_tokens(&multilanguage_tokenizer, black_box(text)));
        });
    group
        .throughput(Throughput::Bytes(ASCII_SHORT.len() as u64))
        .bench_with_input(
            "multilanguage-prefix-lang-tokenize-short",
            ASCII_WITH_LANG_PREFIX_SHORT,
            |b, text| {
                b.iter(|| process_tokens(&multilanguage_tokenizer, black_box(text)));
            },
        );
    group
        .throughput(Throughput::Bytes(ASCII_MEDIUM.len() as u64))
        .bench_with_input(
            "multilanguage-prefix-lang-detection-tokenize-long",
            ASCII_WITH_LANG_PREFIX_MEDIUM,
            |b, text| {
                b.iter(|| process_tokens(&multilanguage_tokenizer, black_box(text)));
            },
        );
    group
        .throughput(Throughput::Bytes(JP_SHORT.len() as u64))
        .bench_with_input("multilanguage-tokenize-jpn-short", JP_SHORT, |b, text| {
            b.iter(|| process_tokens(&multilanguage_tokenizer, black_box(text)));
        });
    group
        .throughput(Throughput::Bytes(JP_MEDIUM.len() as u64))
        .bench_with_input("multilanguage-tokenize-jpn-medium", JP_MEDIUM, |b, text| {
            b.iter(|| process_tokens(&multilanguage_tokenizer, black_box(text)));
        });
    group
        .throughput(Throughput::Bytes(CN_SHORT.len() as u64))
        .bench_with_input("multilanguage-tokenize-cmn-short", CN_SHORT, |b, text| {
            b.iter(|| process_tokens(&multilanguage_tokenizer, black_box(text)));
        });
    group
        .throughput(Throughput::Bytes(CN_MEDIUM.len() as u64))
        .bench_with_input("multilanguage-tokenize-cmn-medium", CN_MEDIUM, |b, text| {
            b.iter(|| process_tokens(&multilanguage_tokenizer, black_box(text)));
        });
    group
        .throughput(Throughput::Bytes(CN_SHORT.len() as u64))
        .bench_with_input(
            "chinese-compatible-tokenize-cmn-short",
            CN_SHORT,
            |b, text| {
                b.iter(|| process_tokens(&chinese_tokenizer, black_box(text)));
            },
        );
    group
        .throughput(Throughput::Bytes(CN_MEDIUM.len() as u64))
        .bench_with_input(
            "chinese-compatible-tokenize-cmn-medium",
            CN_MEDIUM,
            |b, text| {
                b.iter(|| process_tokens(&chinese_tokenizer, black_box(text)));
            },
        );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
