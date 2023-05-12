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

use std::net::{IpAddr, Ipv6Addr};

use serde::{Deserialize, Serialize};
use tantivy::schema::IntoIpv6Addr;
use tantivy::time::format_description::well_known::Rfc3339;
use tantivy::time::OffsetDateTime;

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug)]
#[serde(untagged)]
pub enum JsonLiteral {
    Number(serde_json::Number),
    // String is a bit special.
    //
    // It can either mean it was passed as a string by the user (via the es query dsl for
    // instance), or it can mean its type is unknown as it was parsed out of tantivy's query
    // language.
    //
    // We have decided to not make a difference at the moment.
    String(String),
    Bool(bool),
}

pub trait InterpretUserInput<'a>: Sized {
    fn interpret(user_input: &'a JsonLiteral) -> Option<Self>;
    fn name() -> &'static str {
        std::any::type_name::<Self>()
    }
}

impl<'a> InterpretUserInput<'a> for &'a str {
    fn interpret(user_input: &'a JsonLiteral) -> Option<&'a str> {
        match user_input {
            JsonLiteral::Number(_) => None,
            JsonLiteral::String(text) => Some(text.as_str()),
            JsonLiteral::Bool(_) => None,
        }
    }
}

impl<'a> InterpretUserInput<'a> for u64 {
    fn interpret(user_input: &JsonLiteral) -> Option<u64> {
        match user_input {
            JsonLiteral::Number(json_number) => json_number.as_u64(),
            JsonLiteral::String(text) => text.parse().ok(),
            JsonLiteral::Bool(_) => None,
        }
    }
}

impl<'a> InterpretUserInput<'a> for i64 {
    fn interpret(user_input: &JsonLiteral) -> Option<i64> {
        match user_input {
            JsonLiteral::Number(json_number) => json_number.as_i64(),
            JsonLiteral::String(text) => text.parse().ok(),
            JsonLiteral::Bool(_) => None,
        }
    }
}

// We refuse NaN and infinity.
impl<'a> InterpretUserInput<'a> for f64 {
    fn interpret(user_input: &JsonLiteral) -> Option<f64> {
        let val: f64 = match user_input {
            JsonLiteral::Number(json_number) => json_number.as_f64()?,
            JsonLiteral::String(text) => text.parse().ok()?,
            JsonLiteral::Bool(_) => {
                return None;
            }
        };
        if val.is_nan() || val.is_infinite() {
            return None;
        }
        Some(val)
    }
}

impl<'a> InterpretUserInput<'a> for bool {
    fn interpret(user_input: &JsonLiteral) -> Option<bool> {
        match user_input {
            JsonLiteral::Number(_) => None,
            JsonLiteral::String(text) => text.parse().ok(),
            JsonLiteral::Bool(bool_value) => Some(*bool_value),
        }
    }
}

impl<'a> InterpretUserInput<'a> for Ipv6Addr {
    fn interpret(user_input: &JsonLiteral) -> Option<Ipv6Addr> {
        match user_input {
            JsonLiteral::Number(_) => None,
            JsonLiteral::String(text) => {
                let ip_addr: IpAddr = text.parse().ok()?;
                Some(ip_addr.into_ipv6_addr())
            }
            JsonLiteral::Bool(_) => None,
        }
    }
}

impl<'a> InterpretUserInput<'a> for tantivy::DateTime {
    fn interpret(user_input: &JsonLiteral) -> Option<tantivy::DateTime> {
        match user_input {
            JsonLiteral::Number(_) => None,
            JsonLiteral::String(text) => {
                let dt = OffsetDateTime::parse(text, &Rfc3339).ok()?;
                Some(tantivy::DateTime::from_utc(dt))
            }
            JsonLiteral::Bool(_) => None,
        }
    }
}
