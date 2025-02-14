/// Represents a calculation node in the AST for a calculated field. Function
/// descriptions can be found in the following links:
/// <https://datadoghq.atlassian.net/wiki/spaces/EP/pages/3203203931/Short+expressions+spec>
/// <https://docs.google.com/document/d/184iqx-6rdVv7i4urGcjdnfWlOFqVidSLGBdIcfM4hOw/edit?usp=sharing>
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CalcNode {
    /// A node in the AST.
    #[prost(oneof = "calc_node::CalcNode", tags = "1, 2, 3")]
    pub calc_node: ::core::option::Option<calc_node::CalcNode>,
}
/// Nested message and enum types in `CalcNode`.
pub mod calc_node {
    /// Literal value. Can be an int, double, or string.
    #[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Literal {
        #[prost(oneof = "literal::LiteralValue", tags = "1, 2, 3, 4")]
        pub literal_value: ::core::option::Option<literal::LiteralValue>,
    }
    /// Nested message and enum types in `Literal`.
    pub mod literal {
        #[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
        #[serde(rename_all = "snake_case")]
        #[allow(clippy::derive_partial_eq_without_eq)]
        #[derive(Clone, PartialEq, ::prost::Oneof)]
        pub enum LiteralValue {
            #[prost(int64, tag = "1")]
            IntValue(i64),
            #[prost(double, tag = "2")]
            DoubleValue(f64),
            #[prost(string, tag = "3")]
            StringValue(::prost::alloc::string::String),
            #[prost(bool, tag = "4")]
            BoolValue(bool),
        }
    }
    /// Field reference. Can be a calculated field (like '#foo') or a regular field
    /// (like 'service' or '@duration').
    #[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct FieldRef {
        /// Includes the leading '#' or '@' (if any).
        #[prost(string, tag = "1")]
        pub field_name: ::prost::alloc::string::String,
    }
    /// Function call. Besides regular functions like 'lower' or 'concat', this
    /// includes binary operators like '+' and '==' as well as unary operators like
    /// 'not' and 'is_null'.
    ///
    /// Note that array literals can be represented as function calls in the AST.
    #[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct FuncCall {
        #[prost(enumeration = "func_call::FuncName", tag = "1")]
        pub func_name: i32,
        #[prost(message, repeated, tag = "2")]
        pub arg: ::prost::alloc::vec::Vec<super::CalcNode>,
    }
    /// Nested message and enum types in `FuncCall`.
    pub mod func_call {
        /// Next available value: 65
        #[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
        #[serde(rename_all = "snake_case")]
        #[derive(
            Clone,
            Copy,
            Debug,
            PartialEq,
            Eq,
            Hash,
            PartialOrd,
            Ord,
            ::prost::Enumeration
        )]
        #[repr(i32)]
        pub enum FuncName {
            Unknown = 0,
            /// -- Boolean
            ///
            /// n arguments all booleans
            And = 1,
            /// n arguments all booleans
            Or = 2,
            /// 2 arguments boolean
            Xor = 64,
            /// 1 argument boolean
            Not = 3,
            /// -- Arithmetic
            ///
            /// n arguments number / col types
            Add = 4,
            /// 2 arguments number / col types
            Subtract = 5,
            /// n arguments number / col types
            Multiply = 6,
            /// 2 arguments number / col types
            Divide = 7,
            /// 2 arguments integer / col types
            IntDiv = 23,
            /// 2 arguments integer / col types
            IntMod = 24,
            /// 1 argument number / col types
            Abs = 25,
            /// 1 argument number / col types
            Sign = 26,
            /// 1 argument number, 2nd argument optional constant integer
            Round = 27,
            /// 1 argument number
            Floor = 28,
            /// 1 argument number
            Ceil = 29,
            /// 1 argument number
            Exp = 30,
            /// 1 argument number
            Ln = 31,
            /// 2 arguments number, 2nd argument must be constant
            Log = 32,
            /// 1 argument number
            Log10 = 33,
            /// 2 arguments number, 2nd argument must be constant
            Pow = 34,
            /// 1 argument number
            Sqrt = 35,
            /// -- Random
            ///
            /// 1 argument: 0: integer seed
            RandFloat = 36,
            /// 2 arguments: 0: integer seed, 1: integer max (exclusive)
            RandInt = 37,
            /// -- Comparison
            ///
            /// 2 arguments any type
            Eq = 8,
            /// 2 arguments any type
            Neq = 22,
            /// 2 arguments any type
            Gt = 9,
            /// 2 arguments any type
            Lt = 10,
            /// 2 arguments any type
            GtEq = 11,
            /// 2 arguments any type
            LtEq = 12,
            /// 1 argument any type
            IsNull = 13,
            /// 1 argument any type
            IsNotNull = 14,
            /// 2+ arguments any type: 0: a column,
            In = 21,
            /// 1+: list of constant values
            ///
            /// 1+ arguments number / col types
            Min = 38,
            /// 1+ arguments number / col types
            Max = 39,
            /// -- Misc
            ///
            /// 2+ arguments, 0: column containing IP addresses, 1+: string
            Cidr = 15,
            /// constant IP masks like "10.20.3.4/12"
            /// -- String operators
            ///
            /// 1 argument, string type
            Upper = 16,
            /// 1 argument, string type
            Lower = 17,
            /// 1 argument, string type
            Proper = 18,
            /// n arguments, string type
            Concat = 19,
            /// 3+ arguments string type, 0: join token like ",",
            TextJoin = 20,
            /// 1,2+: columns / strings to join together
            ///
            /// 2 arguments, 0: string, 1: integer
            Left = 40,
            /// 2 arguments, 0: string, 1: integer
            Right = 41,
            /// 3 arguments, 0: string, 1: integer start, 2: optional
            Substring = 42,
            /// integer length
            ///
            /// 3 arguments, 0: string, 1: string separator,
            SplitBefore = 43,
            /// 2: integer index
            ///
            /// 3 arguments, 0: string, 1: string separator,
            SplitAfter = 44,
            /// 2: integer index
            ///
            /// 3 arguments, 0: string, 1: string, 2: optional
            ContainsSubstr = 45,
            /// boolean (case sensitive)
            ///
            /// 2 arguments, 0: string, 1: string
            StartsWith = 46,
            /// 2 arguments, 0: string, 1: string
            EndsWith = 47,
            /// 1 argument, string
            ByteLength = 48,
            /// 2 arguments, 0: string, 1: string
            RegexpMatch = 49,
            /// 4 arguments, 0: string, 1: string, 2: optional
            RegexpExtract = 50,
            /// integer capturing group index, 3: optional
            /// integer position, 4: optional integer occurrence
            ///
            /// 3 arguments, 0: string, 1: string, 2: string
            RegexpReplace = 51,
            /// 2 arguments, 0: string, 1: optional string chars-to-trim
            Rtrim = 52,
            /// 2 arguments, 0: string, 1: optional string chars-to-trim
            Ltrim = 53,
            /// 2 arguments, 0: string, 1: optional string chars-to-trim
            Trim = 54,
            /// -- Conditional operators
            ///
            /// 3 arguments, 0: boolean, 1: any type, 2: any type
            If = 55,
            /// n arguments, any type (returns first non-null argument)
            Coalesce = 56,
            /// n arguments (returns (K*2)th argument if 0th argument
            Switch = 57,
            /// equals (K*2-1)th argument, else returns last argument).
            ///
            /// n arguments (returns (K*2)th argument for the first
            SwitchWhen = 58,
            /// true (K*2-1)th argument, else returns last argument).
            /// -- Cast operators
            ///
            /// 1 argument, any type
            CastString = 59,
            /// 1 argument, any type (aborts query on failure)
            CastInt = 60,
            /// 1 argument, any type (returns null on failure)
            TryCastInt = 61,
            /// 1 argument, any type (aborts query on failure)
            CastFloat = 62,
            /// 1 argument, any type (returns null on failure)
            TryCastFloat = 63,
            /// 2 arguments, 0: string, 1: string
            ToTimestamp = 65,
            /// 2 arguments, 0: string, 1: string
            Extract = 66,
        }
        impl FuncName {
            /// String value of the enum field names used in the ProtoBuf definition.
            ///
            /// The values are not transformed in any way and thus are considered stable
            /// (if the ProtoBuf definition does not change) and safe for programmatic use.
            pub fn as_str_name(&self) -> &'static str {
                match self {
                    FuncName::Unknown => "UNKNOWN",
                    FuncName::And => "AND",
                    FuncName::Or => "OR",
                    FuncName::Xor => "XOR",
                    FuncName::Not => "NOT",
                    FuncName::Add => "ADD",
                    FuncName::Subtract => "SUBTRACT",
                    FuncName::Multiply => "MULTIPLY",
                    FuncName::Divide => "DIVIDE",
                    FuncName::IntDiv => "INT_DIV",
                    FuncName::IntMod => "INT_MOD",
                    FuncName::Abs => "ABS",
                    FuncName::Sign => "SIGN",
                    FuncName::Round => "ROUND",
                    FuncName::Floor => "FLOOR",
                    FuncName::Ceil => "CEIL",
                    FuncName::Exp => "EXP",
                    FuncName::Ln => "LN",
                    FuncName::Log => "LOG",
                    FuncName::Log10 => "LOG10",
                    FuncName::Pow => "POW",
                    FuncName::Sqrt => "SQRT",
                    FuncName::RandFloat => "RAND_FLOAT",
                    FuncName::RandInt => "RAND_INT",
                    FuncName::Eq => "EQ",
                    FuncName::Neq => "NEQ",
                    FuncName::Gt => "GT",
                    FuncName::Lt => "LT",
                    FuncName::GtEq => "GT_EQ",
                    FuncName::LtEq => "LT_EQ",
                    FuncName::IsNull => "IS_NULL",
                    FuncName::IsNotNull => "IS_NOT_NULL",
                    FuncName::In => "IN",
                    FuncName::Min => "MIN",
                    FuncName::Max => "MAX",
                    FuncName::Cidr => "CIDR",
                    FuncName::Upper => "UPPER",
                    FuncName::Lower => "LOWER",
                    FuncName::Proper => "PROPER",
                    FuncName::Concat => "CONCAT",
                    FuncName::TextJoin => "TEXT_JOIN",
                    FuncName::Left => "LEFT",
                    FuncName::Right => "RIGHT",
                    FuncName::Substring => "SUBSTRING",
                    FuncName::SplitBefore => "SPLIT_BEFORE",
                    FuncName::SplitAfter => "SPLIT_AFTER",
                    FuncName::ContainsSubstr => "CONTAINS_SUBSTR",
                    FuncName::StartsWith => "STARTS_WITH",
                    FuncName::EndsWith => "ENDS_WITH",
                    FuncName::ByteLength => "BYTE_LENGTH",
                    FuncName::RegexpMatch => "REGEXP_MATCH",
                    FuncName::RegexpExtract => "REGEXP_EXTRACT",
                    FuncName::RegexpReplace => "REGEXP_REPLACE",
                    FuncName::Rtrim => "RTRIM",
                    FuncName::Ltrim => "LTRIM",
                    FuncName::Trim => "TRIM",
                    FuncName::If => "IF",
                    FuncName::Coalesce => "COALESCE",
                    FuncName::Switch => "SWITCH",
                    FuncName::SwitchWhen => "SWITCH_WHEN",
                    FuncName::CastString => "CAST_STRING",
                    FuncName::CastInt => "CAST_INT",
                    FuncName::TryCastInt => "TRY_CAST_INT",
                    FuncName::CastFloat => "CAST_FLOAT",
                    FuncName::TryCastFloat => "TRY_CAST_FLOAT",
                    FuncName::ToTimestamp => "TO_TIMESTAMP",
                    FuncName::Extract => "EXTRACT",
                }
            }
            /// Creates an enum from field names used in the ProtoBuf definition.
            pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
                match value {
                    "UNKNOWN" => Some(Self::Unknown),
                    "AND" => Some(Self::And),
                    "OR" => Some(Self::Or),
                    "XOR" => Some(Self::Xor),
                    "NOT" => Some(Self::Not),
                    "ADD" => Some(Self::Add),
                    "SUBTRACT" => Some(Self::Subtract),
                    "MULTIPLY" => Some(Self::Multiply),
                    "DIVIDE" => Some(Self::Divide),
                    "INT_DIV" => Some(Self::IntDiv),
                    "INT_MOD" => Some(Self::IntMod),
                    "ABS" => Some(Self::Abs),
                    "SIGN" => Some(Self::Sign),
                    "ROUND" => Some(Self::Round),
                    "FLOOR" => Some(Self::Floor),
                    "CEIL" => Some(Self::Ceil),
                    "EXP" => Some(Self::Exp),
                    "LN" => Some(Self::Ln),
                    "LOG" => Some(Self::Log),
                    "LOG10" => Some(Self::Log10),
                    "POW" => Some(Self::Pow),
                    "SQRT" => Some(Self::Sqrt),
                    "RAND_FLOAT" => Some(Self::RandFloat),
                    "RAND_INT" => Some(Self::RandInt),
                    "EQ" => Some(Self::Eq),
                    "NEQ" => Some(Self::Neq),
                    "GT" => Some(Self::Gt),
                    "LT" => Some(Self::Lt),
                    "GT_EQ" => Some(Self::GtEq),
                    "LT_EQ" => Some(Self::LtEq),
                    "IS_NULL" => Some(Self::IsNull),
                    "IS_NOT_NULL" => Some(Self::IsNotNull),
                    "IN" => Some(Self::In),
                    "MIN" => Some(Self::Min),
                    "MAX" => Some(Self::Max),
                    "CIDR" => Some(Self::Cidr),
                    "UPPER" => Some(Self::Upper),
                    "LOWER" => Some(Self::Lower),
                    "PROPER" => Some(Self::Proper),
                    "CONCAT" => Some(Self::Concat),
                    "TEXT_JOIN" => Some(Self::TextJoin),
                    "LEFT" => Some(Self::Left),
                    "RIGHT" => Some(Self::Right),
                    "SUBSTRING" => Some(Self::Substring),
                    "SPLIT_BEFORE" => Some(Self::SplitBefore),
                    "SPLIT_AFTER" => Some(Self::SplitAfter),
                    "CONTAINS_SUBSTR" => Some(Self::ContainsSubstr),
                    "STARTS_WITH" => Some(Self::StartsWith),
                    "ENDS_WITH" => Some(Self::EndsWith),
                    "BYTE_LENGTH" => Some(Self::ByteLength),
                    "REGEXP_MATCH" => Some(Self::RegexpMatch),
                    "REGEXP_EXTRACT" => Some(Self::RegexpExtract),
                    "REGEXP_REPLACE" => Some(Self::RegexpReplace),
                    "RTRIM" => Some(Self::Rtrim),
                    "LTRIM" => Some(Self::Ltrim),
                    "TRIM" => Some(Self::Trim),
                    "IF" => Some(Self::If),
                    "COALESCE" => Some(Self::Coalesce),
                    "SWITCH" => Some(Self::Switch),
                    "SWITCH_WHEN" => Some(Self::SwitchWhen),
                    "CAST_STRING" => Some(Self::CastString),
                    "CAST_INT" => Some(Self::CastInt),
                    "TRY_CAST_INT" => Some(Self::TryCastInt),
                    "CAST_FLOAT" => Some(Self::CastFloat),
                    "TRY_CAST_FLOAT" => Some(Self::TryCastFloat),
                    "TO_TIMESTAMP" => Some(Self::ToTimestamp),
                    "EXTRACT" => Some(Self::Extract),
                    _ => None,
                }
            }
        }
    }
    /// A node in the AST.
    #[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
    #[serde(rename_all = "snake_case")]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum CalcNode {
        #[prost(message, tag = "1")]
        Literal(Literal),
        #[prost(message, tag = "2")]
        FieldRef(FieldRef),
        #[prost(message, tag = "3")]
        FuncCall(FuncCall),
    }
}
/// Represents a calculated field: defined by a name and an AST.
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CalcField {
    /// Does not include the leading '#'.
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub calc_node: ::core::option::Option<CalcNode>,
}
/// Represents a list of calculated fields. Duplicate definitions and circular
/// dependencies are not allowed.
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CalcFields {
    #[prost(message, repeated, tag = "1")]
    pub calc_field: ::prost::alloc::vec::Vec<CalcField>,
}
