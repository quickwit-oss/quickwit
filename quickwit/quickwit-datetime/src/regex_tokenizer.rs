use regex::{Regex, RegexSet};

pub(crate) struct RegexTokenizer<T> {
    regexset: RegexSet,
    regexs: Vec<Regex>,
    callbacks: Vec<fn(&str) -> Option<T>>
}

impl<T> RegexTokenizer<T> {
    pub fn new(ptns_router: Vec<(&str, fn(&str)->Option<T>)>) -> Result<Self, regex::Error> {
        let ptns: Vec<String> = ptns_router.iter().map(|(ptn, _)| format!("^{ptn}")).collect();
        let regexs: Vec<Regex> = ptns.iter().map(|ptn| Regex::new(ptn)).collect::<Result<_, _>>()?;
        let callbacks: Vec<_> = ptns_router.into_iter().map(|(_, callback)| callback).collect();
        let regexset = regex::RegexSet::new(ptns)?;
        Ok(RegexTokenizer {
            regexset,
            regexs,
            callbacks
        })
    }

    fn match_token(&self, text: &mut &str) -> Option<T> {
        let matches = self.regexset.matches(text);
        for pattern_id in matches {
            // unfortunately regexset does not give us the length of the match, so we need to rerun the targetted ptn.
            let m = self.regexs[pattern_id].find(*text)?;
            let match_len = m.len();
            if match_len == 0 {
                return None;
            }
            let match_str = &text[0..match_len];
            if let Some(token) = self.callbacks[pattern_id](match_str) {
                *text = &text[match_len..];
                return Some(token);
            }
        }
        None
    }

    /// Tokenize the input text. If no pattern matches, returns the position of the error.
    pub fn tokenize(&self, mut text: &str) -> Result<Vec<T>, usize> {
        let len = text.len();
        let mut tokens = Vec::new();
        while !text.is_empty() {
            let token = self.match_token(&mut text)
                .ok_or_else(|| {
                    len - text.len()
                })?;
            tokens.push(token);
        }
        Ok(tokens)
    }
}

#[cfg(test)]
mod tests {
    use super::RegexTokenizer;


    #[derive(Eq, PartialEq, Debug)]
    enum Token {
        Number(u64),
        Ip(String),
        Dot
    }

    #[test]
    fn test_regex_tokenizer_simple_priority() {
        use std::str::FromStr;
        let regex_tokenizer = RegexTokenizer::new(vec![
            (r#"\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}"#, |s| { Some(Token::Ip(s.to_string())) }),
            (r#"\d{1,10}"#, |s| { Some(Token::Number(u64::from_str(s).unwrap())) }),
            (r#"\."#, |_s| { Some(Token::Dot) }),
        ]).unwrap();
        let tokens = regex_tokenizer.tokenize("128.1.").unwrap();
        assert_eq!(tokens.len(), 4);
        assert_eq!(tokens[0], Token::Number(128));
        assert_eq!(tokens[1], Token::Dot);
        assert_eq!(tokens[2], Token::Number(1));
        assert_eq!(tokens[3], Token::Dot);
        let tokens = regex_tokenizer.tokenize("128.1.1.12").unwrap();
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0], Token::Ip("128.1.1.12".to_string()));
    }

    #[test]
    fn test_regex_tokenizer_invalid() {
            use std::str::FromStr;
            let regex_tokenizer = RegexTokenizer::new(vec![
                (r#"\d+"#, |s| { Some(Token::Number(u64::from_str(s).unwrap())) }),
            ]).unwrap();
            let error_position = regex_tokenizer.tokenize("993s3").unwrap_err();
            assert_eq!(error_position, 3);
        }
}
