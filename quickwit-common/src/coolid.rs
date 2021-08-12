use rand::distributions::Alphanumeric;
use rand::prelude::*;

const ADJECTIVES: &[&str] = &[
    "aged",
    "ancient",
    "autumn",
    "billowing",
    "bitter",
    "black",
    "blue",
    "bold",
    "broken",
    "cold",
    "cool",
    "crimson",
    "damp",
    "dark",
    "dawn",
    "delicate",
    "divine",
    "dry",
    "empty",
    "falling",
    "floral",
    "fragrant",
    "frosty",
    "green",
    "hidden",
    "holy",
    "icy",
    "late",
    "lingering",
    "little",
    "lively",
    "long",
    "misty",
    "morning",
    "muddy",
    "nameless",
    "old",
    "patient",
    "polished",
    "proud",
    "purple",
    "quiet",
    "red",
    "restless",
    "rough",
    "shy",
    "silent",
    "small",
    "snowy",
    "solitary",
    "sparkling",
    "spring",
    "still",
    "summer",
    "throbbing",
    "twilight",
    "wandering",
    "weathered",
    "white",
    "wild",
    "winter",
    "wispy",
    "withered",
    "young",
];

/// Returns a randomly generated id
pub fn new_coolid(name: &str) -> String {
    let mut rng = rand::thread_rng();
    let adjective = ADJECTIVES[rng.gen_range(0..ADJECTIVES.len())];
    let slug: String = rng
        .sample_iter(&Alphanumeric)
        .take(4)
        .map(char::from)
        .collect();
    format!("{}-{}-{}", adjective, name, slug)
}

#[cfg(test)]
mod tests {
    use super::new_coolid;
    use std::collections::HashSet;

    #[test]
    fn test_coolid() {
        let cool_ids: HashSet<String> = std::iter::repeat_with(|| new_coolid("hello"))
            .take(100)
            .collect();
        assert_eq!(cool_ids.len(), 100);
    }
}
