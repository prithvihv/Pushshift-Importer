use anyhow::Context;
use serde::Deserialize;

// author, author_flair_text, created_utc, retrieved_on, title, url, subreddit, id, score
#[derive(Deserialize, Debug, Clone)]
pub struct Post {
    pub id: String,
    pub author: Option<String>,
    pub author_flair_text: Option<String>, // converting this into our code before we push to db
    pub created_utc: i32,
    pub retrieved_on: Option<i32>,
    pub title: String,
    pub url: String,
    pub subreddit: Option<String>,
    pub score: i32,
}

impl Post {
    pub fn from_json_str(line: &str) -> Self {
        let mut json: serde_json::Value = serde_json::from_str(line)
            .with_context(|| format!("Failed to read json for line: {}", line))
            .unwrap();
        if let Some(created) = json.get_mut("created_utc") {
            if let serde_json::Value::String(utc_string) = created {
                let utc: u64 = utc_string.parse().unwrap();
                *created = utc.into();
            }
        }
        if let Some(score) = json.get_mut("score") {
            if matches!(score, serde_json::Value::Null) {
                *score = 0.into()
            }
        }

        if let Some(author) = json.get_mut("author") {
            if matches!(author, serde_json::Value::Null) {
                *author = "".into()
            }
        }
        let mut post = Post::deserialize(json)
            .with_context(|| format!("Failed to deserialize line: {}", line))
            .unwrap();

        post
    }
}