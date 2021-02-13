use std::{path::Path, string};

use anyhow::Result;
// use rusqlite::{params, Connection};
use postgres::{Client, NoTls};

use crate::comment::{self, Comment};

const SETUP: &str = include_str!("comment.sql");

pub struct Sqlite {
    connection: Client,
}

impl Sqlite {
    pub fn new(filename: &Path) -> Result<Self> {
        let connection = Client::connect("postgres://grover:pass1@localhost/gzp_data", NoTls)
            .unwrap();

        // let connection = Connection::open(filename).unwrap();
        // connection.execute_batch(SETUP)?;
        Ok(Sqlite { connection })
    }

    pub fn insert_comment(&mut self, comment: &Comment) -> Result<usize> {
        self.connection.execute(
            "INSERT INTO comment (reddit_id, author, subreddit, body, score, created_utc, retrieved_on, parent_id, parent_is_post) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)", &[
            &comment.id.as_str(),
            &comment.author.as_str(),
            &comment.subreddit.as_str(),
            &comment.body.as_str(),
            &comment.score,
            &comment.created_utc,
            &comment.retrieved_on,
            &comment.parent_id.as_str(),
            &comment.parent_is_post
        ]).unwrap();

        Ok(0)
    }

    pub fn fair_mapping(&mut self,flair: &str)  -> i32 {
        println!("fair is:{}",flair);
        match flair {
            ":libleft: - LibLeft" => 1,
            ":centrist: - Centrist" => 2,
            ":left: - Left" => 3,
            ":right: - Right" => 4,
            ":authright: - AuthRight" => 5,
            ":libright: - LibRight"=>6,
            ":libright2: - LibRight" => 6,
            ":authleft: - AuthLeft" => 7,
            ":lib: - LibCenter" => 8,
            ":auth: - AuthCenter" => 9,
            _ => 0
        }
    }

    pub fn update_comment_field_by_reddit_id(&mut self, comment: &Comment) -> Result<usize> {
        let flair_code = match &comment.author_flair_text {
            None => 0,
            Some(s) => self.fair_mapping(&s)
        };


        println!("updating {} to reddit_id {}",flair_code,comment.id);
        self.connection.execute(
            "UPDATE comment set flair= $1 where reddit_id= $2", &[
            &flair_code ,
            &comment.id.as_str(),
        ]).unwrap();

        Ok(1)
    }    
}
