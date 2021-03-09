extern crate clap;
extern crate flate2;
extern crate hashbrown;
extern crate serde;
extern crate serde_json;

mod comment;
mod decompress;
mod post;
mod sqlite;

use core::panic;
use std::{
    fs,
    io::{self, BufRead, BufReader},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc, RwLock,
    },
    thread, time,
};

use crate::hashbrown::HashSet;
use crate::sqlite::Sqlite;
use bzip2::read::BzDecoder;
use clap::{App, Arg};
use xz2::read::XzDecoder;

use comment::Comment;
use post::Post;

fn main() {
    let matches = App::new("pushshift-importer")
        .version("0.1")
        .author("Paul Ellenbogen")
        .arg(
            Arg::with_name("input-dir")
                .required(true)
                .help("Directory where compressed json files containing comments are located")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("sqlite-outfile")
                .required(true)
                .help("Path for for output Sqlite database.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("username")
                .long("username")
                .required(false)
                .multiple(true)
                .takes_value(true)
                .help("Add a username to the username filter"),
        )
        .arg(
            Arg::with_name("subreddit")
                .long("subreddit")
                .required(false)
                .multiple(true)
                .takes_value(true)
                .help("Add a subreddit to the subreddit filter"),
        )
        .arg(
            Arg::with_name("operation")
                .long("operation")
                .required(true)
                .multiple(false)
                .takes_value(true)
                .help("please provide an operation"),
        )
        .about("Import data from pushshift dump into a Sqlite database. Currently limited to comment data only.\
        Multiple filters can be applied, and if any of the filter criteria match, the comment is included. If no filters are supplied, all comments match; ie the whole dataset will be added to the sqlite file.")
        .get_matches();
    let users: HashSet<String> = matches
        .values_of("username")
        .map(|users| users.map(|user| user.to_string()).collect())
        .unwrap_or_else(HashSet::new);
    let subreddits: HashSet<String> = matches
        .values_of("subreddit")
        .map(|users| users.map(|user| user.to_string()).collect())
        .unwrap_or_else(HashSet::new);
    let sqlite_filename = Path::new(matches.value_of("sqlite-outfile").unwrap());
    let mut sqlite = Sqlite::new(sqlite_filename).expect("Error setting up sqlite DB");
    let filter: CommentFilter = CommentFilter { users, subreddits };
    let input_dir = Path::new(matches.value_of("input-dir").unwrap());
    let file_list = get_file_list(input_dir);
    process(
        file_list,
        filter,
        &mut sqlite,
        matches.value_of("operation").unwrap(),
    );
}

fn process(file_list: Vec<PathBuf>, filter: CommentFilter, db: &mut Sqlite, ops: &str) {
    let shared_file_list = Arc::new(RwLock::new(file_list));
    let shared_filter = Arc::new(filter);
    let completed = Arc::new(AtomicUsize::new(0));
    let mut threads = Vec::new();
    let (txpost, rxpost) = mpsc::sync_channel(100000);
    let (tx, rx) = mpsc::sync_channel(100000);
    let num_cpus = num_cpus::get_physical();

    let opCode = match ops {
        "insert" => 1,
        "update_flair" => 2,
        "insert_post" => 3,
        _ => 0,
    };

    for _i in 0..(num_cpus - 1) {
        let filter_context = FilterContext::new(
            shared_filter.clone(),
            shared_file_list.clone(),
            completed.clone(),
            tx.clone(),
            txpost.clone(),
        );
        let thread = thread::spawn(move || {
            if opCode == 3 {
                filter_context.process_queue_post();
            } else {
                filter_context.process_queue_comment();
            }
        });
        threads.push(thread);
    }

    if opCode == 0 {
        panic!("couldn't figure out operation goodbye!")
    }

    // let mut updatedCount = 0;
    let mut slept = false;

    // time to read one type!
    if opCode == 3 {
        loop {
            let maybe_post = rxpost.try_recv();
            match maybe_post {
                Ok(post) => {
                    slept = true;
                    let count = match opCode {
                        3 => db.insert_post(&post).expect("Error inserting comment"),
                        _ => panic!("invalid opCode"),
                    };
                    // updatedCount = updatedCount + count
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    println!("Disconnect?");
                    maybe_post.unwrap();
                }
                Err(mpsc) => {
                    slept = true;
                    if completed.load(Ordering::Relaxed) < (num_cpus - 1) {
                        println!("Sleeping? {}", mpsc);
                        thread::sleep(time::Duration::from_secs(1));
                    } else {
                        break;
                    }
                }
            }
        }
    } else {
        loop {
            let maybe_comment = rx.try_recv();
            match maybe_comment {
                Ok(comment) => {
                    slept = true;
                    let count = match opCode {
                        1 => db
                            .insert_comment(&comment)
                            .expect("Error inserting comment"),
                        2 => db
                            .update_comment_field_by_reddit_id(&comment)
                            .expect("Error while updating comment"),
                        _ => panic!("invalid opCode"),
                    };
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    println!("Disconnect?");
                    maybe_comment.unwrap();
                }
                Err(mpsc) => {
                    slept = true;
                    if completed.load(Ordering::Relaxed) < (num_cpus - 1) {
                        println!("Sleeping? {}", mpsc);
                        thread::sleep(time::Duration::from_secs(1));
                    } else {
                        break;
                    }
                }
            }
        }
    }
    for thread in threads {
        thread.join().unwrap();
    }
}

fn get_file_list(dir: &Path) -> Vec<PathBuf> {
    fs::read_dir(dir)
        .unwrap()
        .into_iter()
        .filter_map(|dir_entry| dir_entry.ok().map(|ent| ent.path()))
        .collect()
}

struct FilterContext {
    filter: Arc<CommentFilter>,
    queue: Arc<RwLock<Vec<PathBuf>>>,
    completed: Arc<AtomicUsize>,
    send_channel_comment: mpsc::SyncSender<comment::Comment>,
    send_channel_post: mpsc::SyncSender<post::Post>,
}

impl FilterContext {
    fn new(
        filter: Arc<CommentFilter>,
        queue: Arc<RwLock<Vec<PathBuf>>>,
        completed: Arc<AtomicUsize>,
        send_channel_comment: mpsc::SyncSender<comment::Comment>,
        send_channel_post: mpsc::SyncSender<post::Post>,
    ) -> Self {
        FilterContext {
            filter,
            queue,
            completed,
            send_channel_comment,
            send_channel_post,
        }
    }

    fn get_next_file(&self) -> Option<PathBuf> {
        let mut queue = self.queue.write().unwrap();
        queue.pop()
    }

    fn process_queue_comment(&self) {
        let mut readCount = 0;
        while let Some(filename) = self.get_next_file() {
            for comment in iter_comments(filename.as_path()) {
                if self.filter.filter(&comment) {
                    self.send_channel_comment.send(comment).unwrap();
                }
                readCount += 1;
                if readCount % 1000 == 0 {
                    println!("read: {}", readCount);
                }
            }
        }
        self.completed.fetch_add(1, Ordering::Relaxed);
    }

    fn process_queue_post(&self) {
        let mut readCount = 0;
        while let Some(filename) = self.get_next_file() {
            for posts in iter_posts(filename.as_path()) {
                self.send_channel_post.send(posts).unwrap();
                readCount += 1;
                if readCount % 1000 == 0 {
                    println!("read: {}", readCount);
                }
            }
        }
        self.completed.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Default)]
struct CommentFilter {
    users: HashSet<String>,
    subreddits: HashSet<String>,
}

impl CommentFilter {
    fn filter(&self, comment: &comment::Comment) -> bool {
        if self.users.is_empty() && self.subreddits.is_empty() {
            return true;
        }
        if self.users.contains(comment.author.as_str()) {
            return true;
        }
        if self.subreddits.contains(comment.subreddit.as_str()) {
            return true;
        }
        false
    }
}

fn deserialize_lines(line: io::Result<String>) -> comment::Comment {
    let line = line.unwrap();
    Comment::from_json_str(line.as_str())
}

fn iter_comments(filename: &Path) -> Box<dyn Iterator<Item = comment::Comment>> {
    let extension = filename.extension().unwrap().to_str().unwrap();
    if extension == "gz" {
        let gzip_file = decompress::gzip_file(filename);
        let iter = gzip_file.lines().into_iter().map(deserialize_lines);
        return Box::new(iter);
    } else if extension == "bz2" {
        let reader = fs::File::open(filename).unwrap();
        let decoder = BufReader::new(BzDecoder::new(reader));
        let iter = decoder.lines().into_iter().map(deserialize_lines);
        return Box::new(iter);
    } else if extension == "xz" {
        let reader = fs::File::open(filename).unwrap();
        let decoder = BufReader::new(XzDecoder::new_multi_decoder(reader));
        let iter = decoder.lines().into_iter().map(deserialize_lines);
        return Box::new(iter);
    } else if extension == "zst" {
        let reader = fs::File::open(filename).unwrap();
        let decoder = BufReader::new(zstd::stream::read::Decoder::new(reader).unwrap());
        let iter = decoder.lines().into_iter().map(deserialize_lines);
        return Box::new(iter);
    }
    panic!("Unknown file extension for file {}", filename.display());
}

fn deserialize_lines_posts(line: io::Result<String>) -> post::Post {
    let line = line.unwrap();
    Post::from_json_str(line.as_str())
}

fn iter_posts(filename: &Path) -> Box<dyn Iterator<Item = post::Post>> {
    let extension = filename.extension().unwrap().to_str().unwrap();
    if extension == "gz" {
        let gzip_file = decompress::gzip_file(filename);
        let iter = gzip_file.lines().into_iter().map(deserialize_lines_posts);
        return Box::new(iter);
    } else if extension == "bz2" {
        let reader = fs::File::open(filename).unwrap();
        let decoder = BufReader::new(BzDecoder::new(reader));
        let iter = decoder.lines().into_iter().map(deserialize_lines_posts);
        return Box::new(iter);
    } else if extension == "xz" {
        let reader = fs::File::open(filename).unwrap();
        let decoder = BufReader::new(XzDecoder::new_multi_decoder(reader));
        let iter = decoder.lines().into_iter().map(deserialize_lines_posts);
        return Box::new(iter);
    } else if extension == "zst" {
        let reader = fs::File::open(filename).unwrap();
        let decoder = BufReader::new(zstd::stream::read::Decoder::new(reader).unwrap());
        let iter = decoder.lines().into_iter().map(deserialize_lines_posts);
        return Box::new(iter);
    }
    panic!("Unknown file extension for file {}", filename.display());
}

// {"downs":0,"link_flair_text":null,"distinguished":null,"media":null,"url":"http://i.imgur.com/ksM1N.jpg","link_flair_css_class":null,"id":"eut41","edited":false,"num_reports":null,"created_utc":1293944394,"banned_by":null,"name":"t3_eut41","subreddit":"pics","title":"Last nights pocket full of rubbers [NSFW]","author_flair_text":null,"is_self":false,"author":"magicks","media_embed":{},"permalink":"/r/pics/comments/eut41/last_nights_pocket_full_of_rubbers_nsfw/","author_flair_css_class":null,"selftext":"","domain":"i.imgur.com","num_comments":0,"likes":null,"clicked":false,"thumbnail":"nsfw","saved":false,"subreddit_id":"t5_2qh0u","ups":1,"approved_by":null,"score":1,"selftext_html":null,"created":1293944394,"hidden":false,"over_18":true}

// {"downs":3,"link_flair_text":null,"distinguished":null,"media":null,"url":"http://www.vaytech.com/","link_flair_css_class":null,"id":"eurax","edited":false,"num_reports":null,"created_utc":1293935045,"banned_by":null,"name":"t3_eurax","subreddit":"promos","title":"Custom Ubuntu Desktops - Vaytech Computers","author_flair_text":null,"promoted":true,"is_self":false,"media_embed":{},"permalink":"/comments/eurax/custom_ubuntu_desktops_vaytech_computers/","author_flair_css_class":null,"selftext":"","domain":"vaytech.com","num_comments":0,"likes":null,"clicked":false,"thumbnail":"http://thumbs.reddit.com/t3_eurax.png?v=748dd8b37d027f65c7f706cbb2c82a9873bc4a64","saved":false,"ups":9,"subreddit_id":"t5_2r4w1","approved_by":null,"score":6,"selftext_html":null,"created":1293935045,"hidden":false,"over_18":false}
