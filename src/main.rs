use std::collections::HashMap;
use std::hash::Hash;
use std::ops::Deref;
use std::sync::Arc;
use std::thread;
use std::thread::current;
use concurrent_queue::ConcurrentQueue;
use mysql::*;
use mysql::prelude::*;
use reqwest::Url;
use serde::{Serialize};
use select::document::Document;
use select::predicate::Name;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::{futures, Mutex, RwLock};

const USER_AGENT: &str = "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; jackz-search-engine/0.1; +https://github.com/Jackzmc/search-engine-rs) Chrome/W.X.Y.Z Safari/537.36";

#[derive(PartialEq, Eq, Serialize)]
struct QueueEntry {
    pub url: String,
    pub referrer: Option<String>
}

struct UrlRecord {
    pub id: u32,
    pub url: String,
    pub referrer: Option<String>,
    pub queried: u64
}

struct UrlEntry {
    pub record: UrlRecord,
    pub keywords: Vec<String>
}

struct SearchEngine {
    pool: Pool,
    entries: HashMap<String, Option<String>>,
    pub client: reqwest::Client,
    pub queue: ConcurrentQueue<QueueEntry>
}

struct HtmlResult {
    keywords: Vec<String>,
    links: Vec<String>
}

impl SearchEngine {
    pub fn new() -> SearchEngine {
        let db_url = std::env::var("DB_URL").unwrap_or_else(|_| "mysql://localhost/searchengine".to_string());

        let pool = Pool::new(db_url.as_str()).expect("could not connect to db");

        SearchEngine {
            pool,
            entries: HashMap::new(),
            client: reqwest::Client::new(),
            queue:  ConcurrentQueue::unbounded()
        }
    }

    async fn get_html(&self, url: &Url) -> Option<String> {
        match self.client.get(url.as_str())
            .header("User-Agent", USER_AGENT)
            .send()
            .await
        {
            Ok(res) => {
                match res.text().await {
                    Ok(text) => Some(text),
                    Err(err) => {
                        eprintln!("Could not read {}:\n\t{}", url, err);
                        None
                    }
                }
            },
            Err(err) => {
                eprintln!("Could not fetch {}:\n\t{}", url, err);
                None
            }
        }
    }

    fn parse_html(source: &str) -> HtmlResult {
        let doc = Document::from(source);

        let keywords = SearchEngine::extract_keywords(&doc);
        let links = SearchEngine::extract_links(&doc);

        HtmlResult { keywords, links }
    }

    fn extract_keywords(doc: &Document) -> Vec<String> {
        let stop_words = vec![];
        let mut keywords = vec![];
        let texts = doc.find(Name("p"))
            .filter_map(|n| n.as_text())
            .for_each(|source| {
                let mut new = keyword_extraction::rake::Rake::new(source, &stop_words)
                    .get_ranked_keyword(30);
                keywords.append(&mut new);
            });
        keywords
    }

    fn extract_links(doc: &Document) -> Vec<String> {

        let mut links = doc.find(Name("a"))
            .filter_map(|n| n.attr("href"))
            .map(|s| s.to_string())
            .collect();
        SearchEngine::deduplicate_links(links)
    }

    // TODO: need to record all links and check against that
    fn deduplicate_links(links: Vec<String>) -> Vec<String> {
        let mut result: Vec<String> = Vec::with_capacity(links.len());
        for link in links.iter() {
            let url = Url::parse(link);
            if let Ok(mut url) = url {
                // Filter out non http(s) and http links w/ passwords:
                if !url.scheme().starts_with("http") { continue; }
                if url.password().is_some() { continue; }

                // Remove query parameters, just match base. Also remove trailing ?
                SearchEngine::normalize_url(&mut url);

                let str = url.to_string();
                if !result.contains(&str) {
                    result.push(str);
                }
            }
        }
        result
    }

    fn normalize_url(url: &mut Url) {
        //Strip out ?& query parameters
        url.set_query(None);
        if let Ok(mut seg) = url.path_segments_mut() {
            seg.pop_if_empty();
        }
    }

    /// Determines if robots.txt blocks us from crawling
    async fn can_crawl(&self, url: &Url) -> bool {
        let host = url.host();
        if host.is_none() {
            println!("can_crawl: invalid host for {}; skipping", url.as_str());
            return false;
        }
        return match self.client.get(format!("http://{}/robots.txt", host.as_ref().unwrap()))
            .header("User-Agent", USER_AGENT)
            .send()
            .await
        {
            Ok(res) => {
                match res.text().await {
                    Ok(text) => {
                        let path = url.path();
                        let mut current_user_agent: Option<String> = None;
                        for line in text.lines() {
                            let split: Vec<&str> = line.split(": ").collect();
                            if split.len() == 2 {
                                let key = split[0].to_lowercase();
                                let value = split[1];
                                match key.as_str() {
                                    "user-agent" => {
                                        println!("current user agent: {}", value);
                                        current_user_agent = Some(value.to_string())
                                    },
                                    "allow" => {
                                        if path.starts_with(value) {
                                            println!("can_crawl: found allow for {:?} on {}", current_user_agent, url.as_str());
                                            return true;
                                        }
                                    },
                                    "disallow" => {
                                        if path.starts_with(value) {
                                            println!("can_crawl: found disallow for {:?} on {}", current_user_agent, url.as_str());
                                            return false;
                                        }
                                    },
                                    _ => {}
                                }
                            }
                        }
                        println!("can_crawl: no entries found, allowing");
                        true
                    },
                    _ => {
                        println!("can_crawl: Could not parse {}/robots.txt", host.unwrap());
                        false
                    }
                }
            },
            Err(err) => {
                if let Some(status) = err.status() {
                    if status == 404 {
                        println!("can_crawl: 404 on {}/robots.txt; allowing", host.unwrap());
                        return true;
                    }
                }
                println!("can_crawl: Could not fetch {}/robots.txt:\n\t{}", host.unwrap(), err);
                false
            }
        }
    }

    async fn crawl(&self, raw_url: &str) -> Option<HtmlResult> {
        let url = Url::parse(raw_url);
        if url.is_err() {
            eprintln!("cannot crawl {} due to error:\n\t{}", raw_url, url.unwrap_err());
            return None;
        }
        let mut url = url.unwrap();
        SearchEngine::normalize_url(&mut url);
        if self.has_entry(&url) {
            println!("cannot crawl {} (duplicate entry); skipping", url);
            return None
        } else if !self.can_crawl(&url).await {
            println!("cannot crawl {} (robots.txt disallowed); skipping", url);
            return None
        }
        if let Some(html) = self.get_html(&url).await {
            let result = SearchEngine::parse_html(&html);
            println!("results for {}", url);
            println!("keywords {:?}", result.keywords);
            println!("links {:?}", result.links);
            // for link in links {
            //     self.enqueue(link, Some(url.to_string()));
            // }
            return Some(HtmlResult {
                links: result.links,
                keywords: result.keywords
            })
        }
        None
    }

    fn enqueue(&mut self, url: String, referrer: Option<String>) {
        let entry = QueueEntry {
            url,
            referrer
        };
        self.queue.push(entry).ok();
    }

    async fn get_entries(&mut self) {
        let mut conn = self.pool.get_conn().expect("could not get connection");
        let results: Vec<QueueEntry> = conn.query_map(r"SELECT url, referrer FROM se_urls", |(url, referrer)| QueueEntry { url, referrer }).expect("could not fetch entries");
        let count = results.len();
        for result in results {
            if let Ok(mut url) = Url::parse(&result.url) {
                SearchEngine::normalize_url(&mut url);
                self.entries.insert(url.to_string(), result.referrer);
            }
        }
        println!("loaded {} saved entries", count);
    }

    async fn record_entries(&mut self) {
        let mut conn = self.pool.get_conn().expect("could not get connection");
        conn.exec_batch(
            r"INSERT INTO se_urls (url, referrer) VALUES (:url, :referrer)",
            self.entries.iter().map(|e| params! {
                "url" => e.0,
                "referrer" => e.1,
            })
        ).expect("could not submit entries");
        println!("saved {} entries", self.entries.len());
    }

    fn has_entry(&self, uri: &Url) -> bool {
        // for e in self.entries.iter() {
        //     println!("[e:test] {} vs {} -> {}", e.0, uri.as_str(), self.entries.contains_key(uri.as_str()));
        // }
        self.entries.contains_key(uri.as_str())
    }

    async fn record_entry(&mut self, url: &str, referrer: Option<String>) {
        println!("writing {} to storage", url);
        let mut conn = self.pool.get_conn().expect("could not get connection");
        conn.exec_drop(r"INSERT INTO se_urls (url, referrer, queried) VALUES (:url, :referrer, UNIX_TIMESTAMP())", params! {
            "url" => url,
            "referrer" => &referrer
        }).expect("could not record entry");
        self.entries.insert(url.to_string(), referrer);
    }


    async fn _thread(&mut self) {
        while !self.queue.is_empty() {
            if let Ok(entry) = self.queue.pop() {
                self.crawl(&entry.url).await;
            }
        }
        println!("queue empty, exiting...");
    }


    pub async fn start_crawl(&mut self, seed_urls: Vec<&str>) {
        self.get_entries().await;
        for url in seed_urls {
            self.enqueue(url.to_string(), None);
        }
        println!("enqueued {} urls", self.queue.len());
    }

    pub fn has_queued(&self) -> bool {
        !self.queue.is_empty() && !self.queue.is_closed()
    }
}


const NUM_THREADS: u16 = 2;

#[tokio::main]
async fn main() {
    let mut output_file = File::create("se_links.txt").await.expect("no write output_file");
    let mut engine = SearchEngine::new();

    engine.start_crawl(vec![
        "https://google.com",
        "https://jackz.me",
        "https://microsoft.com",
        "https://github.com"
    ]).await;

    let handle = Arc::new(RwLock::new(engine));
    let mut set = tokio::task::JoinSet::new();
    for i in 0..NUM_THREADS {
        let my_handle = handle.clone();
        set.spawn(async move {
            let handle = my_handle;
            println!("thread reader spawned");
            while handle.read().await.has_queued() {
                println!("reading next url...");
                let mut lock = handle.write().await;
                let entry = lock.queue.pop().unwrap();
                println!("url: {}", &entry.url);
                drop(lock);
                let r_lock = handle.read().await;
                if let Some(result) = r_lock.crawl(&entry.url).await {
                    drop(r_lock);
                    println!("got result, waiting for write lock");
                    let mut lock = handle.write().await;
                    lock.record_entry(&entry.url, entry.referrer).await;
                    println!("got lock, pushing {} found urls", result.links.len());
                    for link in result.links {
                        lock.enqueue(link, Some(entry.url.clone()));
                    }
                    println!("queue size: {}", lock.queue.len());
                    drop(lock);
                }
                println!("sleeping...");
                // Arbitrary sleep
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                println!("end of iteration");
            }
            println!("queue empty, shutting down");
        });
    }

    while let Some(res) = set.join_next().await {
    }
}
