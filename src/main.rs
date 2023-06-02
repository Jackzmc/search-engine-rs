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
    file: File,
    pub client: reqwest::Client,
    pub queue: ConcurrentQueue<QueueEntry>
}

struct HtmlResult {
    texts: Vec<String>,
    links: Vec<String>
}

impl SearchEngine {
    pub fn new(file: File) -> SearchEngine {
        SearchEngine {
            file,
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

        let texts = SearchEngine::extract_texts(&doc);
        let links = SearchEngine::extract_links(&doc);

        HtmlResult { texts, links }
    }

    fn extract_texts(doc: &Document) -> Vec<String> {
        doc.find(Name("p"))
            .filter_map(|n| n.as_text())
            .map(|s| {
                let t = s.to_string();
                println!("{}", t);
                t
            })
            .collect()
    }

    fn extract_links(doc: &Document) -> Vec<String> {
        doc.find(Name("a"))
            .filter_map(|n| n.attr("href"))
            .map(|s| s.to_string())
            .collect()
    }


    fn extract_keywords(sources: Vec<&str>) -> Vec<String> {
        let stop_words = vec![];
        let mut keywords = vec![];
        for source in sources {
            let mut new = keyword_extraction::rake::Rake::new(source, &stop_words)
                .get_ranked_keyword(30);
            keywords.append(&mut new);
        }
        keywords
    }

    fn deduplicate_links(links: Vec<String>) -> Vec<String> {
        let mut result: Vec<String> = Vec::with_capacity(links.len());
        for link in links.iter() {
            let url = Url::parse(link);
            if let Ok(mut url) = url {
                // Filter out non http(s) and http links w/ passwords:
                if !url.scheme().starts_with("http") { continue; }
                if url.password().is_some() { continue; }

                // Strip out query parameters:
                url.query_pairs_mut().clear();

                // Remove query parameters, just match base. Also remove trailing ?
                let str = SearchEngine::normalize_url(&mut url);

                if !result.contains(&str) {
                    result.push(str);
                }
            }
        }
        result
    }

    fn normalize_url(url: &mut Url) -> String {
        let mut str = url.to_string();
        // Remove trailing ? and /
        if str.ends_with("?") {
            str.pop();
        }
        if str.ends_with("/") {
            str.pop();
        }
        str
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
        let url = url.unwrap();
        if !self.can_crawl(&url).await {
            println!("cannot crawl {} (robots.txt disallowed); skipping", url);
            return None
        }
        if let Some(html) = self.get_html(&url).await {
            let result = SearchEngine::parse_html(&html);
            let keywords = SearchEngine::extract_keywords(result.texts.iter().map(|s| &**s).collect());
            println!("results for {}", url);
            println!("keywords {:?}", keywords);
            let links =  SearchEngine::deduplicate_links(result.links);
            println!("links {:?}", links);
            // for link in links {
            //     self.enqueue(link, Some(url.to_string()));
            // }
            return Some(HtmlResult {
                links,
                texts: keywords
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

    async fn record_entry(&mut self, url: &str) {
        println!("writing {} to disk", url);
        self.file.write(format!("{}\n", url).as_bytes()).await.ok();
        // writeln!(&mut self.file, "{}", url).ok();
    }

    async fn _thread(&mut self) {
        while !self.queue.is_empty() {
            if let Ok(entry) = self.queue.pop() {
                self.crawl(&entry.url).await;
            }
        }
        println!("queue empty, exiting...");
    }


    pub fn start_crawl(&mut self, seed_urls: Vec<&str>) {
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
    let mut engine = SearchEngine::new(output_file);

    engine.start_crawl(vec![
        "https://google.com",
        "https://jackz.me",
        "https://microsoft.com",
        "https://github.com"
    ]);

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
                    lock.record_entry(&entry.url).await;
                    println!("got lock, pushing {} found urls", result.links.len());
                    for link in result.links {
                        lock.enqueue(link, Some(entry.url.clone()));
                    }
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
