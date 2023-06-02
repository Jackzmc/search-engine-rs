use std::ops::Deref;
use std::sync::Arc;
use std::thread;
use concurrent_queue::ConcurrentQueue;
use mysql::*;
use mysql::prelude::*;
use reqwest::Url;
use serde::{Serialize};
use select::document::Document;
use select::predicate::Name;
use tokio::sync::{Mutex, RwLock};

const USER_AGENT: &str = "jackz-search-engine/v1.0";

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
    pub client: reqwest::Client,
    pub queue: ConcurrentQueue<QueueEntry>
}

struct HtmlResult {
    texts: Vec<String>,
    links: Vec<String>
}

impl SearchEngine {
    pub fn new() -> SearchEngine {
        SearchEngine {
            client: reqwest::Client::new(),
            queue:  ConcurrentQueue::unbounded()
        }
    }

    async fn get_html(&self, url: &str) -> Option<String> {
        match self.client.get(url)
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

    async fn crawl(&self, url: &str) -> Option<HtmlResult> {
        if let Some(html) = self.get_html(url).await {
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

#[tokio::main]
async fn main() {
    let mut engine = SearchEngine::new();

    engine.start_crawl(vec![
        "https://google.com",
        "https://jackz.me",
        "https://microsoft.com",
        "https://github.com"
    ]);

    let handle = Arc::new(RwLock::new(engine));

    tokio::spawn(async move {
        let handle = handle.clone();
        println!("thread reader spawned");
        while handle.read().await.has_queued() {
            println!("reading next url...");
            let lock = handle.write().await;
            let entry = lock.queue.pop().unwrap();
            println!("url: {}", &entry.url);
            drop(lock);
            let r_lock = handle.read().await;
            if let Some(result) = r_lock.crawl(&entry.url).await {
                drop(r_lock);
                println!("got result, waiting for write lock");
                let mut lock = handle.write().await;
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
    }).await.ok();
}
