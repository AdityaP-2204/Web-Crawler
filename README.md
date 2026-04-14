# Distributed Web Crawler

**Author:** Aditya Kumar Prajesh  
**Date:** 14 April, 2026  
**Status:** Draft  

---

## 1. Objective

Design and implement a **highly concurrent, distributed web crawler** capable of crawling ~1 million pages/day while:

- Respecting `robots.txt`
- Enforcing domain-level politeness
- Avoiding duplicate crawling
- Building a web graph from extracted links

---

## 2. Goals & Non-Goals

### Goals
- High-throughput crawling using asynchronous workers  
- Extract and normalize outbound links from HTML  
- Respect `robots.txt` and domain rate limits  
- Detect duplicate content using hashing  
- Avoid re-crawling already seen URLs  
- Generate a **graph representation of the web**  

### Non-Goals
- Search engine indexing or ranking  
- Crawling dynamic JS-heavy SPAs  
- Long-term storage of raw HTML  

---

## 3. Background / Context

The earlier system relied on a **single-threaded crawler**, leading to:

- Frequent timeouts  
- IP bans due to lack of politeness  
- Manual intervention on failures  
- Poor scalability  

This system replaces it with a **distributed, asynchronous architecture** that:

- Uses **concurrent workers (asyncio-based)**  
- Distributes workload across domains  
- Reduces latency from days → hours  
- Enables scalable crawling pipelines  

---

## 4. High-Level Design (Architecture)

<img width="1183" height="654" alt="Screenshot from 2026-04-14 16-02-41" src="https://github.com/user-attachments/assets/36fbdd08-f4d3-4699-aae0-874f0becc6f1" />

The crawler follows a **decoupled asynchronous pipeline**, implemented using:

- `asyncio` for concurrency  
- `aiohttp` for non-blocking HTTP requests  
- `Redis` for distributed URL frontier  
- `BeautifulSoup` for parsing  
- `NetworkX` for graph generation  

---

### System Flow

- URL Frontier → Fetch → Parse → Filter → Deduplicate → Re-queue

Each URL moves through independent components forming a scalable pipeline.

---

### Core Components

#### 4.1 URL Frontier (Redis-based)
- Stores URLs grouped by domain (`queue:<domain>`)  
- Maintains:
  - `seen_urls` (deduplication)
  - `active_domains`
- Enforces **politeness using cooldown keys**  
- Acts as a distributed queue  

---

#### 4.2 Fetcher & DNS Resolver
- Implemented via `aiohttp` with async worker pool  
- Uses semaphore for concurrency control  
- Handles:
  - Timeouts
  - HTTP errors
- Generates:
  - Raw HTML
  - Content hash (SHA-256)

---

#### 4.3 Parser
- Uses **BeautifulSoup** to extract `<a href>` links  
- Normalizes:
  - Relative → absolute URLs  
  - Removes fragments  
- Filters based on allowed schemes (`http`, `https`)  

---

#### 4.4 Content Seen Filter
- Uses **SHA-256 hashing** of page content  
- Stored in Redis (`seen_content_hashes`)  
- Prevents crawling duplicate pages  

---

#### 4.5 URL Filter (robots.txt + extensions)
- Fetches and caches `robots.txt` per domain  
- Validates URLs using `urllib.robotparser`  
- Filters unwanted file types:
  - `.pdf`, `.jpg`, `.mp4`, `.zip`, etc.  

---

#### 4.6 Host Splitter
- Separates URLs by domain  
- Routes them into domain-specific queues  
- Ensures:
  - No domain is overloaded  
  - Rate limits are respected  

---

#### 4.7 Duplicate URL Eliminator
- Checks Redis set (`seen_urls`)  
- Avoids:
  - Re-crawling  
  - Re-queuing  
- Only unseen URLs are added back to the frontier  

---

### Execution Model

- Multiple async workers continuously:
  1. Dequeue URL  
  2. Fetch page  
  3. Parse links  
  4. Filter + deduplicate  
  5. Re-enqueue new URLs  

- Controlled by a central **Crawler Orchestrator**:
  - Manages worker pool  
  - Tracks crawl progress  
  - Stops after target pages  

---

### Output

- Stores edges in:
- web_graph_edges.csv
- Source URL → Target URL
- - Enables graph visualization of crawled web  

---
