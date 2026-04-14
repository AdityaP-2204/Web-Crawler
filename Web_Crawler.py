import asyncio
import aiohttp
import hashlib
import json
import logging
import urllib.robotparser
import csv
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Set
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from redis.asyncio import Redis
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt

# ==========================================
# 1. DATA MODELS
# ==========================================

@dataclass
class CrawlTask:
    url: str
    domain: str
    depth: int
    priority_score: int
    next_fetch_time: Optional[datetime] = None

@dataclass
class PagePayload:
    url: str
    http_status_code: int
    raw_html: str
    content_hash: str

# ==========================================
# 2. INTERFACES
# ==========================================

class IUrlFrontier(ABC):
    @abstractmethod
    async def enqueue(self, task: CrawlTask) -> None:
        pass
    
    @abstractmethod
    async def dequeue(self) -> CrawlTask:
        pass

    @abstractmethod
    async def is_content_unique(self, content_hash: str) -> bool:
        pass

class IFetcher(ABC):
    @abstractmethod
    async def download(self, task: CrawlTask) -> PagePayload:
        pass
    
    @abstractmethod
    async def initialize(self):
        pass
        
    @abstractmethod
    async def shutdown(self):
        pass

# ==========================================
# 3. IMPLEMENTATIONS
# ==========================================

class AsyncFetcher(IFetcher):
    def __init__(self, user_agent: str, max_concurrent: int = 500):
        self.user_agent = user_agent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.session: Optional[aiohttp.ClientSession] = None

    async def initialize(self):
        timeout = aiohttp.ClientTimeout(total=15) 
        headers = {'User-Agent': self.user_agent}
        self.session = aiohttp.ClientSession(headers=headers, timeout=timeout)

    async def shutdown(self):
        if self.session:
            await self.session.close()

    async def download(self, task: CrawlTask) -> PagePayload:
        if not self.session:
            raise RuntimeError("Fetcher session not initialized")

        async with self.semaphore:
            try:
                async with self.session.get(task.url) as response:
                    status = response.status
                    if status == 200:
                        html = await response.text()
                        content_hash = hashlib.sha256(html.encode('utf-8')).hexdigest()
                    else:
                        html = ""
                        content_hash = ""

                    return PagePayload(task.url, status, html, content_hash)
            except asyncio.TimeoutError:
                return PagePayload(task.url, 408, "", "")
            except aiohttp.ClientError:
                return PagePayload(task.url, 500, "", "")


class RedisUrlFrontier(IUrlFrontier):
    def __init__(self, redis_url: str = "redis://localhost:6379/0", politeness_delay_sec: int = 2):
        self.redis = Redis.from_url(redis_url, decode_responses=True)
        self.delay = politeness_delay_sec

    async def is_content_unique(self, content_hash: str) -> bool:
        if not content_hash: 
            return False
        return await self.redis.sadd("seen_content_hashes", content_hash) == 1

    async def enqueue(self, task: CrawlTask) -> None:
        is_new = await self.redis.sadd("seen_urls", task.url)
        if not is_new:
            return  
            
        payload = json.dumps({"url": task.url, "depth": task.depth})
        queue_key = f"queue:{task.domain}"
        
        await self.redis.rpush(queue_key, payload)
        await self.redis.sadd("active_domains", task.domain)

    async def dequeue(self) -> CrawlTask:
        while True:
            domains = await self.redis.smembers("active_domains")
            for domain in domains:
                cooldown_key = f"cooldown:{domain}"
                is_cooling_down = await self.redis.exists(cooldown_key)
                
                if not is_cooling_down:
                    queue_key = f"queue:{domain}"
                    raw_task = await self.redis.lpop(queue_key)
                    
                    if raw_task:
                        await self.redis.set(cooldown_key, "locked", ex=self.delay)
                        data = json.loads(raw_task)
                        return CrawlTask(url=data["url"], domain=domain, depth=data["depth"], priority_score=0)
                    else:
                        await self.redis.srem("active_domains", domain)
            
            await asyncio.sleep(0.5)


class HtmlParser:
    def __init__(self, allowed_schemes: set = None):
        self.allowed_schemes = allowed_schemes or {"http", "https"}

    def extract_links(self, payload: PagePayload) -> Set[str]:
        extracted_urls = set()
        if payload.http_status_code != 200 or not payload.raw_html:
            return extracted_urls

        soup = BeautifulSoup(payload.raw_html, "lxml")
        for tag in soup.find_all("a", href=True):
            raw_link = tag['href'].strip()
            absolute_url = urljoin(payload.url, raw_link)
            parsed_url = urlparse(absolute_url)
            clean_url = parsed_url._replace(fragment="").geturl()

            if parsed_url.scheme in self.allowed_schemes:
                extracted_urls.add(clean_url)

        return extracted_urls


class UrlFilter:
    def __init__(self, fetcher: IFetcher, user_agent: str):
        self.fetcher = fetcher
        self.user_agent = user_agent
        self.robot_parsers: Dict[str, urllib.robotparser.RobotFileParser] = {}
        self.ignored_extensions = {
            '.pdf', '.jpg', '.jpeg', '.png', '.gif', '.mp4', '.avi', 
            '.zip', '.tar', '.gz', '.exe', '.css', '.js'
        }

    async def _get_robot_parser(self, domain: str, scheme: str) -> urllib.robotparser.RobotFileParser:
        if domain in self.robot_parsers:
            return self.robot_parsers[domain]

        robots_url = f"{scheme}://{domain}/robots.txt"
        parser = urllib.robotparser.RobotFileParser()
        parser.set_url(robots_url)

        task = CrawlTask(url=robots_url, domain=domain, depth=0, priority_score=100)
        payload = await self.fetcher.download(task)

        if payload.http_status_code == 200 and payload.raw_html:
            lines = payload.raw_html.splitlines()
            parser.parse(lines)
        else:
            parser.allow_all = True 

        self.robot_parsers[domain] = parser
        return parser

    async def is_valid_and_allowed(self, url: str) -> bool:
        parsed = urlparse(url)
        domain = parsed.netloc
        scheme = parsed.scheme
        path = parsed.path.lower()

        if any(path.endswith(ext) for ext in self.ignored_extensions):
            return False

        try:
            parser = await self._get_robot_parser(domain, scheme)
            return parser.can_fetch(self.user_agent, url)
        except Exception as e:
            logging.warning(f"Failed to parse robots.txt for {domain}: {e}")
            return True

# ==========================================
# 4. THE ORCHESTRATOR
# ==========================================

class CrawlerOrchestrator:
    def __init__(self, frontier: IUrlFrontier, fetcher: IFetcher, parser: HtmlParser, url_filter: UrlFilter, num_workers: int = 50, max_pages: int = 100):
        self.frontier = frontier
        self.fetcher = fetcher
        self.parser = parser
        self.url_filter = url_filter 
        self.num_workers = num_workers 
        
        self.max_pages = max_pages
        self.pages_crawled = 0
        self.is_running = False
        
        self.edges_file = open("web_graph_edges.csv", "w", newline="", encoding="utf-8")
        self.csv_writer = csv.writer(self.edges_file)
        self.csv_writer.writerow(["Source", "Target"])

    async def worker(self, worker_id: int):
        logging.info(f"Worker {worker_id} started.")
        
        while self.is_running:
            try:
                task = await self.frontier.dequeue()
                payload = await self.fetcher.download(task)
                
                if not await self.frontier.is_content_unique(payload.content_hash):
                    continue

                new_urls = self.parser.extract_links(payload)

                self.pages_crawled += 1
                logging.info(f"[{self.pages_crawled}/{self.max_pages}] Crawled: {payload.url}")

                for url in new_urls:
                    self.csv_writer.writerow([payload.url, url])
                    
                    if await self.url_filter.is_valid_and_allowed(url):
                        domain = urlparse(url).netloc
                        new_task = CrawlTask(
                            url=url, 
                            domain=domain, 
                            depth=task.depth + 1, 
                            priority_score=0
                        )
                        await self.frontier.enqueue(new_task)
                    
            except asyncio.CancelledError:
                # When the orchestrator cancels this task, it will safely break out of the dequeue loop
                break 
            except Exception as e:
                logging.error(f"Worker {worker_id} encountered an error: {e}")

    async def run(self):
        self.is_running = True
        await self.fetcher.initialize() 
        
        # Spin up the concurrent worker pool
        workers = [asyncio.create_task(self.worker(i)) for i in range(self.num_workers)]
        logging.info(f"Crawler running with {self.num_workers} concurrent workers. Target: {self.max_pages} pages.")
        
        try:
            while self.pages_crawled < self.max_pages:
                await asyncio.sleep(0.5)
                
            logging.info(f"Target of {self.max_pages} pages reached. Initiating shutdown sequence...")

        except asyncio.CancelledError:
            logging.info("Shutdown signal received. Stopping crawler...")
        finally:
            self.is_running = False
            self.edges_file.close() 
            
            # Explicitly cancel all workers so they break out of their sleep loops
            for w in workers:
                w.cancel()
                
            await self.fetcher.shutdown()

# ==========================================
# 5. GRAPH PLOTTER
# ==========================================

def plot_web_graph(csv_filepath: str):
    logging.info(f"Loading edge data from {csv_filepath}...")
    df = pd.read_csv(csv_filepath)

    logging.info(f"Loaded {len(df)} edges. Building directed graph...")
    G = nx.from_pandas_edgelist(df, source='Source', target='Target', create_using=nx.DiGraph())
    logging.info(f"Original Graph: {G.number_of_nodes()} nodes and {G.number_of_edges()} edges.")

    MIN_CONNECTIONS = 5
    core_nodes = [node for node, degree in G.degree() if degree >= MIN_CONNECTIONS]
    G_core = G.subgraph(core_nodes)

    logging.info(f"Filtered Graph (Degree >= {MIN_CONNECTIONS}): {G_core.number_of_nodes()} nodes and {G_core.number_of_edges()} edges.")

    plt.figure(figsize=(14, 14))
    
    # Calculate layout on the smaller, filtered graph
    logging.info("Calculating physics layout... This may take a moment.")
    pos = nx.spring_layout(G_core, k=0.15, iterations=20)

    logging.info("Drawing graph...")
    # Make nodes size relative to their degree (importance)
    node_sizes = [G_core.degree(n) * 10 for n in G_core.nodes()]
    
    nx.draw_networkx_nodes(G_core, pos, node_size=node_sizes, node_color='#4285F4', alpha=0.7)
    nx.draw_networkx_edges(G_core, pos, edge_color='gray', arrows=True, arrowsize=5, alpha=0.2)

    plt.title(f"Web Crawl Graph (Filtered: >{MIN_CONNECTIONS} connections)", fontsize=16)
    plt.axis("off")
    plt.tight_layout()
    plt.show()

# ==========================================
# 6. MAIN EXECUTION
# ==========================================

async def run_crawler_engine():
    """Sets up the crawler, injects the seed, and runs the async loop."""
    frontier = RedisUrlFrontier(redis_url="redis://localhost:6379/0", politeness_delay_sec=1)
    await frontier.redis.flushdb()
    fetcher = AsyncFetcher(user_agent="AdityaCrawler-InternProject/1.0", max_concurrent=50)
    parser = HtmlParser()
    url_filter = UrlFilter(fetcher=fetcher, user_agent="AdityaCrawler-InternProject/1.0")

    orchestrator = CrawlerOrchestrator(
        frontier=frontier,
        fetcher=fetcher,
        parser=parser,
        url_filter=url_filter,
        num_workers=20,  
        max_pages=100    
    )

    seed_url = "https://en.wikipedia.org/wiki/Web_crawler"
    logging.info(f"Injecting seed URL: {seed_url}")
    
    seed_task = CrawlTask(
        url=seed_url, 
        domain=urlparse(seed_url).netloc, 
        depth=0, 
        priority_score=100
    )
    await frontier.enqueue(seed_task)

    logging.info("Starting crawler engine...")
    await orchestrator.run()
    logging.info("Crawler engine finished successfully.")

def main():
    # Setup logging output
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    csv_file = "web_graph_edges.csv"

    # Step 1: Run the asynchronous crawler to completion
    try:
        asyncio.run(run_crawler_engine())
    except KeyboardInterrupt:
        logging.info("Crawler run interrupted by user.")
        return

    # Step 2: Once the async loop finishes and the file is closed, plot the graph
    logging.info("Initiating graph visualization phase...")
    plot_web_graph(csv_file)

if __name__ == "__main__":
    main()