#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
多源媒体全量统一爬虫调度引擎 (Unified Multi-Media Crawler Engine)

全量收录国家网信办《互联网新闻信息稿源单位名单》（2025版，1432家全量支持）：
  1. 中央新闻网站和重点理论网站 (31 家)
  2. 中央新闻单位报刊网站 (67 家)
  3. 部委群团报刊网站 (117 家)
  4. 其他单位报刊网站 (17 家)
  5. 地方新闻网站 (511 家)
  6. 地方新闻单位 (报业传媒/广播电视/融媒体中心，573 家)
  7. 中央政务发布平台 (85 家)
  8. 省级政务发布平台 (31 家)

核心特性：
  - 智能自适应正文提取器（兼容通用新闻CMS、国办指引标准模板、地方大汉/方正系统）
  - 接入 DomainMapper 智能寻址解析器，彻底解决地方台与县融媒的公网域名定位
  - 智能编码探测（自动支持 UTF-8 / GBK / GB2312 / GB18030）
  - 全局 URL 去重与断点续爬（维护 data/visited_urls.txt）
  - 规范分层结构化落盘（data/<媒体类别>/<媒体名称>/<频道>.jsonl）
"""

import os
import re
import sys
import json
import time
import random
import logging
import argparse
import threading
import codecs
import gc
import io
import sqlite3
import tempfile
import subprocess
from contextlib import closing
from datetime import datetime
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import urllib3
from bs4 import BeautifulSoup, NavigableString, Comment, Tag
from lxml import etree

# 忽略 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 基础路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

DATA_DIR = os.path.join(BASE_DIR, "data")
LOG_FILE = os.path.join(BASE_DIR, "crawler.log")
VISITED_FILE = os.path.join(DATA_DIR, "visited_urls.txt")

os.makedirs(DATA_DIR, exist_ok=True)

PAGE_WORKER = sys.argv[1:2] == ["--page-worker"]
if not PAGE_WORKER:
    from sites_registry import (
        MEDIA_SITES, get_all_sites, get_news_media_sites,
        get_sites_by_category, list_all_categories,
    )

# 网络配置
TIMEOUT = 10
DELAY_MIN = 0.5
DELAY_MAX = 1.2
MAX_RETRIES = 2
CHUNK_BYTES = 64 * 1024   # I/O 分块，不是网页大小或正文长度上限
MAX_TASK_FAILURES = 2    # 连续失败后明确留待处理；不会标记成采集成功
PAGE_MEMORY_LIMIT_MIB = 100  # 每个网页子进程的总 RSS，包含 Python 和依赖
PAGE_MEMORY_POLL_SECONDS = 0.1  # 采样保护，非操作系统级硬内存配额

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:127.0) Gecko/20100101 Firefox/127.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
]

IGNORE_EXTENSIONS = (
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg",
    ".pdf", ".zip", ".rar", ".7z", ".tar", ".gz",
    ".mp4", ".mp3", ".avi", ".flv", ".mov", ".wmv",
    ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
    ".css", ".js", ".json", ".xml"
)

# 日志初始化
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("UnifiedCrawler")


# 所有线程共用的文章文件写入锁
article_write_lock = threading.Lock()

CANONICAL_FIELDS = (
    "url", "title", "content", "siteName", "source", "category",
    "subCategory", "channel", "publishTime", "images", "crawledAt",
    "docNumber", "indexNumber",
)


def _optional_string(value, field):
    """把缺失或空白的可选字符串统一为 null。"""
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"字段 {field} 必须是字符串或 null")
    return value if value.strip() else None


def canonical_article(record, site_cfg=None, channel=None):
    """生成与现有 data/**/*.jsonl 完全一致的 13 字段记录。"""
    site_cfg = site_cfg or {}
    required = {}
    for field in ("url", "title", "content"):
        value = record.get(field)
        if not isinstance(value, str):
            raise ValueError(f"字段 {field} 必须是字符串")
        if field in ("url", "title") and not value.strip():
            raise ValueError(f"字段 {field} 不能为空")
        required[field] = value

    images = record.get("images")
    if images is None:
        images = []
    if not isinstance(images, list) or any(not isinstance(item, str) for item in images):
        raise ValueError("字段 images 必须是字符串数组或 null")

    def first(*values):
        return next((value for value in values
                     if value is not None
                     and (not isinstance(value, str) or value.strip())), None)

    normalized = {
        **required,
        "siteName": _optional_string(first(
            record.get("siteName"), record.get("site_name"),
            record.get("media_name"), site_cfg.get("name")), "siteName"),
        "source": _optional_string(record.get("source"), "source"),
        "category": _optional_string(first(
            record.get("category"), record.get("media_category"),
            site_cfg.get("category")), "category"),
        "subCategory": _optional_string(first(
            record.get("subCategory"), record.get("sub_category"),
            site_cfg.get("sub_category")), "subCategory"),
        "channel": _optional_string(first(record.get("channel"), channel), "channel"),
        "publishTime": _optional_string(first(
            record.get("publishTime"), record.get("publish_time")), "publishTime"),
        "images": list(images),
        "crawledAt": _optional_string(first(
            record.get("crawledAt"), record.get("crawled_at"),
            record.get("crawl_time")), "crawledAt"),
        "docNumber": _optional_string(first(
            record.get("docNumber"), record.get("doc_number")), "docNumber"),
        "indexNumber": _optional_string(first(
            record.get("indexNumber"), record.get("index_number")), "indexNumber"),
    }
    if tuple(normalized) != CANONICAL_FIELDS:
        raise AssertionError("文章字段集合或顺序不符合统一规范")
    return normalized


# ─── URL 去重管理器 ────────────────────────────────────────────────────────────
class VisitedManager:
    def __init__(self, filepath=VISITED_FILE):
        self.filepath = filepath
        self.visited = set()       # 已成功保存
        self.in_progress = set()   # 正在抓取
        self._lock = threading.Lock()
        self._load()

    def _load(self):
        # 初始化发生在线程池启动前。
        if os.path.exists(self.filepath):
            with open(self.filepath, "r", encoding="utf-8") as f:
                self.visited.update(
                    line.strip() for line in f if line.strip()
                )

    def try_claim(self, url):
        """检查并占用 URL；同一时刻仅允许一个线程成功。"""
        with self._lock:
            if url in self.visited or url in self.in_progress:
                return False
            self.in_progress.add(url)
            return True

    def mark_success(self, url):
        """文章保存成功后，再持久化去重记录。"""
        with self._lock:
            if url not in self.visited:
                with open(self.filepath, "a", encoding="utf-8") as f:
                    f.write(url + "\n")
                self.visited.add(url)

    def release(self, url):
        """释放占用；失败的 URL 可在后续遇到时重试。"""
        with self._lock:
            self.in_progress.discard(url)


visited_mgr = None if PAGE_WORKER else VisitedManager()


def memory_status():
    """Linux 当前 RSS / 峰值；其他系统不伪造数值。"""
    try:
        with open("/proc/self/status", encoding="ascii") as f:
            return ", ".join(line.strip() for line in f
                             if line.startswith(("VmRSS:", "VmHWM:")))
    except OSError:
        return "RSS unavailable"


class TaskJournal:
    """仅存运行状态的 SQLite 日志；文章数据格式和 visited 文件保持不变。"""
    def __init__(self, path=None, recover=True):
        self.path = path or os.path.join(DATA_DIR, "crawl_tasks.sqlite3")
        self.lock = threading.Lock()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("""CREATE TABLE IF NOT EXISTS tasks (
                kind TEXT, url TEXT, site TEXT, channel TEXT, status TEXT,
                phase TEXT, failures INTEGER DEFAULT 0, error TEXT DEFAULT '',
                path TEXT DEFAULT '', downloaded INTEGER DEFAULT 0,
                encoding TEXT DEFAULT '', updated TEXT,
                PRIMARY KEY(kind, url))""")
            # 只在爬虫进程启动时恢复。上次仍在 running 的 URL 可能是被杀进程的在途任务。
            if recover:
                db.execute("""UPDATE tasks SET failures=failures+1,
                status=CASE WHEN failures+1 >= ? THEN 'deferred' ELSE 'pending' END,
                error='previous process interrupted; not proof this URL caused OOM',
                updated=datetime('now') WHERE status='running'""", (MAX_TASK_FAILURES,))
            deferred = db.execute("SELECT count(*) FROM tasks WHERE status='deferred'").fetchone()[0]
        if deferred and recover:
            log.warning("存在 %s 个待处理任务，保存在 %s；没有标记为已访问", deferred, self.path)

    def get(self, key):
        with self.lock, closing(sqlite3.connect(self.path)) as db:
            db.row_factory = sqlite3.Row
            row = db.execute("SELECT * FROM tasks WHERE kind=? AND url=?", key).fetchone()
            return dict(row) if row else None

    def begin(self, kind, url, site, channel):
        key = (kind, url)
        with self.lock, closing(sqlite3.connect(self.path)) as db, db:
            db.execute("""INSERT OR IGNORE INTO tasks
                (kind,url,site,channel,status,phase,updated)
                VALUES (?,?,?,?,'pending','queued',datetime('now'))""",
                       (kind, url, site, channel))
            row = db.execute("SELECT status FROM tasks WHERE kind=? AND url=?", key).fetchone()
            if row[0] in ("running", "deferred"):
                return False
            db.execute("UPDATE tasks SET status='running',phase='start',updated=datetime('now') "
                       "WHERE kind=? AND url=?", key)
        log.info("任务开始 kind=%s URL=%s %s", kind, url, memory_status())
        return True

    def update(self, key, **values):
        allowed = {"status", "phase", "error", "path", "downloaded", "encoding"}
        if not values.keys() <= allowed:
            raise ValueError("invalid task fields")
        with self.lock, closing(sqlite3.connect(self.path)) as db, db:
            sql = ",".join(name + "=?" for name in values)
            db.execute("UPDATE tasks SET " + sql + ",updated=datetime('now') WHERE kind=? AND url=?",
                       (*values.values(), *key))

    def fail(self, key, error):
        with self.lock, closing(sqlite3.connect(self.path)) as db, db:
            db.execute("""UPDATE tasks SET failures=failures+1,
                status=CASE WHEN failures+1 >= ? THEN 'deferred' ELSE 'pending' END,
                error=?,updated=datetime('now') WHERE kind=? AND url=?""",
                       (MAX_TASK_FAILURES, str(error), *key))
        log.error("任务未完成 URL=%s error=%s %s", key[1], error, memory_status())

    def done(self, key):
        row = self.get(key)
        self.update(key, status="done", phase="done", error="", path="", downloaded=0)
        with self.lock, closing(sqlite3.connect(self.path)) as db, db:
            db.execute("UPDATE tasks SET failures=0 WHERE kind=? AND url=?", key)
        if row and row["path"]:
            try:
                discard_download(row["path"])
            except OSError:
                log.exception("任务完成，但下载缓存清理失败 path=%s", row["path"])

    def pending(self, site, channel):
        with self.lock, closing(sqlite3.connect(self.path)) as db:
            return [row[0] for row in db.execute(
                "SELECT url FROM tasks WHERE kind='article' AND site=? AND channel=? "
                "AND status='pending' ORDER BY updated", (site, channel))]


def discard_download(path):
    # 只删除本程序在 data 下创建的下载文件。
    if (os.path.dirname(os.path.realpath(path)) == os.path.realpath(DATA_DIR)
            and os.path.basename(path).startswith("page-")):
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass


def choose_encoding(path, preferred=None, response_encoding=None):
    with open(path, "rb") as f:
        prefix = f.read(8192)
    meta = re.search(rb'charset=["\']?([\w-]+)', prefix)
    candidates = ["utf-8-sig"] if prefix.startswith(codecs.BOM_UTF8) else []
    if prefix.startswith((codecs.BOM_UTF16_LE, codecs.BOM_UTF16_BE)):
        candidates.append("utf-16")
    candidates += [meta.group(1).decode("ascii") if meta else None, preferred,
                   response_encoding, "utf-8", "gb18030", "gbk", "gb2312"]
    tried = set()
    for encoding in candidates:
        if not encoding or encoding.lower() in tried or "iso-8859" in encoding.lower():
            continue
        tried.add(encoding.lower())
        try:
            decoder = codecs.getincrementaldecoder(encoding)(errors="strict")
            with open(path, "rb") as f:
                for chunk in iter(lambda: f.read(CHUNK_BYTES), b""):
                    decoder.decode(chunk)
                decoder.decode(b"", final=True)
            return encoding
        except (UnicodeError, LookupError):
            continue
    raise ValueError("无法无损解码；保留原始下载供检查，不用替换字符保存正文")


def parse_download(path, encoding):
    """分块解码并向 lxml 的 BS4 target 喂入事件，不创建整页 bytes/str 副本。

    CSS 选择器仍需完整树；此处不是恒定内存解析器，也不承诺任意大页面都可处理。
    使用 BS4 lxml builder 的事件接口，离线测试覆盖当前依赖兼容性。
    """
    soup = BeautifulSoup("", "lxml")
    soup.builder.initialize_soup(soup)
    parser = etree.HTMLParser(target=soup.builder, encoding="utf-8", recover=True,
                              no_network=True, huge_tree=True)
    decoder = codecs.getincrementaldecoder(encoding)(errors="strict")
    try:
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(CHUNK_BYTES), b""):
                text = decoder.decode(chunk)
                if text:
                    parser.feed(text.encode("utf-8"))
            tail = decoder.decode(b"", final=True)
            if tail:
                parser.feed(tail.encode("utf-8"))
        parser.close()
        # libxml 的资源限制等错误不能悄悄返回部分文档。
        for error in parser.feed_error_log:
            if error.level_name == "FATAL" or any(term in error.message.lower()
                    for term in ("resource limit", "excessive depth", "buffer size", "out of memory")):
                raise ValueError("HTML 未完整解析: " + error.message)
        soup.endData()
        while soup.currentTag.name != soup.ROOT_TAG_NAME:
            soup.popTag()
        return soup
    except BaseException:
        soup.decompose()
        raise
    finally:
        soup.builder.soup = None


# ─── 高韧性自适应网络请求器 ──────────────────────────────────────────────────
class UnifiedFetcher:
    def __init__(self, journal=None):
        self._local = threading.local()
        self.journal = journal

    @property
    def session(self):
        if not hasattr(self._local, "session"):
            self._local.session = requests.Session()
        return self._local.session

    def fetch(self, url, preferred_encoding=None, task_key=None):
        if not url or not url.startswith("http"):
            return None
        def record(**values):
            if self.journal and task_key:
                self.journal.update(task_key, **values)
        saved = self.journal.get(task_key) if self.journal and task_key else None
        path = saved["path"] if saved else ""
        complete = bool(saved and saved["downloaded"] and os.path.isfile(path))
        response_encoding = saved["encoding"] if saved else None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                if not complete:
                    if path:
                        discard_download(path)
                    with tempfile.NamedTemporaryFile(prefix="page-", suffix=".download",
                                                     dir=DATA_DIR, delete=False) as f:
                        path = f.name
                    record(phase="download", path=path, downloaded=0)
                    headers = {"User-Agent": random.choice(USER_AGENTS),
                               "Accept": "text/html,application/xhtml+xml",
                               "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8", "Referer": url}
                    log.info("下载开始 URL=%s %s", url, memory_status())
                    with self.session.get(url, headers=headers, timeout=TIMEOUT, verify=False,
                                          allow_redirects=True, stream=True) as resp:
                        resp.raise_for_status()
                        mime = resp.headers.get("Content-Type", "").split(";")[0].strip().lower()
                        if mime and mime not in ("text/html", "application/xhtml+xml", "text/plain"):
                            raise ValueError("非HTML响应类型: " + mime)
                        response_encoding = resp.encoding
                        size = 0
                        with open(path, "wb") as f:
                            for chunk in resp.iter_content(chunk_size=CHUNK_BYTES):
                                f.write(chunk)
                                size += len(chunk)
                            f.flush()
                            os.fsync(f.fileno())
                    complete = True
                    record(phase="downloaded", downloaded=1, encoding=response_encoding or "")
                    log.info("下载完成 URL=%s decoded_bytes=%s %s", url, size, memory_status())
                record(phase="decode")
                encoding = choose_encoding(path, preferred_encoding, response_encoding)
                record(phase="parse", encoding=encoding)
                log.info("解析开始 URL=%s encoding=%s bytes=%s %s",
                         url, encoding, os.path.getsize(path), memory_status())
                soup = parse_download(path, encoding)
                log.info("解析完成 URL=%s %s", url, memory_status())
                if not self.journal:
                    discard_download(path)
                return soup
            except Exception as e:
                log.warning("请求/解析失败 URL=%s attempt=%s error=%s %s",
                            url, attempt, e, memory_status())
                if complete or attempt == MAX_RETRIES:
                    if not self.journal and path:
                        discard_download(path)
                    raise
                time.sleep(0.5 * attempt)


# ─── 统一自适应内容抽取器 ───────────────────────────────────────────────────
class UnifiedExtractor:
    @staticmethod
    def ordered_text(root):
        """每个文本节点只访问一次；不按内容去重，不丢弃短句或截断正文。"""
        block_tags = {"p", "div", "section", "article", "main", "li", "ul", "ol",
                      "h1", "h2", "h3", "h4", "h5", "h6", "blockquote", "pre",
                      "table", "tr", "td", "th", "br", "hr", "figure", "figcaption"}
        ignored = {"script", "style", "nav", "header", "footer", "template"}
        out = io.StringIO()
        boundary = False
        stack = [(iter(root.children), False)]
        while stack:
            children, ends_block = stack[-1]
            try:
                node = next(children)
            except StopIteration:
                stack.pop()
                boundary = boundary or ends_block
                continue
            if isinstance(node, Comment):
                continue
            if isinstance(node, NavigableString):
                value = re.sub(r"\s+", " ", str(node))
                if not value.strip():
                    if not boundary and out.tell():
                        out.write(value)
                    continue
                if boundary and out.tell():
                    out.write("\n")
                boundary = False
                out.write(value)
            elif isinstance(node, Tag) and node.name not in ignored:
                is_block = node.name in block_tags
                boundary = boundary or is_block
                stack.append((iter(node.children), is_block))
        return out.getvalue().strip()

    @classmethod
    def extract_links(cls, soup, base_url, pattern=None):
        links = set()
        parsed_base = urlparse(base_url)
        base_host = parsed_base.netloc.replace("www.", "")

        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            link_text = a.get_text(strip=True)
            if not href or href.startswith("javascript:") or href.startswith("#"):
                continue
            full_url = urljoin(base_url, href)
            clean_url = full_url.split("?")[0].split("#")[0]

            if any(clean_url.lower().endswith(ext) for ext in IGNORE_EXTENSIONS):
                continue

            # 命中站点配置正则
            if pattern and re.search(pattern, clean_url):
                links.add(clean_url)
                continue

            # 通用新闻特征匹配
            parsed_curr = urlparse(clean_url)
            if base_host and base_host in parsed_curr.netloc:
                if re.search(r'(\d{4}[/\-_]\d{2}|\d{8}|content_\d+|t\d+_\d+|article|detail|news|node_\d+|/c\.|/c\d+-)', clean_url, re.I):
                    links.add(clean_url)
                elif any(clean_url.endswith(s) for s in [".html", ".htm", ".shtml"]) and len(link_text) >= 6:
                    links.add(clean_url)

        return list(links)

    @classmethod
    def parse_article(cls, soup, url, site_cfg, channel_name="综合"):
        # 1. 标题提取
        title = ""
        for sel in site_cfg.get("title_selectors", ["h1", ".title", ".article-title", "title"]):
            if sel == "title":
                t_tag = soup.find("title")
                if t_tag:
                    raw_t = t_tag.get_text(strip=True)
                    for sep in ["_", "--", "-", "|"]:
                        raw_t = raw_t.split(sep)[0].strip()
                    if len(raw_t) >= 4 and not any(bad in raw_t for bad in ["404", "错误", "NotFound"]):
                        title = raw_t
                        break
            else:
                el = soup.select_one(sel)
                if el:
                    t = el.get_text(strip=True)
                    if len(t) >= 4 and "导航" not in t and "菜单" not in t:
                        title = t
                        break

        if not title:
            h1 = soup.find("h1")
            if h1 and len(h1.get_text(strip=True)) >= 4:
                title = h1.get_text(strip=True)

        if not title:
            return None

        # 2. 发布时间提取
        pub_time = ""
        for m_key in ["pubdate", "publishdate", "article:published_time", "time", "date"]:
            m_tag = soup.find("meta", attrs={"name": m_key}) or soup.find("meta", property=m_key)
            if m_tag and m_tag.get("content"):
                m = re.search(r'(\d{4}[年\-/]\d{1,2}[月\-/]\d{1,2}(\s+\d{1,2}:\d{2}(:\d{2})?)?)', m_tag.get("content").strip())
                if m:
                    pub_time = m.group(1).replace("年", "-").replace("月", "-").replace("/", "-")
                    break

        if not pub_time:
            for sel in [".time", ".date", ".info", ".source", "p.sou", "[class*='time']", "[class*='date']"]:
                el = soup.select_one(sel)
                if el:
                    m = re.search(r'(\d{4}[年\-/]\d{1,2}[月\-/]\d{1,2}(\s+\d{1,2}:\d{2}(:\d{2})?)?)', el.get_text(strip=True))
                    if m:
                        pub_time = m.group(1).replace("年", "-").replace("月", "-").replace("/", "-")
                        break

        # 3. 来源与发文字号（适配政务平台）
        source = ""
        for sel in [".source", "[class*='source']", "p.sou em", ".author", ".origin"]:
            el = soup.select_one(sel)
            if el:
                s_txt = re.sub(r'^(来源|稿源|出处|作者)[:：\s]*', '', el.get_text(strip=True)).strip()
                if s_txt and len(s_txt) < 40:
                    source = s_txt
                    break

        doc_number = ""
        index_number = ""
        doc_pattern = re.compile(r'(〔\d{4}〕\d+号|〔\d{4}〕第\d+号|\d{4}第\d+号|[国省市县发办]\d{4}\d+号)')
        index_pattern = re.compile(r'[0-9A-Za-z][0-9A-Za-z._/-]{3,}')
        awaiting_index = False
        for text_node in soup.stripped_strings:
            text_node = str(text_node).strip()
            doc_match = doc_pattern.search(text_node)
            if doc_match and not doc_number:
                doc_number = doc_match.group(1)
            compact = re.sub(r'\s+', '', text_node)
            if awaiting_index and not index_number:
                index_match = index_pattern.fullmatch(compact)
                if index_match:
                    index_number = index_match.group(0)
                awaiting_index = False
            if not index_number and "索引号" in compact:
                candidate = compact.split("索引号", 1)[1].lstrip(":：")
                index_match = index_pattern.match(candidate)
                if index_match:
                    index_number = index_match.group(0)
                elif not candidate:
                    awaiting_index = True
            if doc_number and index_number:
                break

        # 4. 正文提取
        content = ""
        content_selectors = site_cfg.get("content_selectors", [])
        for sel in content_selectors:
            box = soup.select_one(sel)
            if box:
                for bad in box.select("script, style, nav, .footer, .header, .share, .comment"):
                    bad.decompose()
                txt = cls.ordered_text(box)
                if len(txt) >= 40:
                    content = txt
                    break

        if not content or len(content) < 40:
            # 优先语义正文容器；回退保留段落，包括短句，不重复提取嵌套 p。
            semantic = soup.find("article") or soup.find("main")
            if semantic:
                content = cls.ordered_text(semantic)
            else:
                paras = soup.find_all("p")
                if paras:
                    content = "\n".join(cls.ordered_text(p) for p in paras if p.find_parent("p") is None)
                else:
                    content = cls.ordered_text(soup.body or soup)

        if not content or len(content) < 30:
            return None

        # 配图提取
        images = []
        for img in soup.find_all("img"):
            src = img.get("src") or img.get("data-src")
            if src:
                full_img = urljoin(url, src)
                if not any(full_img.lower().endswith(x) for x in [".gif", ".svg", "icon", "logo"]):
                    images.append(full_img)
                    if len(images) == 5:
                        break

        return canonical_article({
            "siteName": site_cfg["name"],
            "category": site_cfg["category"],
            "subCategory": site_cfg.get("sub_category"),
            "channel": channel_name,
            "title": title,
            "publishTime": pub_time or datetime.now().strftime("%Y-%m-%d"),
            "source": source or site_cfg["name"],
            "docNumber": doc_number,
            "indexNumber": index_number,
            "content": content,
            "images": images[:5],
            "url": url,
            "crawledAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }, site_cfg, channel_name)


class PageMemoryExceeded(Exception):
    pass


def process_memory(pid):
    """返回 (当前RSS, 峰值RSS) 字节；进程已退出返回 None。"""
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes
        class Counters(ctypes.Structure):
            _fields_ = [("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD)] + [
                (name, ctypes.c_size_t) for name in ("PeakWorkingSetSize", "WorkingSetSize",
                "QuotaPeakPagedPoolUsage", "QuotaPagedPoolUsage", "QuotaPeakNonPagedPoolUsage",
                "QuotaNonPagedPoolUsage", "PagefileUsage", "PeakPagefileUsage")]
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        psapi = ctypes.WinDLL("psapi", use_last_error=True)
        kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        kernel.OpenProcess.restype = wintypes.HANDLE
        kernel.CloseHandle.argtypes = [wintypes.HANDLE]
        psapi.GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.POINTER(Counters), wintypes.DWORD]
        handle = kernel.OpenProcess(0x0400 | 0x0010, False, pid)
        if not handle:
            error = ctypes.get_last_error()
            if error == 87:  # ERROR_INVALID_PARAMETER：PID 已消失
                return None
            raise ctypes.WinError(error)
        try:
            counters = Counters()
            counters.cb = ctypes.sizeof(counters)
            if not psapi.GetProcessMemoryInfo(handle, ctypes.byref(counters), counters.cb):
                error = ctypes.get_last_error()
                if error in (6, 87):
                    return None
                raise ctypes.WinError(error)
            return counters.WorkingSetSize, counters.PeakWorkingSetSize
        finally:
            kernel.CloseHandle(handle)
    if sys.platform.startswith("linux"):
        try:
            with open(f"/proc/{pid}/status", encoding="ascii") as f:
                values = {parts[0]: int(parts[1]) * 1024 for line in f
                          if (parts := line.split()) and parts[0] in ("VmRSS:", "VmHWM:")}
            return (values["VmRSS:"], values["VmHWM:"]) if "VmRSS:" in values else None
        except (FileNotFoundError, ProcessLookupError):
            return None
    raise RuntimeError("网页内存监控目前支持 Linux 和 Windows")


def page_worker(request_path, result_path):
    """只处理一个网页；不加载成功URL集合，不写文章/visited，不恢复其他在途任务。"""
    global DATA_DIR
    with open(request_path, encoding="utf-8") as f:
        request = json.load(f)
    DATA_DIR = request["data_dir"]
    limit = request["memory_limit"]
    parent_pid = request["parent_pid"]
    stopped = threading.Event()

    def check_memory():
        usage = process_memory(os.getpid())
        if usage and max(usage) > limit:
            return False
        return True

    def watchdog():
        while not stopped.wait(PAGE_MEMORY_POLL_SECONDS):
            try:
                if not check_memory():
                    os._exit(86)
                if (sys.platform.startswith("linux") and os.getppid() != parent_pid
                        or process_memory(parent_pid) is None):
                    os._exit(87)
            except Exception:
                os._exit(88)  # 不能监控时失败关闭，不默默取消保护。

    if not check_memory():
        return 86
    threading.Thread(target=watchdog, daemon=True).start()
    soup = None
    try:
        journal = TaskJournal(request["journal"], recover=False)
        key = tuple(request["key"])
        cfg = request["site_cfg"]
        soup = UnifiedFetcher(journal).fetch(key[1], preferred_encoding=cfg.get("encoding"), task_key=key)
        if soup is None:
            raise ValueError("网页没有解析结果")
        journal.update(key, phase="extract")
        if key[0] == "channel":
            data = UnifiedExtractor.extract_links(soup, key[1], cfg.get("url_pattern"))
        else:
            data = UnifiedExtractor.parse_article(soup, key[1], cfg, request["channel"])
            if not data:
                raise ValueError("没有完整可用的标题/正文")
        soup.decompose()
        soup = None
        if not check_memory():
            return 86
        # 完整结果通过磁盘传递，避免管道缓冲导致父子进程互相等待。
        with open(result_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        return 0 if check_memory() else 86
    except Exception as exc:
        log.exception("网页子进程失败 URL=%s", request["key"][1])
        with open(result_path, "w", encoding="utf-8") as f:
            json.dump({"error": str(exc)}, f, ensure_ascii=False)
        return 1
    finally:
        stopped.set()
        if soup is not None:
            soup.decompose()


def monitor_page(process, limit):
    """主进程中由对应工作线程监控；所有退出路径均等待回收子进程。"""
    peak = 0
    try:
        while process.poll() is None:
            usage = process_memory(process.pid)
            if usage:
                peak = max(peak, *usage)
                if peak > limit:
                    raise PageMemoryExceeded(f"子进程 PID={process.pid} RSS峰值={peak} 超过 {limit} 字节")
            time.sleep(PAGE_MEMORY_POLL_SECONDS)
        code = process.wait()
        if code == 86:
            raise PageMemoryExceeded(f"子进程 PID={process.pid} 内存超过 {limit} 字节")
        return code, peak
    finally:
        if process.poll() is None:
            process.kill()
        process.wait()


# ─── 核心调度控制层 ───────────────────────────────────────────────────────────
class UnifiedCrawler:
    def __init__(self):
        self.tasks = TaskJournal()

    def process_page(self, key, site_cfg, channel):
        if PAGE_MEMORY_LIMIT_MIB <= 0:
            raise ValueError("PAGE_MEMORY_LIMIT_MIB 必须大于0")
        paths = []
        try:
            for _ in range(2):
                with tempfile.NamedTemporaryFile(prefix="page-ipc-", suffix=".json",
                                                 dir=DATA_DIR, delete=False) as f:
                    paths.append(f.name)
            request_path, result_path = paths
            with open(request_path, "w", encoding="utf-8") as f:
                json.dump({"key": key, "site_cfg": site_cfg, "channel": channel,
                           "data_dir": DATA_DIR, "journal": self.tasks.path,
                           "parent_pid": os.getpid(),
                           "memory_limit": int(PAGE_MEMORY_LIMIT_MIB * 1024 * 1024)}, f)
            log.info("分配网页子进程 URL=%s limit_mib=%s", key[1], PAGE_MEMORY_LIMIT_MIB)
            child = subprocess.Popen([sys.executable, "-u", os.path.abspath(__file__),
                                      "--page-worker", request_path, result_path],
                                     stdin=subprocess.DEVNULL, env=dict(os.environ, PYTHONIOENCODING="utf-8"))
            code, peak = monitor_page(child, int(PAGE_MEMORY_LIMIT_MIB * 1024 * 1024))
            if code != 0:
                error = f"网页子进程异常退出 code={code}"
                try:
                    with open(result_path, encoding="utf-8") as f:
                        error += ": " + json.load(f).get("error", "")
                except (ValueError, OSError, AttributeError):
                    pass
                raise RuntimeError(error)
            with open(result_path, encoding="utf-8") as f:
                result = json.load(f)
            log.info("网页子进程完成 URL=%s sampled_peak_bytes=%s", key[1], peak)
            return result
        finally:
            for path in paths:
                discard_download(path)

    def page_failed(self, key, exc):
        self.tasks.fail(key, exc)
        if isinstance(exc, PageMemoryExceeded):
            self.tasks.update(key, status="deferred", phase="memory_limit")
            log.warning("超限网页已转待处理，不写入已访问列表 URL=%s", key[1])

    def crawl_site(self, site_cfg, max_articles=2):
        name = site_cfg["name"]
        cat = site_cfg["category"]
        channels = site_cfg.get("channels") or {"综合": site_cfg.get("home_url", "")}
        total_collected = 0
        out_dir = os.path.join(DATA_DIR, cat.replace("/", "_"),
                               name.replace("/", "_").replace("\n", ""))
        os.makedirs(out_dir, exist_ok=True)
        log.info("开始采集媒体 [%s] %s", cat, name)
        for ch_name, ch_url in channels.items():
            if total_collected >= max_articles:
                break
            links = self.tasks.pending(name, ch_name)
            channel_key = ("channel", ch_url)
            if ch_url and self.tasks.begin(*channel_key, name, ch_name):
                try:
                    links.extend(self.process_page(channel_key, site_cfg, ch_name))
                    self.tasks.done(channel_key)
                except Exception as exc:
                    self.page_failed(channel_key, exc)
            # 即使频道暂时失败，也处理此前已持久化的未完成文章。
            links = list(dict.fromkeys(links))
            log.info("[%s/%s] 待检查链接=%s %s", name, ch_name, len(links), memory_status())
            out_file = os.path.join(out_dir, f"{ch_name}.jsonl")
            for link in links:
                if total_collected >= max_articles:
                    break
                if not visited_mgr.try_claim(link):
                    # 上次可能已写好 visited 后、尚未更新任务表时退出。
                    with visited_mgr._lock:
                        saved = link in visited_mgr.visited
                    if saved:
                        old = self.tasks.get(("article", link))
                        if old and old["status"] != "done":
                            self.tasks.done(("article", link))
                    continue
                data = None
                key = ("article", link)
                begun = False
                try:
                    begun = self.tasks.begin(*key, name, ch_name)
                    if not begun:
                        continue
                    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
                    data = self.process_page(key, site_cfg, ch_name)
                    if not data:
                        raise ValueError("没有完整可用的标题/正文；保留下载，不标记成功")
                    data = canonical_article(data, site_cfg, ch_name)
                    self.tasks.update(key, phase="save")
                    with article_write_lock:
                        with open(out_file, "a", encoding="utf-8") as f:
                            json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
                            f.write("\n")
                        visited_mgr.mark_success(link)
                    self.tasks.done(key)
                    total_collected += 1
                    log.info("成功采集 [%s] %s chars=%s URL=%s %s", name,
                             data["title"], len(data["content"]), link, memory_status())
                except Exception as exc:
                    if begun:
                        self.page_failed(key, exc)
                    else:
                        raise
                finally:
                    data = None
                    visited_mgr.release(link)
            gc.collect()  # 频道结束时回收残留循环引用，不在每个节点强制全局回收。
            log.info("频道清理完成 [%s/%s] %s", name, ch_name, memory_status())
        log.info("媒体 [%s] 完成，本次落盘 %s 篇", name, total_collected)
        return total_collected


def main():
    parser = argparse.ArgumentParser(description="多源媒体全量统一爬虫调度系统")
    parser.add_argument("--name", type=str, default="", help="指定媒体名称进行爬取")
    parser.add_argument("--category", type=str, default="", help="指定大类媒体进行爬取")
    parser.add_argument("--all", action="store_true", help="执行全网全量媒体自动化爬取")
    parser.add_argument("--limit", type=int, default=0, help="限制爬取媒体数量 (0为不限制)")
    parser.add_argument("--max-articles", type=int, default=2, help="每家媒体采集篇数 (默认 2)")
    parser.add_argument("--workers", type=int, default=5, help="并发媒体数 (默认 5)")
    parser.add_argument("--list-sites", action="store_true", help="列出已收录的媒体分类概况或指定分类下的媒体清单")
    args = parser.parse_args()

    if args.list_sites:
        if args.category:
            matched = get_sites_by_category(args.category)
            print(f"【{args.category}】收录媒体列表 (共 {len(matched)} 家):")
            for name, info in matched.items():
                print(f"  - {name} ({info.get('home_url')})")
        else:
            print(f"国家网信办全量合规媒体注册中心 (共 {len(MEDIA_SITES)} 家):")
            for cat in list_all_categories():
                c_sites = [k for k, v in MEDIA_SITES.items() if v["category"] == cat]
                print(f"  - 【{cat}】: 共 {len(c_sites)} 家")
            print("\n使用 --list-sites --category <分类名> 可查看该分类下的全部媒体明细。")
        return

    crawler = UnifiedCrawler()

    # 筛选待爬媒体
    target_sites = []
    if args.name:
        if args.name in MEDIA_SITES:
            target_sites.append(MEDIA_SITES[args.name])
        else:
            # 模糊匹配
            matched = [v for k, v in MEDIA_SITES.items() if args.name in k]
            if matched:
                target_sites.extend(matched)
            else:
                print(f"❌ 未找到匹配媒体: {args.name}")
                return
    elif args.category:
        sites_in_cat = get_sites_by_category(args.category)
        target_sites = list(sites_in_cat.values())
    elif args.all:
        target_sites = list(MEDIA_SITES.values())
    else:
        print("请指定 --name、--category 或 --all 参数运行！")
        print("可用媒体总数:", len(MEDIA_SITES))
        print("可用大类列表:")
        for cat in list_all_categories():
            print(f"  - {cat}")
        return

    if args.limit > 0:
        target_sites = target_sites[:args.limit]

    print(f"==================================================")
    print(f"  多源媒体全量统一爬虫调度系统启动")
    print(f"  待采集媒体总数: {len(target_sites)} 家")
    print(f"  每家采样上限: {args.max_articles} 篇")
    print(f"  并发工作线程数: {args.workers}")
    print(f"==================================================\n")

    if args.workers > 1 and len(target_sites) > 1:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = [
                executor.submit(crawler.crawl_site, site, args.max_articles)
                for site in target_sites
            ]
            for f in as_completed(futures):
                try:
                    f.result()
                except Exception as e:
                    log.error(f"媒体采集异常: {e}")
    else:
        for site in target_sites:
            try:
                crawler.crawl_site(site, args.max_articles)
            except Exception as e:
                log.error(f"媒体采集异常: {e}")

    with closing(sqlite3.connect(crawler.tasks.path)) as db:
        summary = dict(db.execute("SELECT status,count(*) FROM tasks GROUP BY status"))
    log.info("任务状态汇总=%s；pending/deferred 不是采集成功，详情见 %s", summary, crawler.tasks.path)
    print("本轮媒体遍历结束；未完成任务保留在 data/crawl_tasks.sqlite3 中。")


if __name__ == "__main__":
    if PAGE_WORKER:
        raise SystemExit(page_worker(sys.argv[2], sys.argv[3]))
    main()
