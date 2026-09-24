#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""立即开始连续采集，完成指定轮数后退出；失败立即重试当前轮。

启动：python daily_crawler.py
只检查配置：python daily_crawler.py --check
修改常量后需重启调度器。Ctrl+C 只停止调度，已经启动的爬虫继续运行。
路径相对于本文件，整个项目可直接搬到 Linux 服务器。
"""

# ===== README 5.5 的全部爬虫参数：通常只需修改这里 =====
NAME = ""              # --name：媒体全称或模糊名称
CATEGORY = ""          # --category：媒体类别
ALL = True             # --all：遍历当前 all_media_manifest.json 注册的全部媒体
MAX_ARTICLES = 20      # --max-articles：每家媒体每轮最多采集篇数，不是全部历史文章
WORKERS = 8            # --workers：同一爬虫进程内部的并发线程数
LIMIT = 0              # --limit：媒体数量上限；0 表示遍历完整名录
LIST_SITES = False     # --list-sites：True 时每轮只打印名单，不采集文章
# 单媒体或分类采集：将 ALL 改为 False，再设置 NAME 或 CATEGORY（二选一）。

# ===== 调度参数 =====
LOOP_COUNT = 50        # 正常退出的轮数；异常重试不计入，每次启动调度器重新计数
UTC_OFFSET_HOURS = 8   # 仅用于日志文件名，不限制启动时间
POLL_SECONDS = 1       # 发现进程退出的检查间隔；发现失败后不额外等待

import argparse
from datetime import datetime, timedelta, timezone
import errno
import json
import logging
from pathlib import Path
import os
import runpy
import signal
import subprocess
import sys
import time
import sqlite3
from contextlib import closing

BASE_DIR = Path(__file__).resolve().parent
ENTRYPOINT = BASE_DIR / "crawler" / "main.py"
MANIFEST = BASE_DIR / "crawler" / "all_media_manifest.json"
RUNTIME_DIR = BASE_DIR / "scheduler_logs"
SCHEDULER_LOCK = RUNTIME_DIR / "scheduler.lock"
CRAWLER_LOCK = RUNTIME_DIR / "crawler.lock"
BUSY_EXIT_CODE = 75
log = logging.getLogger("DailyCrawler")


class AlreadyRunning(Exception):
    pass


class ProcessLock:
    """操作系统文件锁；文件留在磁盘不代表被占用，不要删除锁文件。"""

    def __init__(self, path):
        self.path = Path(path)
        self.handle = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a+b")
        try:
            if os.name == "nt":
                import msvcrt
                if self.path.stat().st_size == 0:
                    self.handle.write(b"0")
                    self.handle.flush()
                self.handle.seek(0)
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self.handle.close()
            self.handle = None
            if exc.errno in (errno.EACCES, errno.EAGAIN, errno.EDEADLK):
                raise AlreadyRunning(str(self.path)) from exc
            raise
        return self

    def __exit__(self, *args):
        self.handle.close()  # 关闭句柄即释放系统锁，进程崩溃也由系统释放。
        self.handle = None


def build_arguments():
    for label, value in (("NAME", NAME), ("CATEGORY", CATEGORY)):
        if not isinstance(value, str):
            raise ValueError(label + " 必须是字符串")
    if type(ALL) is not bool or type(LIST_SITES) is not bool:
        raise ValueError("ALL 和 LIST_SITES 必须是 True 或 False")
    for label, value, minimum in (("MAX_ARTICLES", MAX_ARTICLES, 1),
                                  ("WORKERS", WORKERS, 1), ("LIMIT", LIMIT, 0)):
        if type(value) is not int or value < minimum:
            raise ValueError(f"{label} 必须是 >= {minimum} 的整数")
    if not LIST_SITES and sum((bool(NAME), bool(CATEGORY), ALL)) != 1:
        raise ValueError("NAME、CATEGORY、ALL 必须且只能选择一种采集范围")
    args = []
    if NAME:
        args.extend(["--name", NAME])
    if CATEGORY:
        args.extend(["--category", CATEGORY])
    if ALL:
        args.append("--all")
    args.extend(["--max-articles", str(MAX_ARTICLES), "--workers", str(WORKERS),
                 "--limit", str(LIMIT)])
    if LIST_SITES:
        args.append("--list-sites")
    return args


def validate_config():
    args = build_arguments()
    if type(LOOP_COUNT) is not int or LOOP_COUNT < 1:
        raise ValueError("LOOP_COUNT 必须是 >= 1 的整数")
    if not 0 < POLL_SECONDS <= 60:
        raise ValueError("POLL_SECONDS 必须大于 0 且不超过 60")
    timezone(timedelta(hours=UTC_OFFSET_HOURS))
    if not ENTRYPOINT.is_file():
        raise ValueError(f"缺少爬虫入口：{ENTRYPOINT}")
    with MANIFEST.open(encoding="utf-8") as handle:
        manifest = json.load(handle)
    if not isinstance(manifest, list) or not manifest:
        raise ValueError("媒体名录必须是非空 JSON 列表")
    if any(not isinstance(item, dict) or not item.get("name") or not item.get("category")
           for item in manifest):
        raise ValueError("媒体名录中的每项必须包含 name 和 category")
    return args, len(manifest)


def run_crawler(arguments):
    # 子进程自己持锁：即使调度器被重启，旧一轮仍能阻止新一轮重叠。
    try:
        with ProcessLock(CRAWLER_LOCK):
            os.chdir(BASE_DIR)
            sys.path.insert(0, str(ENTRYPOINT.parent))
            sys.argv = [str(ENTRYPOINT)] + arguments
            runpy.run_path(str(ENTRYPOINT), run_name="__main__")
        return 0
    except AlreadyRunning:
        print("另一爬虫仍持有运行锁，等待其结束后重试。", flush=True)
        return BUSY_EXIT_CODE


def launch_crawler(arguments, now):
    output = RUNTIME_DIR / ("crawl_" + now.strftime("%Y%m%d_%H%M%S_%f") + ".log")
    command = [sys.executable, "-u", str(Path(__file__).resolve()),
               "--run-crawler"] + arguments
    options = {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP} if os.name == "nt" else {
        "start_new_session": True}
    env = dict(os.environ, PYTHONIOENCODING="utf-8")
    with output.open("ab") as handle:
        process = subprocess.Popen(command, cwd=str(BASE_DIR), stdin=subprocess.DEVNULL,
                                   stdout=handle, stderr=subprocess.STDOUT, env=env, **options)
    log.info("启动本轮，PID=%s，输出=%s", process.pid, output)
    return process


class DailyScheduler:
    def __init__(self, now, launch, loop_count=None):
        self.loop_count = LOOP_COUNT if loop_count is None else loop_count
        if type(self.loop_count) is not int or self.loop_count < 1:
            raise ValueError("循环次数必须是正整数")
        self.completed = 0
        self.attempt = 0
        self.launch = launch
        self.process = None

    @property
    def finished(self):
        return self.completed >= self.loop_count

    def step(self, now):
        if self.finished:
            return
        if self.process is not None:
            code = self.process.poll()
            if code is None:
                return
            self.process = None
            if code == BUSY_EXIT_CODE:
                log.info("旧工作进程仍在运行，等待释放运行锁；不计入轮数")
            elif code == 0:
                self.completed += 1
                self.attempt = 0
                log.info("已完成 %s/%s 轮；站点结果请查采集日志", self.completed, self.loop_count)
            else:
                log.error("第 %s 轮异常退出，退出码=%s；立即重试，沿用已保存 URL 断点",
                          self.completed + 1, code)
        if self.finished:
            log.info("全部 %s 轮完成，停止调度", self.loop_count)
            return
        # 重启调度器时旧工作进程可能还在，先探测锁以免每秒产生空进程和日志。
        try:
            with ProcessLock(CRAWLER_LOCK):
                pass
        except AlreadyRunning:
            return
        try:
            self.attempt += 1
            log.info("开始第 %s/%s 轮，第 %s 次尝试",
                     self.completed + 1, self.loop_count, self.attempt)
            self.process = self.launch(now)
        except OSError:
            log.exception("进程启动失败，下次检查立即重试；不计入完成轮数")


def main():
    # 内部工作进程直接运行原入口；不会递归启动调度器。
    if sys.argv[1:2] == ["--run-crawler"]:
        return run_crawler(sys.argv[2:])
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="只检查配置、打印参数，不采集")
    parser.add_argument("--task-status", action="store_true", help="只显示任务汇总和未完成URL")
    parser.add_argument("--retry-task", metavar="URL", help="停止调度与采集后，将该URL重新放回待处理队列；不立即采集")
    options = parser.parse_args()
    if options.task_status or options.retry_task:
        task_db = BASE_DIR / "crawler" / "data" / "crawl_tasks.sqlite3"
        if not task_db.exists():
            print("尚无任务记录")
            return 0
        if options.retry_task:
            try:
                with ProcessLock(SCHEDULER_LOCK), ProcessLock(CRAWLER_LOCK):
                    with closing(sqlite3.connect(task_db)) as db, db:
                        changed = db.execute("UPDATE tasks SET status='pending',failures=0,error='' "
                            "WHERE url=? AND status != 'done'", (options.retry_task,)).rowcount
                    print(f"已重置 {changed} 个任务；下次运行时重试，保留完整下载文件。")
            except AlreadyRunning:
                print("请先停止调度器和采集进程，再重置任务。")
                return 1
        if options.task_status:
            with closing(sqlite3.connect(task_db)) as db:
                print("任务汇总:", dict(db.execute("SELECT status,count(*) FROM tasks GROUP BY status")))
                for row in db.execute("SELECT kind,url,status,phase,failures,error,path FROM tasks "
                                      "WHERE status != 'done' ORDER BY updated"):
                    print(json.dumps(row, ensure_ascii=False))
        return 0
    arguments, count = validate_config()
    tz = timezone(timedelta(hours=UTC_OFFSET_HOURS))
    if options.check:
        print(f"配置通过；名录 {count} 条；日志文件时区 {tz}")
        print("参数：" + json.dumps(arguments, ensure_ascii=False))
        print(f"立即启动，连续完成 {LOOP_COUNT} 轮；异常立即重试当前轮，不计入完成次数。")
        print("断点为 data/visited_urls.txt 中的已保存 URL；每次重启调度器轮数从 0 开始。")
        return 0
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.FileHandler(RUNTIME_DIR / "scheduler.log", encoding="utf-8"),
                                  logging.StreamHandler()])
    stopping = False

    def request_stop(signum, frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    try:
        with ProcessLock(SCHEDULER_LOCK):
            scheduler = DailyScheduler(datetime.now(tz), lambda now: launch_crawler(arguments, now))
            log.info("立即循环调度启动，名录=%s，目标轮数=%s，参数=%s",
                     count, LOOP_COUNT, arguments)
            while not stopping and not scheduler.finished:
                scheduler.step(datetime.now(tz))
                if not scheduler.finished and not stopping:
                    time.sleep(POLL_SECONDS)
            if stopping:
                log.info("停止后续调度；已启动的爬虫继续执行。")
        return 0
    except AlreadyRunning:
        log.error("此项目已有调度器运行，不重复启动。")
        return 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, OSError) as exc:
        print(f"启动失败：{exc}", file=sys.stderr)
        raise SystemExit(1)
