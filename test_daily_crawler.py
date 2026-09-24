"""离线调度测试：python -m unittest -v test_daily_crawler.py"""
from datetime import datetime
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

import daily_crawler as daily


def moment(day, hour=0, second=0):
    return datetime(2026, 9, day, hour, 0, second,
                    tzinfo=daily.timezone(daily.timedelta(hours=8)))


class SchedulerTests(unittest.TestCase):
    def setUp(self):
        self.child = Mock(pid=123)
        self.child.poll.return_value = None
        self.launch = Mock(return_value=self.child)
        self.scheduler = daily.DailyScheduler(moment(14, 12), self.launch, loop_count=2)
        lock_patch = patch.object(daily, "ProcessLock")
        self.lock_class = lock_patch.start()
        self.addCleanup(lock_patch.stop)
        # Integration tests below need the real operating-system lock.
        if self._testMethodName.startswith(("test_lock_", "test_worker_", "test_launch_complete", "test_resume_")):
            lock_patch.stop()

    def test_starts_immediately_during_day(self):
        self.scheduler.step(moment(14, 12))
        self.launch.assert_called_once()

    def test_running_process_never_overlaps(self):
        self.scheduler.step(moment(14, 12))
        self.scheduler.step(moment(15))
        self.launch.assert_called_once()
        self.assertEqual(self.scheduler.completed, 0)

    def test_success_starts_next_round_and_stops_at_limit(self):
        self.scheduler.step(moment(14, 12))
        self.child.poll.return_value = 0
        self.scheduler.step(moment(14, 12, 1))
        self.assertEqual(self.launch.call_count, 2)
        self.assertEqual(self.scheduler.completed, 1)
        self.scheduler.step(moment(14, 12, 2))
        self.assertTrue(self.scheduler.finished)
        self.scheduler.step(moment(15))
        self.assertEqual(self.launch.call_count, 2)

    def test_negative_exit_retries_same_round_immediately(self):
        self.scheduler.step(moment(14, 12))
        self.child.poll.return_value = -9
        with self.assertLogs(daily.log, level="ERROR"):
            self.scheduler.step(moment(14, 12, 1))
        self.assertEqual(self.launch.call_count, 2)
        self.assertEqual(self.scheduler.completed, 0)
        self.child.poll.return_value = 0
        self.scheduler.step(moment(14, 12, 2))
        self.assertEqual(self.scheduler.completed, 1)

    def test_launch_failure_retries_without_counting(self):
        self.launch.side_effect = [OSError("failure"), self.child]
        with self.assertLogs(daily.log, level="ERROR"):
            self.scheduler.step(moment(14, 12))
        self.scheduler.step(moment(14, 12, 1))
        self.assertEqual(self.launch.call_count, 2)
        self.assertEqual(self.scheduler.completed, 0)

    def test_busy_lock_waits_without_launching(self):
        self.lock_class.return_value.__enter__.side_effect = daily.AlreadyRunning()
        self.scheduler.step(moment(14, 12))
        self.launch.assert_not_called()
        self.lock_class.return_value.__enter__.side_effect = None
        self.scheduler.step(moment(14, 12, 1))
        self.launch.assert_called_once()

    def test_default_fifty_successful_rounds(self):
        scheduler = daily.DailyScheduler(moment(14), self.launch)
        self.assertEqual(scheduler.loop_count, 50)
        self.child.poll.return_value = 0
        for _ in range(51):
            scheduler.step(moment(14, 12))
        self.assertEqual(self.launch.call_count, 50)
        self.assertTrue(scheduler.finished)

    def test_invalid_loop_count(self):
        for count in (0, -1, True, 1.5):
            with self.assertRaises(ValueError):
                daily.DailyScheduler(moment(14), self.launch, count)

    def test_all_seven_arguments_and_validation(self):
        with patch.multiple(daily, NAME="", CATEGORY="", ALL=True, MAX_ARTICLES=3,
                            WORKERS=10, LIMIT=0, LIST_SITES=False):
            self.assertEqual(daily.build_arguments(), ["--all", "--max-articles", "3",
                                                      "--workers", "10", "--limit", "0"])
            with patch.multiple(daily, ALL=False, NAME="测试媒体"):
                self.assertEqual(daily.build_arguments()[:2], ["--name", "测试媒体"])
            with patch.multiple(daily, ALL=False, CATEGORY="地方新闻单位", LIST_SITES=True):
                args = daily.build_arguments()
                self.assertEqual(args[:2], ["--category", "地方新闻单位"])
                self.assertIn("--list-sites", args)
            with patch.object(daily, "WORKERS", 0):
                with self.assertRaises(ValueError):
                    daily.build_arguments()
            with patch.object(daily, "NAME", "不能同时全量和按名称"):
                with self.assertRaises(ValueError):
                    daily.build_arguments()

    def test_resume_saved_url_after_real_process_failure(self):
        import ast
        source = daily.BASE_DIR / "crawler" / "unified_crawler.py"
        tree = ast.parse(source.read_text(encoding="utf-8-sig"))
        manager = next(node for node in tree.body
                       if isinstance(node, ast.ClassDef) and node.name == "VisitedManager")
        # 使用真实去重管理器，模拟存好第一篇后异常退出；重试只保存第二篇。
        manager_source = ast.get_source_segment(source.read_text(encoding="utf-8-sig"), manager)
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as folder:
            folder = Path(folder)
            script = folder / "daily_crawler.py"
            script.write_text(Path(daily.__file__).read_text(encoding="utf-8"), encoding="utf-8")
            crawler = folder / "crawler"
            crawler.mkdir()
            (crawler / "main.py").write_text(
                "import os, sys, threading\nVISITED_FILE = 'visited.txt'\n" + manager_source +
                "\nm = VisitedManager()\n"
                "if m.try_claim('https://test.invalid/first'):\n"
                "    m.mark_success('https://test.invalid/first')\n"
                "    sys.exit(7)\n"
                "assert m.try_claim('https://test.invalid/second')\n"
                "m.mark_success('https://test.invalid/second')\n",
                encoding="utf-8")
            logs = folder / "scheduler_logs"
            logs.mkdir()
            with patch.multiple(daily, __file__=str(script), BASE_DIR=folder,
                                RUNTIME_DIR=logs, CRAWLER_LOCK=logs / "crawler.lock"):
                scheduler = daily.DailyScheduler(moment(14, 12),
                    lambda now: daily.launch_crawler([], now), loop_count=1)
                try:
                    scheduler.step(moment(14, 12))
                    self.assertEqual(scheduler.process.wait(timeout=15), 7)
                    with self.assertLogs(daily.log, level="ERROR"):
                        scheduler.step(moment(14, 12, 1))
                    self.assertEqual(scheduler.completed, 0)
                    self.assertEqual(scheduler.process.wait(timeout=15), 0)
                    scheduler.step(moment(14, 12, 2))
                    self.assertTrue(scheduler.finished)
                finally:
                    if scheduler.process is not None and scheduler.process.poll() is None:
                        scheduler.process.kill()
                        scheduler.process.wait(timeout=5)
            self.assertEqual((folder / "visited.txt").read_text().splitlines(),
                             ["https://test.invalid/first", "https://test.invalid/second"])

    def test_lock_excludes_other_process_and_is_reusable(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as folder:
            path = Path(folder) / "run.lock"
            code = ("import sys; from pathlib import Path; import daily_crawler as d; "
                    "d.CRAWLER_LOCK=Path(sys.argv[1]); sys.exit(d.run_crawler([]))")
            with daily.ProcessLock(path):
                result = subprocess.run([sys.executable, "-c", code, str(path)],
                                        cwd=daily.BASE_DIR, capture_output=True, timeout=15)
                self.assertEqual(result.returncode, daily.BUSY_EXIT_CODE, result.stderr)
            with daily.ProcessLock(path):
                pass

    def test_worker_runs_entrypoint_with_arguments_and_holds_lock(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as folder:
            folder = Path(folder)
            entry = folder / "fake_main.py"
            output = folder / "args.txt"
            lock = folder / "worker.lock"
            # 实际子进程执行本地假入口，绝不导入正式爬虫或访问网络。
            entry.write_text(
                "import sys\nfrom pathlib import Path\nimport daily_crawler as d\n"
                "try:\n    with d.ProcessLock(d.CRAWLER_LOCK):\n"
                "        raise AssertionError('worker lock not held')\n"
                "except d.AlreadyRunning:\n    pass\n"
                "Path(sys.argv[1]).write_text(sys.argv[2], encoding='utf-8')\n",
                encoding="utf-8")
            code = ("import sys; from pathlib import Path; import daily_crawler as d; "
                    "d.ENTRYPOINT=Path(sys.argv[1]); d.CRAWLER_LOCK=Path(sys.argv[2]); "
                    "sys.exit(d.run_crawler(sys.argv[3:]))")
            result = subprocess.run([sys.executable, "-c", code, str(entry), str(lock),
                                     str(output), "中文参数"], cwd=daily.BASE_DIR,
                                    capture_output=True, timeout=15)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(output.read_text(encoding="utf-8"), "中文参数")
            with daily.ProcessLock(lock):
                pass

    def test_launch_complete_chain_with_fake_entrypoint(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as folder:
            folder = Path(folder)
            script = folder / "daily_crawler.py"
            script.write_text(Path(daily.__file__).read_text(encoding="utf-8"), encoding="utf-8")
            crawler_dir = folder / "crawler"
            crawler_dir.mkdir()
            (crawler_dir / "main.py").write_text(
                "import sys, json\nfrom pathlib import Path\n"
                "Path('received.json').write_text(json.dumps(sys.argv[1:]), encoding='utf-8')\n"
                "print('fake crawler finished')\n", encoding="utf-8")
            logs = folder / "scheduler_logs"
            logs.mkdir()
            with patch.multiple(daily, __file__=str(script), BASE_DIR=folder, RUNTIME_DIR=logs):
                process = daily.launch_crawler(["--name", "中文名称"], moment(15))
                try:
                    self.assertEqual(process.wait(timeout=15), 0)
                finally:
                    if process.poll() is None:
                        process.kill()
                        process.wait(timeout=5)
            import json
            self.assertEqual(json.loads((folder / "received.json").read_text()),
                             ["--name", "中文名称"])
            output = next(logs.glob("crawl_*.log")).read_text(encoding="utf-8")
            self.assertIn("fake crawler finished", output)



class MemoryAndContentTests(unittest.TestCase):
    def setUp(self):
        import ast
        import types
        self.tmp = tempfile.TemporaryDirectory(dir=Path(__file__).parent)
        self.addCleanup(self.tmp.cleanup)
        self.folder = Path(self.tmp.name)
        source = daily.BASE_DIR / "crawler" / "unified_crawler.py"
        tree = ast.parse(source.read_text(encoding="utf-8-sig"))
        excluded = {"BASE_DIR", "DATA_DIR", "LOG_FILE", "VISITED_FILE", "visited_mgr"}
        nodes = []
        for node in tree.body:
            if isinstance(node, ast.ImportFrom) and node.module == "sites_registry":
                continue
            if isinstance(node, (ast.Import, ast.ImportFrom, ast.ClassDef, ast.FunctionDef)):
                nodes.append(node)
            elif isinstance(node, ast.Assign) and not any(
                    isinstance(t, ast.Name) and t.id in excluded for t in node.targets):
                nodes.append(node)
        (self.folder / "unified_crawler.py").write_text(source.read_text(encoding="utf-8-sig"), encoding="utf-8")
        self.m = types.ModuleType("isolated_crawler")
        self.m.__dict__.update(__file__=str(self.folder / "unified_crawler.py"), DATA_DIR=str(self.folder), BASE_DIR=str(self.folder),
                               VISITED_FILE=str(self.folder / "visited.txt"))
        exec(compile(ast.Module(body=nodes, type_ignores=[]), str(source), "exec"), self.m.__dict__)
        self.m.visited_mgr = self.m.VisitedManager()
        self.m.DELAY_MIN = self.m.DELAY_MAX = 0
        self.cfg = {"name": "test", "category": "test", "channels": {"general": "https://test.invalid/index"},
                    "content_selectors": ["#content"]}

    def page(self, content):
        return self.m.BeautifulSoup('<h1>Test title</h1><div id="content">' + content + '</div>', 'lxml')

    def test_nested_text_not_multiplied_and_short_repeats_kept(self):
        soup = self.page('开头。<div>直接正文<p>重复。</p><p>重复。</p><div>尾声。</div></div>结束。')
        try:
            text = self.m.UnifiedExtractor.ordered_text(soup.select_one('#content'))
            self.assertEqual(text.splitlines(), ['开头。', '直接正文', '重复。', '重复。', '尾声。', '结束。'])
        finally:
            soup.decompose()

    def test_nested_large_text_preserved_once(self):
        original = '正文ABC' * 20000
        soup = self.page('<div>' * 30 + '<p>' + original + '</p>' + '</div>' * 30)
        try:
            data = self.m.UnifiedExtractor.parse_article(soup, 'https://test.invalid/a', self.cfg)
            self.assertEqual(data['content'], original)
        finally:
            soup.decompose()

    def test_inline_text_order_and_repeated_sentences(self):
        soup = self.page('<p>Hello <b>world</b>! Again.</p><p>Hello <b>world</b>! Again.</p>')
        try:
            self.assertEqual(self.m.UnifiedExtractor.ordered_text(soup.select_one('#content')),
                             'Hello world! Again.\nHello world! Again.')
        finally:
            soup.decompose()

    def test_incremental_encodings_preserve_last_character(self):
        self.m.CHUNK_BYTES = 7
        body = '<h1>中文标题</h1><div id="content">' + ('一段完整正文。' * 50) + '终点</div>'
        for encoding in ('utf-8', 'gb18030', 'utf-16'):
            path = self.folder / 'page-encoding.download'
            path.write_bytes(body.encode(encoding))
            chosen = self.m.choose_encoding(str(path), preferred=encoding)
            soup = self.m.parse_download(str(path), chosen)
            try:
                text = self.m.UnifiedExtractor.ordered_text(soup.select_one('#content'))
                self.assertEqual(text, '一段完整正文。' * 50 + '终点')
            finally:
                soup.decompose()

    def test_parser_does_not_truncate_long_text_node(self):
        path = self.folder / 'page-long.download'
        original = 'x' * (11 * 1024 * 1024) + 'END'
        with path.open('w', encoding='utf-8') as f:
            f.write('<div id="content">')
            f.write(original)
            f.write('</div>')
        soup = self.m.parse_download(str(path), 'utf-8')
        try:
            self.assertEqual(soup.select_one('#content').get_text(), original)
        finally:
            soup.decompose()

    def response(self, chunks):
        response = Mock()
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        response.headers = {'Content-Type': 'text/html; charset=utf-8'}
        response.encoding = 'utf-8'
        response.iter_content.side_effect = lambda **kwargs: iter(chunks)
        # 禁止走回整页 content/text 路径。
        from unittest.mock import PropertyMock
        type(response).content = PropertyMock(side_effect=AssertionError('whole response buffered'))
        type(response).text = PropertyMock(side_effect=AssertionError('whole response decoded'))
        return response

    def test_stream_download_reused_after_interruption(self):
        journal = self.m.TaskJournal()
        key = ('article', 'https://test.invalid/a')
        journal.begin(*key, 'test', 'general')
        fetcher = self.m.UnifiedFetcher(journal)
        session = Mock()
        session.get.return_value = self.response([b'<div id="content">', b'complete body ' * 100, b'END</div>'])
        fetcher._local.session = session
        soup = fetcher.fetch(key[1], task_key=key)
        soup.decompose()
        saved = journal.get(key)
        self.assertTrue(saved['downloaded'])
        self.assertTrue(Path(saved['path']).exists())
        self.assertTrue(session.get.call_args.kwargs['stream'])
        # 模拟未写文章就重启：保留下载，从磁盘重新解析，无第二次网络请求。
        recovered = self.m.TaskJournal()
        self.assertEqual(recovered.get(key)['status'], 'pending')
        recovered.begin(*key, 'test', 'general')
        fetcher.journal = recovered
        soup = fetcher.fetch(key[1], task_key=key)
        try:
            self.assertTrue(soup.get_text().endswith('END'))
        finally:
            soup.decompose()
        session.get.assert_called_once()
        recovered.done(key)
        self.assertFalse(Path(saved['path']).exists())

    def test_repeated_interruptions_deferred_not_visited(self):
        key = ('article', 'https://test.invalid/a')
        journal = self.m.TaskJournal()
        journal.begin(*key, 'test', 'general')
        journal.update(key, phase='parse')
        journal = self.m.TaskJournal()
        self.assertEqual(journal.get(key)['status'], 'pending')
        journal.begin(*key, 'test', 'general')
        journal = self.m.TaskJournal()
        self.assertEqual(journal.get(key)['status'], 'deferred')
        self.assertFalse(journal.begin(*key, 'test', 'general'))
        self.assertNotIn(key[1], self.m.visited_mgr.visited)

    def test_interrupted_download_is_not_marked_complete(self):
        key = ('article', 'https://test.invalid/broken')
        journal = self.m.TaskJournal()
        journal.begin(*key, 'test', 'general')
        fetcher = self.m.UnifiedFetcher(journal)
        response = self.response([])
        def chunks(**kwargs):
            yield b'<html>partial'
            raise OSError('connection interrupted')
        response.iter_content.side_effect = chunks
        fetcher._local.session = Mock()
        fetcher.session.get.return_value = response
        self.m.MAX_RETRIES = 1
        with self.assertLogs(self.m.log, level='WARNING'):
            with self.assertRaises(OSError):
                fetcher.fetch(key[1], task_key=key)
        saved = journal.get(key)
        self.assertFalse(saved['downloaded'])
        self.assertEqual(Path(saved['path']).read_bytes(), b'<html>partial')
        self.assertNotIn(key[1], self.m.visited_mgr.visited)
        response.__exit__.assert_called_once()

    def test_task_status_and_explicit_retry_command(self):
        import io
        from contextlib import redirect_stdout
        data_dir = self.folder / 'crawler' / 'data'
        data_dir.mkdir(parents=True)
        self.m.DATA_DIR = str(data_dir)
        journal = self.m.TaskJournal()
        key = ('article', 'https://test.invalid/deferred')
        journal.begin(*key, 'test', 'general')
        with self.assertLogs(self.m.log, level='ERROR'):
            journal.fail(key, 'bad document')
            journal.fail(key, 'bad document')
        with patch.multiple(daily, BASE_DIR=self.folder,
                            SCHEDULER_LOCK=self.folder / 'scheduler.lock',
                            CRAWLER_LOCK=self.folder / 'crawler.lock'):
            output = io.StringIO()
            with patch.object(sys, 'argv', ['daily_crawler.py', '--task-status']), redirect_stdout(output):
                self.assertEqual(daily.main(), 0)
            self.assertIn(key[1], output.getvalue())
            self.assertEqual(journal.get(key)['status'], 'deferred')
            with patch.object(sys, 'argv', ['daily_crawler.py', '--retry-task', key[1]]), redirect_stdout(io.StringIO()):
                self.assertEqual(daily.main(), 0)
            self.assertEqual(journal.get(key)['status'], 'pending')
            self.assertEqual(journal.get(key)['failures'], 0)

    def seed_download(self, crawler, key, html):
        crawler.tasks.begin(*key, 'test', 'general')
        path = self.folder / ('page-seed-' + str(len(list(self.folder.glob('page-seed-*')))) + '.download')
        path.write_text(html, encoding='utf-8')
        crawler.tasks.update(key, path=str(path), downloaded=1, encoding='utf-8')

    def test_real_page_process_returns_full_article_and_keeps_other_task_running(self):
        crawler = self.m.UnifiedCrawler()
        key = ('article', 'https://test.invalid/article')
        other = ('article', 'https://test.invalid/other')
        self.seed_download(crawler, key, '<h1>Test title</h1><div id="content"><p>' + '正文完整。' * 50 + '</p><p>短句。</p></div>')
        crawler.tasks.begin(*other, 'test', 'general')
        result = crawler.process_page(key, self.cfg, 'general')
        self.assertEqual(result['content'], '正文完整。' * 50 + '\n短句。')
        self.assertEqual(crawler.tasks.get(other)['status'], 'running')
        self.assertFalse(self.m.visited_mgr.visited)
        self.assertFalse(list(self.folder.glob('page-ipc-*')))

    def test_real_channel_process_returns_links(self):
        crawler = self.m.UnifiedCrawler()
        key = ('channel', 'https://test.invalid/index')
        self.seed_download(crawler, key, '<a href="/20260917/article.html">完整文章标题链接</a>')
        links = crawler.process_page(key, self.cfg, 'general')
        self.assertIn('https://test.invalid/20260917/article.html', links)

    def test_memory_limit_reaps_process_and_next_page_can_run(self):
        crawler = self.m.UnifiedCrawler()
        key = ('article', 'https://test.invalid/large')
        self.seed_download(crawler, key, '<h1>Test title</h1><div id="content">' + '正文完整。' * 50 + '</div>')
        self.m.PAGE_MEMORY_LIMIT_MIB = 1
        with self.assertRaises(self.m.PageMemoryExceeded) as caught:
            crawler.process_page(key, self.cfg, 'general')
        with self.assertLogs(self.m.log, level='ERROR'):
            crawler.page_failed(key, caught.exception)
        self.assertEqual(crawler.tasks.get(key)['status'], 'deferred')
        self.assertNotIn(key[1], self.m.visited_mgr.visited)
        self.assertFalse(list(self.folder.glob('page-ipc-*')))
        other = ('article', 'https://test.invalid/small')
        self.seed_download(crawler, other, '<h1>Test title</h1><div id="content">' + '正文完整。' * 50 + '</div>')
        self.m.PAGE_MEMORY_LIMIT_MIB = 100
        self.assertEqual(crawler.process_page(other, self.cfg, 'general')['url'], other[1])

    def test_monitor_terminates_allocating_child(self):
        child = subprocess.Popen([sys.executable, '-c',
            'import time; x=bytearray(80*1024*1024); time.sleep(15)'])
        with self.assertRaises(self.m.PageMemoryExceeded):
            self.m.monitor_page(child, 32*1024*1024)
        self.assertIsNotNone(child.poll())

    def test_crawl_saves_only_complete_worker_results(self):
        import json
        crawler = self.m.UnifiedCrawler()
        url = 'https://test.invalid/article'
        data = {'url':url, 'title':'Test title', 'content':'完整内容。'*30+'\n短句。'}
        with patch.object(crawler, 'process_page', side_effect=[[url], data]):
            self.assertEqual(crawler.crawl_site(self.cfg, 1), 1)
        output = self.folder / 'test' / 'test' / 'general.jsonl'
        self.assertEqual(json.loads(output.read_text(encoding='utf-8')), data)
        self.assertIn(url, self.m.visited_mgr.visited)

    def test_pending_article_runs_even_when_channel_fails(self):
        crawler = self.m.UnifiedCrawler()
        key = ('article', 'https://test.invalid/unfinished')
        crawler.tasks.begin(*key, 'test', 'general')
        with self.assertLogs(self.m.log, level='ERROR'):
            crawler.tasks.fail(key, 'previous download error')
        data = {'url':key[1], 'title':'Test title', 'content':'完整内容。'*30}
        with patch.object(crawler, 'process_page', side_effect=[ValueError('channel failed'), data]):
            with self.assertLogs(self.m.log, level='ERROR'):
                self.assertEqual(crawler.crawl_site(self.cfg, 1), 1)
        self.assertIn(key[1], self.m.visited_mgr.visited)

    def test_overlimit_article_deferred_and_next_article_saved(self):
        crawler = self.m.UnifiedCrawler()
        first, second = 'https://test.invalid/large', 'https://test.invalid/small'
        data = {'url':second, 'title':'Test title', 'content':'完整内容。'*30}
        with patch.object(crawler, 'process_page', side_effect=[[first,second], self.m.PageMemoryExceeded('quota'), data]):
            with self.assertLogs(self.m.log, level='ERROR'):
                self.assertEqual(crawler.crawl_site(self.cfg, 1), 1)
        self.assertEqual(crawler.tasks.get(('article',first))['status'], 'deferred')
        self.assertNotIn(first, self.m.visited_mgr.visited)
        self.assertIn(second, self.m.visited_mgr.visited)
        self.assertFalse(self.m.visited_mgr.in_progress)

if __name__ == "__main__":
    unittest.main()
