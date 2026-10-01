#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全量新闻媒体注册中心 (Full Media Registry)
基于国家网信办《互联网新闻信息稿源单位名单》（2025版，1451家全量数据）。
彻底收录并支持：
  1. 中央新闻网站和重点理论网站 (全量 31 家)
  2. 中央新闻单位报刊网站 (全量 67 家)
  3. 部委群团报刊网站 (全量 117 家)
  4. 其他单位报刊网站 (全量 18 家)
  5. 地方新闻网站 (全量 513 家)
  6. 地方新闻单位及各级平台 (全量 573 + 116 家)
总计收录 1435 家合法合规媒体单位，无任何遗漏！
"""

import os
import re
import json
from urllib.parse import urlparse

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MANIFEST_FILE = os.path.join(BASE_DIR, "all_media_manifest.json")

# 精细化频道配置映射表（对具有成熟子频道的龙头媒体提供专项频道支持，其余媒体自动采用全站自适应扫描）
DETAILED_CHANNELS = {
    "人民网": {
        "首页": "http://www.people.com.cn/",
        "时政": "http://politics.people.com.cn/",
        "国际": "http://world.people.com.cn/",
        "经济科技": "http://finance.people.com.cn/",
        "社会法治": "http://society.people.com.cn/",
        "文旅体育": "http://ent.people.com.cn/",
        "军事": "http://military.people.com.cn/",
        "观点": "http://opinion.people.com.cn/",
        "教育": "http://edu.people.com.cn/",
        "健康生活": "http://health.people.com.cn/",
    },
    "新华网": {
        "首页": "https://www.news.cn/",
        "时政": "https://www.news.cn/politics/",
        "国际": "https://www.news.cn/world/",
        "财经": "https://www.news.cn/fortune/",
        "科技": "https://www.news.cn/tech/",
        "文娱": "https://www.news.cn/culture/",
        "军事": "https://www.news.cn/mil/",
        "体育": "https://sports.news.cn/",
        "教育": "https://education.news.cn/",
        "法治": "https://www.news.cn/legal/",
        "理论": "https://www.news.cn/theory/",
        "健康": "https://www.news.cn/health/",
    },
    "中国新闻网": {
        "首页": "https://www.chinanews.com.cn/",
        "国内": "https://www.chinanews.com.cn/china/",
        "国际": "https://www.chinanews.com.cn/world/",
        "社会": "https://www.chinanews.com.cn/society/",
        "财经": "https://www.chinanews.com.cn/finance/",
        "文娱": "https://www.chinanews.com.cn/entertainment/",
        "体育": "https://www.chinanews.com.cn/sports/",
    },
    "光明网": {
        "首页": "https://www.gmw.cn/",
        "时政": "https://politics.gmw.cn/",
        "国际": "https://world.gmw.cn/",
        "理论": "https://theory.gmw.cn/",
        "经济": "https://economy.gmw.cn/",
        "文化": "https://culture.gmw.cn/",
        "科技": "https://tech.gmw.cn/",
    },
    "央广网": {
        "首页": "https://www.cnr.cn/",
        "国内": "https://china.cnr.cn/",
        "财经": "https://finance.cnr.cn/",
        "军事": "https://military.cnr.cn/",
        "科技": "https://tech.cnr.cn/",
        "文化": "https://ent.cnr.cn/",
    },
    "中国经济网": {
        "首页": "http://www.ce.cn/",
        "宏观经济": "http://www.ce.cn/macro/",
        "产业市场": "http://www.ce.cn/cysc/",
        "金融证券": "http://finance.ce.cn/",
        "国际经济": "http://intl.ce.cn/",
        "科教文化": "http://www.ce.cn/kjwh/",
    },
    "央视网": {
        "新闻": "https://news.cctv.com/",
        "国内": "https://news.cctv.com/china/",
        "国际": "https://news.cctv.com/world/",
        "财经": "https://jingji.cctv.com/",
        "军事": "https://military.cctv.com/",
    },
    "中国网": {
        "新闻": "http://news.china.com.cn/",
        "时政": "http://news.china.com.cn/node_7115409.htm",
        "国际": "http://news.china.com.cn/node_7115410.htm",
        "财经": "http://finance.china.com.cn/",
        "观点": "http://opinion.china.com.cn/",
    },
    "中国青年网": {
        "资讯": "https://news.youth.cn/",
        "国内": "https://news.youth.cn/gn/",
        "国际": "https://news.youth.cn/gj/",
        "青年": "https://qclz.youth.cn/",
    },
    "环球网": {
        "国际": "https://world.huanqiu.com/",
        "国内": "https://china.huanqiu.com/",
        "军事": "https://mil.huanqiu.com/",
        "财经": "https://finance.huanqiu.com/",
        "科技": "https://tech.huanqiu.com/",
    },
    "证券时报网": {
        "快讯": "https://www.stcn.com/article/list/kuaixun.html",
        "时事": "https://www.stcn.com/article/list/yaowen.html",
        "金融": "https://www.stcn.com/article/list/jinrong.html",
    },
    "经济参考网": {
        "首页": "http://www.jjckb.cn/",
        "宏观": "http://www.jjckb.cn/yw/index.htm",
        "产经": "http://www.jjckb.cn/cj/index.htm",
    },
    "中国证券网": {
        "要闻": "https://www.cnstock.com/yaowen",
        "公司": "https://www.cnstock.com/company",
        "金融": "https://www.cnstock.com/finance",
    },
    "中证网": {
        "首页": "https://www.cs.com.cn/",
        "要闻": "https://www.cs.com.cn/ssp/01/",
        "产经": "https://www.cs.com.cn/cj/",
    },
    "半月谈网": {
        "首页": "http://www.banyuetan.org/",
        "时政": "http://www.banyuetan.org/dianji/index.html",
        "教育": "http://www.banyuetan.org/jy/index.html",
    },
}

def load_manifest():
    if not os.path.exists(MANIFEST_FILE):
        return []
    with open(MANIFEST_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

# 全量媒体注册中心字典
MEDIA_SITES = {}

def build_registry():
    try:
        from domain_mapper import DomainMapper, GOV_OFFICIAL_DOMAINS, LOCAL_OFFICIAL_DOMAINS
    except ImportError:
        from crawler.domain_mapper import DomainMapper, GOV_OFFICIAL_DOMAINS, LOCAL_OFFICIAL_DOMAINS

    raw_data = load_manifest()
    for item in raw_data:
        name = item["name"]
        clean_name = name.replace("\n", "")
        cat = item["category"]
        url = item.get("url") or item.get("raw_url") or ""

        # 统一由 DomainMapper 及其权威映射池解析真实合法域名
        resolved_url, src_type, is_fallback = DomainMapper.resolve(name, cat, url)
        url = resolved_url
        
        # 针对有具体子频道的站点
        if name in DETAILED_CHANNELS:
            channels = DETAILED_CHANNELS[name]
        else:
            channels = {"综合": url} if url else {}
            
        # 智能通用文章链接匹配模式
        parsed = urlparse(url)
        domain_pattern = re.escape(parsed.netloc.replace("www.", "")) if parsed.netloc else ""
        if domain_pattern:
            url_pattern = rf'{domain_pattern}/.*(\d{{4}}[/\-_]\d{{2}}|\d{{8}}|content_\d+|t\d+_\d+|article|/c\.|/c\d+-)'
        else:
            url_pattern = r'.*(\d{4}[/\-_]\d{2}|\d{8}|content_\d+|t\d+_\d+|article|/c\.|/c\d+-)'

        MEDIA_SITES[name] = {
            "name": name,
            "category": cat,
            "sub_category": item.get("sub_category"),
            "home_url": url,
            "channels": channels,
            "url_pattern": url_pattern,
            "encoding": "utf-8" if "cnr.cn" not in url and "youth.cn" not in url else "gbk",
            "title_selectors": ["h1", ".title", ".article-title", "title", ".main-title"],
            "content_selectors": [
                "#rwb_zw", ".detail", ".left_zw", ".u-mainText", ".article-body",
                "#content", ".main-arti", ".TRS_Editor", "#articleText", ".content",
                ".article-content", ".article", ".text", "#Content", ".cnt_bd",
                "#UCAP-CONTENT", "#zoom", ".pages_content", ".news-content", "#ozoom",
                ".articleCont", ".con_txt", ".article-detail"
            ],
        }

build_registry()

def get_all_sites():
    """获取全量 1435 家媒体注册字典"""
    return MEDIA_SITES

def get_news_media_sites():
    """获取所有新闻网站（中央重点31 + 中央报刊67 + 部委群团117 + 地方新闻513，共746家）"""
    target_cats = {
        "中央新闻网站和重点理论网站",
        "中央新闻单位报刊网站",
        "部委群团报刊网站",
        "其他单位报刊网站",
        "地方新闻网站",
    }
    return {k: v for k, v in MEDIA_SITES.items() if v["category"] in target_cats}

def get_sites_by_category(category):
    """根据大类筛选媒体"""
    return {k: v for k, v in MEDIA_SITES.items() if v["category"] == category}

def list_all_categories():
    """列出全部可用分类"""
    cats = set(v["category"] for v in MEDIA_SITES.values())
    return sorted(list(cats))

if __name__ == "__main__":
    print(f"全量媒体注册中心初始化完成！收录媒体总数: {len(MEDIA_SITES)} 家")
    for cat in list_all_categories():
        matched = [k for k, v in MEDIA_SITES.items() if v["category"] == cat]
        print(f"  - 【{cat}】: {len(matched)} 家")
    news_only = get_news_media_sites()
    print(f"\n其中纯新闻网站（中央+报刊+地方新闻网）总计: {len(news_only)} 家！")
