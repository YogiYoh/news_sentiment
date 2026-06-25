import os
import time
import html
import requests
import feedparser
import trafilatura

import pandas as pd
from dotenv import load_dotenv
from googlenewsdecoder import gnewsdecoder


import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

GUARDIAN_KEY = os.environ["GUARDIAN_KEY"]
NYTIMES_KEY = os.environ["NYTIMES_KEY"]

user = os.environ["user"]
password = os.environ["password"]
account = os.environ["account"]
warehouse = os.environ["warehouse"]
database = os.environ["database"]
schema = os.environ["schema"]

def to_table(df, table_name):
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_localize(None)

    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema,
    )

    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name=table_name,
        auto_create_table=True,
        use_logical_type=True,
    )

    print(f"Successfully loaded {nrows} rows.")
    conn.close()



def extract_guardian():
    response = requests.get(
        "https://content.guardianapis.com/search",
        params={
            "api-key": GUARDIAN_KEY,
            "show-fields": "trailText",
            "section": "politics"
        }
    )

    response.raise_for_status()
    data = response.json()

    data_json = response.json()
    df = pd.json_normalize(data_json["response"]["results"])

    df = df.rename(columns={
        "webTitle": "headline",
        "webUrl": "link",
        "fields.trailText": "body",
        "webPublicationDate": "timestamp",
    })
    df["source"] = "guardian"
    df = df[["headline", "link", "body", "timestamp", "source"]]

    print(df.head())
    
    
    to_table(df, "raw.guardian_articles")
    
    return df

def extract_nytimes(target=100):
    results = []
    page = 0

    while len(results) < target:
        response = requests.get(
            "https://api.nytimes.com/svc/search/v2/articlesearch.json",
            params={
                "api-key": NYTIMES_KEY,
                "q": "politics",
                "sort": "newest",
                "page": page,
            }
        )
        if response.status_code == 429:
            print("Rate limited by NYT, waiting 60s...")
            time.sleep(60)
            continue
        response.raise_for_status()
        docs = response.json()["response"]["docs"]

        if not docs:
            break
        results.extend(docs)
        page += 1
        time.sleep(12)

    df = pd.json_normalize(results[:target])
    df = df.rename(columns={
        "headline.main": "headline",
        "web_url": "link",
        "pub_date": "timestamp",
    })
    # abstract is often empty; fall back to lead_paragraph when it is
    df["body"] = df["abstract"].where(
        df["abstract"].str.strip().ne(""), df.get("lead_paragraph", "")
    )
    df["source"] = "nytimes"
    df = df[["headline", "link", "body", "timestamp", "source"]]
    df = df[df["body"].str.strip().ne("")]  # drop rows with no usable body

    print(df.head())

    to_table(df, "raw.nytimes_articles")
    
    return df

def extract_rss(url, source_name, table_name):
    feed = feedparser.parse(url)

    df = pd.DataFrame(feed.entries)
    df = df.rename(columns={
        "title": "headline",
        "link": "link",
        "summary": "body",
        "published": "timestamp",
    })
    df["source"] = source_name
    df = df[["headline", "link", "body", "timestamp", "source"]]
    df["body"] = df["body"].apply(lambda x: html.unescape(x) if isinstance(x, str) else x)

    print(df.head())

    to_table(df, table_name)

    return df


def extract_google_news_rss(query, source_name, table_name):
    # Google News RSS only gives a snippet (an HTML anchor pointing back at
    # the obfuscated Google News link) instead of real article text, so we
    # decode each entry's link to the publisher's real URL and scrape the
    # full body from there.
    feed = feedparser.parse(
        f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
    )

    rows = []
    for entry in feed.entries:
        decoded = gnewsdecoder(entry.link, interval=1)
        if not decoded.get("status"):
            continue
        real_url = decoded["decoded_url"]

        downloaded = trafilatura.fetch_url(real_url)
        body = trafilatura.extract(downloaded) if downloaded else None
        if not body:
            continue

        rows.append({
            "headline": entry.title,
            "link": real_url,
            "body": body,
            "timestamp": entry.published,
            "source": source_name,
        })

    df = pd.DataFrame(rows, columns=["headline", "link", "body", "timestamp", "source"])

    print(df.head())

    to_table(df, table_name)

    return df


def extract_bbc():
    return extract_rss(
        "http://feeds.bbci.co.uk/news/politics/rss.xml", "bbc", "raw.bbc_articles"
    )


def extract_foxnews():
    return extract_rss(
        "https://feeds.foxnews.com/foxnews/politics", "foxnews", "raw.foxnews_articles"
    )


def extract_cnn():
    # CNN's own RSS feeds (rss.cnn.com) are abandoned and stuck serving
    # years-old articles, so we use Google News RSS scoped to cnn.com instead.
    return extract_google_news_rss("site:cnn.com+politics", "cnn", "raw.cnn_articles")


def extract_npr():
    # Reuters discontinued its public RSS feeds in 2020 and blocks scraping
    # (401s even with a decoded Google News link), so NPR's politics feed is
    # used as the neutral wire-style replacement instead.
    return extract_rss(
        "https://feeds.npr.org/1014/rss.xml", "npr", "raw.npr_articles"
    )

if __name__ == "__main__":
    guardian_df = extract_guardian()
    nytimes_df = extract_nytimes()
    bbc_df = extract_bbc()
    foxnews_df = extract_foxnews()
    cnn_df = extract_cnn()
    npr_df = extract_npr()

    combined = pd.concat(
        [guardian_df, nytimes_df, bbc_df, foxnews_df, cnn_df, npr_df],
        ignore_index=True,
    )
    print(combined.shape)