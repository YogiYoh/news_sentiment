import os
import requests
import feedparser

import pandas as pd
from dotenv import load_dotenv


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

def extract_nytimes():
    response = requests.get(
        "https://api.nytimes.com/svc/search/v2/articlesearch.json",
        params={
            "api-key": NYTIMES_KEY,
            "q": "politics"
        }
    )
    
    response.raise_for_status()
    
    data = response.json()
    data_json = response.json()
    
    df = pd.json_normalize(data_json["response"]["docs"])
    
    df = df.rename(columns={
        "headline.main": "headline",
        "web_url": "link",
        "abstract": "body",
        "pub_date": "timestamp",
    })
    df["source"] = "nytimes"
    df = df[["headline", "link", "body", "timestamp", "source"]]

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
    return extract_rss(
        "https://news.google.com/rss/search?q=site:cnn.com+politics&hl=en-US&gl=US&ceid=US:en",
        "cnn",
        "raw.cnn_articles",
    )


def extract_reuters():
    # Reuters discontinued its public RSS feeds in 2020, so we use Google
    # News RSS scoped to reuters.com as a stand-in feed of Reuters articles.
    return extract_rss(
        "https://news.google.com/rss/search?q=site:reuters.com+politics&hl=en-US&gl=US&ceid=US:en",
        "reuters",
        "raw.reuters_articles",
    )

if __name__ == "__main__":
    guardian_df = extract_guardian()
    nytimes_df = extract_nytimes()
    bbc_df = extract_bbc()
    foxnews_df = extract_foxnews()
    cnn_df = extract_cnn()
    reuters_df = extract_reuters()

    combined = pd.concat(
        [guardian_df, nytimes_df, bbc_df, foxnews_df, cnn_df, reuters_df],
        ignore_index=True,
    )
    print(combined.shape)