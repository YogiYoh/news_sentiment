import os
from openai import OpenAI
from dotenv import load_dotenv

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
user = os.environ["user"]
password = os.environ["password"]
account = os.environ["account"]
warehouse = os.environ["warehouse"]
database = os.environ["database"]
schema = os.environ["schema"]

def score_article(headline, content):
    client = OpenAI()

    response = client.responses.create(
        model="gpt-4.1-mini",
        input=[
            {
                "role": "system",
                "content": (
                    "You are a political news sentiment analyzer. "
                    "Given a news article headline and body excerpt, return ONLY a single float "
                    "between -1.0 and 1.0 representing the sentiment of the article's tone. "
                    "-1.0 = strongly negative/critical, 0.0 = neutral/factual, 1.0 = strongly positive/favorable. "
                    "Return nothing else — no explanation, no words, just the number."
                ),
            },
            {
                "role": "user",
                "content": f"Headline: {headline}\n\nBody: {str(content)[:500]}",
            },
        ],
    )

    sentiment_score = float(response.output_text.strip())
    return sentiment_score
    
    
def to_table(df, table_name):
    df["scored_at"] = pd.to_datetime(df["scored_at"], utc=True).dt.tz_localize(None)

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

def fetch_table(table_name1, table_name2=None):
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema,
    )

    cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT s.* FROM {table_name1} s
            LEFT JOIN {table_name2} a ON s."link" = a."link"
            WHERE a."link" IS NULL
        """)
    except Exception:
        cursor.execute(f"SELECT * FROM {table_name1}")
    
    df = cursor.fetch_pandas_all()  
    cursor.close()
    conn.close()
    
    return df

if __name__ == "__main__":
    df = fetch_table("SILVER.SILVER_ARTICLES", "ARTICLE_SENTIMENT")

    results = []
    for _, row in df.iterrows():
        try:
            score = score_article(row["headline"], row["body"])
            score = max(-1.0, min(1.0, score))
            results.append({
                "link": row["link"],
                "sentiment_score": score,
                "scored_at": pd.Timestamp.now(tz="UTC").tz_convert(None),
            })
            print(f"Scored: {row['headline'][:60]} → {score}")
        except Exception as e:
            print(f"Skipped: {e}")

    scores_df = pd.DataFrame(results)

    if scores_df.empty:
        print("No articles scored successfully.")
    else:
        to_table(scores_df, "ARTICLE_SENTIMENT")
