import news_extract
import news_score

import pandas as pd
import subprocess

def extract():
    guardian_df = news_extract.extract_guardian()
    nytimes_df = news_extract.extract_nytimes()
    bbc_df = news_extract.extract_bbc()
    foxnews_df = news_extract.extract_foxnews()
    cnn_df = news_extract.extract_cnn()
    npr_df = news_extract.extract_npr()

    combined = pd.concat(
        [guardian_df, nytimes_df, bbc_df, foxnews_df, cnn_df, npr_df],
        ignore_index=True,
    )
    print(combined.shape)

def score():
    df = news_score.fetch_table("SILVER.SILVER_ARTICLES", "ARTICLE_SENTIMENT")

    results = []
    for _, row in df.iterrows():
        try:
            score = news_score.score_article(row["headline"], row["body"])
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
        news_score.to_table(scores_df, "ARTICLE_SENTIMENT")

def main():
    extract()                                                                  
    subprocess.run(["dbt", "source", "freshness"], cwd="news_sentiment", check=True)  
    subprocess.run(["dbt", "run", "--select", "bronze"], cwd="news_sentiment", check=True)         
    subprocess.run(["dbt", "run", "--select", "silver"], cwd="news_sentiment", check=True)  
    score()                                                                    
    subprocess.run(["dbt", "run", "--select", "gold"], cwd="news_sentiment", check=True)  
    subprocess.run(["dbt", "test"], cwd="news_sentiment", check=True)        


if __name__ == "__main__":
    main()
