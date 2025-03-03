from bs4 import BeautifulSoup
import requests
import urllib.parse
import time
import re
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
import datetime
import smart_open
import random

BASIC_URL = "https://www.clien.net/service/search?q={query}&sort=recency&p={page_num}&boardCd=&isBoard=false"
CLIEN_URL = "https://www.clien.net"
REQUEST_REST = 0

# 데이터 저장 경로 설정
s3 = boto3.client("s3")
bucket_name = "mysamplebucket001036"
data_dir = "clienTest/"  # 디렉토리 경로

query = "벤츠 배터리 화재"
page = 0

posts_url= []

def lambda_handler(event, context):

    for p in range(2):
        full_url = BASIC_URL.format(query=urllib.parse.quote(query), page_num=p)

        REQUEST_REST = 0 + random.random()
        response = requests.get(full_url, allow_redirects=False)
        if response.status_code != 200:
            print("status code is not 200")
            continue
        soup = BeautifulSoup(response.content, "html.parser",)
        posts_raw = soup.find_all("a", class_="subject_fixed")

        for link in posts_raw:
            print(link.text)
            posts_url.append((link["href"]))
        
        time.sleep(REQUEST_REST)

    all_posts = []
    all_comments = []

    for post_url in posts_url[:10]:
        REQUEST_REST = 0 + random.random()
        full_url = CLIEN_URL + post_url
        headers = {
            "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
        }
        
        response = requests.get(full_url, headers=headers, allow_redirects=False)
        if response.status_code != 200:
            print(f"{full_url} - Status Code: {response.status_code}")
            continue

        soup = BeautifulSoup(response.content, "html.parser")

        # 게시글 정보 수집
        post_data = {
            "platform": "clien",
            "keyword": query,
            "post_id": soup.find("input", id="boardSn").get("value"),
            "title": soup.find("h3", class_="post_subject").find_all("span")[0].text,
            "url": full_url,
            "content": soup.find("div", class_="post_article").get_text(separator="\n", strip=True),
            "hits": soup.find("div", class_="post_author").find("span", class_="view_count").find("strong").text,
            "created_at": re.search(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", soup.find("span", class_="view_count date").text).group(0),
            "like_count": soup.find("a", class_="symph_count").find("strong").text if soup.find("a", class_="symph_count") else "0",
            "dislike_count": "0",
            "comment_count": soup.find("a", class_="post_reply").find("span").text if soup.find("a", class_="post_reply") else "0"
        }
        
        all_posts.append(post_data)

        # 댓글 정보 수집
        comments_raw = soup.find_all("div", class_="comment_row")
        
        for row in comments_raw:
            if "blocked" in row.get("class", []):
                continue  # 차단된 댓글 제외

            comment_content = row.find("div", class_="comment_view").get_text(separator="\n", strip=True)
            comment_created_at = row.find("span", class_="timestamp").get_text(strip=True)
            comment_created_at = re.search(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", comment_created_at).group(0)

            comment_like = row.find("div", class_="comment_content_symph")
            comment_like = comment_like.find("strong").text if comment_like else "0"

            comment_data = {
                "post_id": post_data["post_id"],
                "comment_content": comment_content,
                "comment_created_at": comment_created_at,
                "comment_like": comment_like,
                "comment_dislike": None
            }
            all_comments.append(comment_data)

        print(f"{full_url} - Done!")
        time.sleep(REQUEST_REST)

    # Parquet 저장
    post_table = pa.Table.from_pydict({key: [d[key] for d in all_posts] for key in all_posts[0]})
    comments_table = pa.Table.from_pydict({key: [d[key] for d in all_comments] for key in all_comments[0]})

    try: 
        with smart_open.open(f"s3://{bucket_name}/{data_dir}posts.parquet", "wb") as s3_file:
            pq.write_table(post_table, s3_file)
        with smart_open.open(f"s3://{bucket_name}/{data_dir}comments.parquet", "wb") as s3_file:
            pq.write_table(comments_table, s3_file)
    except Exception as e:
        print("error:", e)
    print("모든 데이터 Parquet 저장 완료!")