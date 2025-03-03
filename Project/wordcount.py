import json
import os
from collections import Counter
import matplotlib.pyplot as plt
from wordcloud import WordCloud

# 전역 변수 설정
INPUT_DIR = 'script'
OUTPUT_DIR = 'wordcloud_output'
STOPWORDS = {"너무", "진짜", "그냥", "좀", "더", "이건", "이게", "ㅋㅋ", "근데", "아니", "왜", "이거", "걍", "이제", "한", "것"}
STOPWORDS_FILE = 'mapping_data/webtoon_stopwords.json'
FONT_PATH = 'fonts/NanumGothic.ttf'

os.makedirs(OUTPUT_DIR, exist_ok=True)

def load_comments(title, episode):
    """지정된 웹툰과 에피소드의 댓글 데이터를 불러오기"""
    file_path = INPUT_DIR
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # 독자 유형별 댓글 분류
    positive_comments = [comment['text'] for comment in data['comments'] if comment['sentiment_score'] >= 0.4 and '찜' not in comment['text']]
    negative_comments = [comment['text'] for comment in data['comments'] if comment['sentiment_score'] <= -0.6 and '찜' not in comment['text']]

    return positive_comments, negative_comments

def load_webtoon_stopwords(title):
    """해당 웹툰에 설정된 불용어 불러오기"""
    with open(STOPWORDS_FILE, 'r', encoding='utf-8') as f:
        stopwords_data = json.load(f)

    return set(stopwords_data.get(title, {}).get("stopwords", []))

def preprocess_text(comments, webtoon_stopwords):
    """댓글 텍스트 전처리 및 불용어 제거"""
    all_text = ' '.join(comments)
    words = all_text.split()
    filtered_words = [word for word in words if word not in STOPWORDS.union(webtoon_stopwords)]
    return filtered_words

def generate_combined_wordcloud(title, episode):
    """일반 독자와 충성 독자의 워드 클라우드를 하나의 이미지로 결합"""

    # 댓글 데이터 로드 및 전처리
    positive_comments, negative_comments = load_comments(title, episode)
    webtoon_stopwords = load_webtoon_stopwords(title)
    positive_words = preprocess_text(positive_comments, webtoon_stopwords)
    negative_words = preprocess_text(negative_comments, webtoon_stopwords)
    
    # 긍정 댓글 워드클라우드 생성
    positive_word_counts = Counter(positive_words)
    positive_wordcloud = WordCloud(
        width=600, 
        height=600, 
        background_color='white',
        font_path=FONT_PATH
    ).generate_from_frequencies(positive_word_counts)

    # 부정 댓글 워드클라우드 생성
    negative_word_counts = Counter(negative_words)
    negative_wordcloud = WordCloud(
        width=600, 
        height=600, 
        background_color='white',
        font_path=FONT_PATH
    ).generate_from_frequencies(negative_word_counts)

    # 플롯 설정
    plt.figure(figsize=(14, 7))

    # 긍정 댓글 워드클라우드
    plt.subplot(1, 2, 1)
    plt.imshow(positive_wordcloud, interpolation='bilinear')
    plt.title('Positive comments Word Cloud')
    plt.axis('off')

    # 부정 댓글 워드클라우드
    plt.subplot(1, 2, 2)
    plt.imshow(negative_wordcloud, interpolation='bilinear')
    plt.title('Negative comments Word Cloud')
    plt.axis('off')

    # 저장 및 출력
    output_file = os.path.join(OUTPUT_DIR, f"{title}_{episode}_combined_wordcloud.png")
    plt.savefig(output_file)
    plt.show()
    print(f"워드 클라우드가 {output_file}로 저장되었습니다.")

def main():
    title = '퀘스트지상주의'
    episode = 153

    # 워드 클라우드 생성 및 저장
    generate_combined_wordcloud(title, episode)

if __name__ == '__main__':
    main()