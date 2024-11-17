from reddit_api import SubRedditDataAPI
import spacy
from collections import Counter

subreddit_data = SubRedditDataAPI().get_reddit_request("dataengineering")
submission_limit = 1000
top_submissions = subreddit_data.top(limit=submission_limit)

nlp = spacy.load("en_core_web_sm")

# List to hold tech-related entities
top_topics = []

custom_stopwords = {"Data Engineering", "Data Engineer", "DE"}

for submission in top_submissions:
    # Combine title and selftext for entity extraction
    text = f"{submission.title} {submission.selftext}"
    doc = nlp(text)

    # Extract ORG and PRODUCT entities
    entities = [ent.text for ent in doc.ents if ent.label_ in {"ORG", "PRODUCT"} and ent.text not in custom_stopwords]
    top_topics.extend(entities)

# Count the frequency of each entity
entity_counts = Counter(top_topics)

# Get the top 15 most frequent ORG and PRODUCT entities
top_20_entities = entity_counts.most_common(20)

# Print the top 15 entities
for entity, count in top_20_entities:
    print(f"{entity}: {count}")