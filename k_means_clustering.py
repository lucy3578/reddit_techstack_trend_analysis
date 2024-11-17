from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import Word2Vec
from collections import Counter
import spacy
from reddit_api import SubRedditDataAPI

# Get the top submissions from the DataEngineering subreddit
subreddit_data = SubRedditDataAPI().get_reddit_request("dataengineering")
submission_limit = 1000
top_submissions = subreddit_data.top(limit=submission_limit)

# Load spaCy model for NER
nlp = spacy.load("en_core_web_sm")

# List to hold tech-related entities
topics = []

# Custom stopwords for exclusion
custom_stopwords = {"Data Engineering", "Data Engineer", "DE"}

# Process the submissions to extract entities
for submission in top_submissions:
    # Combine title and selftext for entity extraction
    text = f"{submission.title} {submission.selftext}"
    doc = nlp(text)

    # Extract ORG and PRODUCT entities (ignoring custom stopwords)
    entities = [ent.text for ent in doc.ents if ent.label_ in {"ORG", "PRODUCT"} and ent.text not in custom_stopwords]
    topics.extend(entities)

# Count the frequency of each entity
entity_counts = Counter(topics)

# Get the top 100 most frequent entities
top_entities = [entity for entity, _ in entity_counts.most_common(500)]
top_50_entities = [entity for entity, _ in entity_counts.most_common(50)]

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KMeans Clustering") \
    .getOrCreate()

# Create a DataFrame from the top entities
df = spark.createDataFrame([(word,) for word in top_entities], ["word"])

# Group all words into a single list (document) for Word2Vec
df_grouped = df.withColumn("text", functions.array("word"))

# Initialize and train the Word2Vec model
word2Vec = Word2Vec(vectorSize=3, minCount=0, inputCol="text", outputCol="result")

# Fit the model with the grouped data
model = word2Vec.fit(df_grouped)
result = model.transform(df_grouped)

# Apply KMeans clustering on the Word2Vec vectors
kmeans = KMeans(k=3, seed=1, featuresCol="result", predictionCol="cluster")
kmeans_model = kmeans.fit(result)

# Make predictions (assign clusters) to each word vector
predictions = kmeans_model.transform(result)

# Filter predictions to include only words in the top_50_entities list
filtered_predictions = predictions.filter(col("word").isin(top_50_entities))
filtered_predictions = filtered_predictions.distinct()

# Show the filtered results with word, vectors, and cluster IDs, convert the DataFrame to Pandas
df_pandas = filtered_predictions.select("word", "result", "cluster").toPandas()

# Save to CSV locally
df_pandas.to_csv("data_eng_keywords_clustering_updated.csv", index=False)
