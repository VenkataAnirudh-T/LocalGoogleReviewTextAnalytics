import nltk
from nltk.corpus import stopwords
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import lit,udf,when,col
from textblob import TextBlob
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from datetime import datetime
from gensim.parsing.preprocessing import remove_stopwords
import re
from wordcloud import WordCloud
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation as LDA
import pandas as pd
from pyspark.sql import functions as f

# Configure Spark Context
conf= SparkConf().setAppName("GoogleApp").setMaster("local")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)
#sql_c = SQLContext(sc)

# Show Spark Session
print(sc.uiWebUrl)

# Define Variables
lemmatizer = nltk.wordnet.WordNetLemmatizer()
siaObject = SentimentIntensityAnalyzer()

# Create Schema to Read Raw User review file
user_schema = StructType([
StructField("ID", IntegerType(), True),
StructField("Rating", DecimalType(10), True),
StructField("ReviewerName", StringType(), True),
StructField("ReviewerText", StringType(), True),
StructField("Lang", StringType(), True),
StructField("Categories", StringType(), True),
StructField("gPlusPlaceId", 	DecimalType(21), True),
StructField("UnixReviewTime", IntegerType(), True),
StructField("ReviewTime", StringType(), True),
StructField("gPlusUserId", 	DecimalType(21), True)])

# Read Raw File and load it into data frame
df= spark.read.schema(user_schema).option("header","True").csv('D:/670/GoogleReviewsData/CleanFile.csv')
print('File read complete: ', datetime.now())

# Filter English Language Text Only
df_en=df.filter(col('Lang')=="en")

# Create Method to get polarity values from Textblob library
blob_polarity_text_udf = udf(lambda x: blob_polarity_text(x), StringType())
def blob_polarity_text(text):
    try:
        blob=TextBlob(text)
        return str(round(blob.polarity,2))+'/' + str(round(blob.subjectivity,2))  #TextBlob(text).polarity
    except:
        return 'NA/NA'

# Create Method to get polarity values from Vader library
sentiment_analyzer_udf = udf(lambda x: sentiment_analyzer(x), StringType())
def sentiment_analyzer(text):
    try:
        blob=siaObject.polarity_scores(text)
        return str(round(blob['neg'],2))+'/' + str(round(blob['neu'],2)) + '/' + str(round(blob['pos'],2)) + '/' + str(round(blob['compound'],2))  #TextBlob(text).polarity
    except:
        return 'NA/NA/NA/NA'

# Text Cleaning Method
blob_cleanse_text_udf = udf(lambda x: blob_cleanse_text(x), StringType())
def blob_cleanse_text(text):
    lower_text = text.lower()
    text_rm_punc = re.sub(r'[^\w\s]', '', lower_text)
    text_rm_num = re.sub(" \d+", " ", text_rm_punc)
    text_remove_sw = remove_stopwords(text_rm_num)
    tokenized_text = nltk.word_tokenize(text_remove_sw)
    lemmatized_text = ' '.join([lemmatizer.lemmatize(word) for word in tokenized_text])
    return lemmatized_text

print('Begin Cleansing Text: ', datetime.now())
df_cleanse = df_en.withColumn('FilteredText', blob_cleanse_text_udf(df_en.ReviewerText))
print("Cleansing Text Complete: ", datetime.now())

print('Showing Cleaned Text Begin: ', datetime.now())
print(df_cleanse.show())
print('Showing Cleaned Text Finished: ', datetime.now())

# Analyze the sentiment and add polarity to Columns
print('Begin Analyzing  Sentiment',datetime.now())
df_cleanse=df_cleanse.withColumn('BlobPolarity',blob_polarity_text_udf(df_en.FilteredText)).\
    withColumn('SentimentAnalyzer',sentiment_analyzer_udf(df_en.FilteredText))
print("Analyzing Sentiment Complete",datetime.now())

print('Word Cloud Started: ', datetime.now())
long_string = ','.join(df_cleanse.select('FilteredText').rdd.flatMap(lambda x: x).collect())
wordcloud = WordCloud(background_color="white", max_words=5000, contour_width=3, contour_color='steelblue',width=1200, height=600)
wordcloud.generate(long_string)
wordcloud.to_image().show()
print('Word Cloud Finished: ', datetime.now())

print('Define Vectorizer: ', datetime.now())
count_vectorizer = CountVectorizer(max_df=0.8, min_df=2, stop_words='english')
print('Document Term Matrix Begin: ', datetime.now())
doc_term_matrix = count_vectorizer.fit_transform(df_cleanse.select('FilteredText').toPandas()['FilteredText'])
print('Document Term Matrix End: ', datetime.now())

# Helper function
def print_topics(model, count_vectorizer, n_top_words):
    words = count_vectorizer.get_feature_names()
    for topic_idx, topic in enumerate(model.components_):
        print("\nTop 10 words for Topic: #%d:" % topic_idx)
        print(" ,".join([words[i] for i in topic.argsort()[:-n_top_words - 1:-1]]))

number_topics = 5
number_words = 10
# # Create and fit the LDA model
print('Defined LDA Begin: ', datetime.now())
lda = LDA(n_components=number_topics, n_jobs=-1)
print('Defined LDA End: ', datetime.now())

print('Fit LDA Begin: ', datetime.now())
lda.fit(doc_term_matrix)
print('Fit LDA End: ', datetime.now())

print('Collect Topic List Begin: ', datetime.now())
topic_values = lda.transform(doc_term_matrix)
print('Collect Topic List End: ', datetime.now())
print('Topic List to Pandas Begin: ', datetime.now())
pandaslist = pd.DataFrame(topic_values.argmax(axis=1))
print('Topic List to Pandas End: ', datetime.now())
print('Pandas to Spark Begin: ', datetime.now())
sparklist = spark.createDataFrame(pandaslist)
print('Pandas to Spark End: ', datetime.now())

print("Topics found via LDA:")
print_topics(lda, count_vectorizer, number_words)

print('Indexing Data Frames Begin: ', datetime.now())
sparklist = sparklist.withColumn("colid1", f.monotonically_increasing_id())
df_cleanse = df_cleanse.withColumn("colid2", f.monotonically_increasing_id())
df_cleanse=df_cleanse.select("Rating","ReviewerName","Categories","gPlusPlaceId","gPlusUserId","ReviewTime","BlobPolarity","SentimentAnalyzer","colid2")
print('Indexing Data Frames End: ', datetime.now())

# Write Cleansed file to Output
print('Writing to output File Begin: ', datetime.now())
sparklist.repartition(1).write.format("com.databricks.spark.csv").option("header","true").save("D:/670/GoogleReviewsData/Topic.csv")
df_cleanse.repartition(1).write.format("com.databricks.spark.csv").option("header","true").save("D:/670/GoogleReviewsData/Sentiment.csv")
print('Writing to output File End: ', datetime.now())

# Stop the Spark Context
sc.stop()