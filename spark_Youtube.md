
### Youtube Comment Analysis
In this notebook, we have a dataset of user comments for youtube videos related to animals or pets. We will attempt to identify cat or dog owners based on these comments, find out the topics important to them, and then identify video creators with the most viewers that are cat or dog owners.

#### 0. Data Exploration and Cleaning


```python
# read data
df_raw = spark.read.load("/FileStore/tables/animals_comments.csv", format='csv', header = True, inferSchema = True)
df_raw.show(10)
```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout">+--------------------+------+--------------------+
        creator_name|userid|             comment|
+--------------------+------+--------------------+
        Doug The Pug|  87.0|I shared this to ...|
        Doug The Pug|  87.0|  Super cute  😀🐕🐶|
         bulletproof| 530.0|stop saying get e...|
       Meu Zoológico| 670.0|Tenho uma jiboia ...|
              ojatro|1031.0|I wanna see what ...|
     Tingle Triggers|1212.0|Well shit now Im ...|
Hope For Paws - O...|1806.0|when I saw the en...|
Hope For Paws - O...|2036.0|Holy crap. That i...|
          Life Story|2637.0|武器はクエストで貰えるんじゃないん...|
       Brian Barczyk|2698.0|Call the teddy Larry|
+--------------------+------+--------------------+
only showing top 10 rows

</div>



```python
df, df_rest= df_raw.randomSplit([0.05, 0.95])
```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout"></div>



```python
# find user with preference of dog and cat
# note: please propose your own approach and rule to label data 
#cond = (df_clean["comment"].like("%my dog%") | df_clean["comment"].like("%I have a dog%")\
#        | df_clean["comment"].like("%my cat%") | df_clean["comment"].like("%I have a cat%"))

cond = (df["comment"].like("%my dog%") | df["comment"].like("%I have a dog%") | df["comment"].like("%my dogs%") | df["comment"].like("%I have dog%")
        | df["comment"].like("%my cat%") | df["comment"].like("%my cats%") | df["comment"].like("%I have a cat%") | df["comment"].like("%I have cat%") 
        | df["comment"].like("%my puppy%") | df["comment"].like("%my puppies%") | df["comment"].like("%my kitty%") | df["comment"].like("%my kitties%") 
        | df["comment"].like("%I have a kitty%") | df["comment"].like("%I have kitties%") | df["comment"].like("%I have a puppy%") | df["comment"].like("%I have puppies%"))

df_clean = df.withColumn('dog_cat',  cond)

# find user do not have 
df_clean = df_clean.withColumn('no_pet', ~df_clean["comment"].like("%my%") & ~df_clean["comment"].like("%have%")) 
df_clean = df_clean.withColumn('label', col("dog_cat").cast(IntegerType()).cast('double'))

df_clean.show()
```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout">+------------+--------+--------------------+-------+------+-----+
creator_name|  userid|             comment|dog_cat|no_pet|label|
+------------+--------+--------------------+-------+------+-----+
        null| 13596.0|The last sentence...|  false|  true|  0.0|
        null| 20666.0|You look like old...|  false|  true|  0.0|
        null| 21461.0|bhai fsd ki jhang...|  false|  true|  0.0|
        null| 50782.0|You watch JTV and...|  false|  true|  0.0|
        null| 55762.0|Its so funny how ...|  false|  true|  0.0|
        null| 63684.0|Do you know what ...|  false|  true|  0.0|
        null| 63747.0|you sure know how...|  false|  true|  0.0|
        null| 79376.0|I loove this new ...|  false|  true|  0.0|
        null| 87712.0|            fuck you|  false|  true|  0.0|
        null|108962.0|I cant wait for m...|  false|  true|  0.0|
        null|114122.0|i like the new na...|  false|  true|  0.0|
        null|114776.0|I really enjoy yo...|  false|  true|  0.0|
        null|114776.0|OMG THIS REACTION...|  false|  true|  0.0|
        null|122137.0|                 Wow|  false|  true|  0.0|
        null|130826.0|Nice video always...|  false|  true|  0.0|
        null|130826.0|    Nice videp💐🌹😘|  false|  true|  0.0|
        null|141180.0|This was when I s...|  false|  true|  0.0|
        null|141180.0|           delena ❤❤|  false|  true|  0.0|
        null|153390.0|          dangerous!|  false|  true|  0.0|
        null|157301.0|Eu falo português...|  false|  true|  0.0|
+------------+--------+--------------------+-------+------+-----+
only showing top 20 rows

</div>



```python
for colume in df_clean.columns:
  df_clean=df_clean.filter(df_clean[colume].isNotNull())
```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout"></div>



```python
from pyspark.ml.feature import StopWordsRemover

# Define a list of stop words or use default list
remover = StopWordsRemover()
stopwords = remover.getStopWords() 

# Display some of the stop words
stopwords[:10]
```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout"><span class="ansired">Out[</span><span class="ansired">196</span><span class="ansired">]: </span>[&apos;i&apos;, &apos;me&apos;, &apos;my&apos;, &apos;myself&apos;, &apos;we&apos;, &apos;our&apos;, &apos;ours&apos;, &apos;ourselves&apos;, &apos;you&apos;, &apos;your&apos;]
</div>



```python
# data preprocessing 
from pyspark.ml.feature import RegexTokenizer

regexTokenizer = RegexTokenizer(inputCol="comment", outputCol="text", pattern="\\W")
df_clean= regexTokenizer.transform(df_clean)
 
```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout"></div>



```python

remover.setInputCol("text")
remover.setOutputCol("vector_no_stopw")
df_clean = remover.transform(df_clean)
df_clean.show(10)

```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout">+--------------------+---------+--------------------+-------+------+-----+--------------------+--------------------+
        creator_name|   userid|             comment|dog_cat|no_pet|label|                text|     vector_no_stopw|
+--------------------+---------+--------------------+-------+------+-----+--------------------+--------------------+
#CameraLord™ • Ko...| 412776.0|Keep going yall g...|  false|  true|  0.0|[keep, going, yal...|[keep, going, yal...|
#CameraLord™ • Ko...| 890114.0|Who still watchs ...|  false|  true|  0.0|[who, still, watc...|[still, watchs, v...|
#CameraLord™ • Ko...| 913684.0|          BIG GELATO|  false|  true|  0.0|       [big, gelato]|       [big, gelato]|
#CameraLord™ • Ko...|1000411.0|All Gas No Brake!...|  false|  true|  0.0|[all, gas, no, br...|        [gas, brake]|
#CameraLord™ • Ko...|1000411.0|                Dope|  false|  true|  0.0|              [dope]|              [dope]|
#CameraLord™ • Ko...|1049623.0|damn this beat go...|  false|  true|  0.0|[damn, this, beat...|[damn, beat, go, ...|
#CameraLord™ • Ko...|1060242.0|All i heard was m...|  false|  true|  0.0|[all, i, heard, w...|[heard, mumbles, ...|
#CameraLord™ • Ko...|1327825.0|fresh as fuck lik...|  false|  true|  0.0|[fresh, as, fuck,...|[fresh, fuck, lik...|
#CameraLord™ • Ko...|1449078.0|Favorite song on ...|  false|  true|  0.0|[favorite, song, ...|[favorite, song, ...|
#CameraLord™ • Ko...|1507593.0|               die 6|  false|  true|  0.0|            [die, 6]|            [die, 6]|
+--------------------+---------+--------------------+-------+------+-----+--------------------+--------------------+
only showing top 10 rows

</div>


#### 1. Build the classifier 
In order to train a model against the comments, you can use RegexTokenizer to split each comment into a list of words and then use Word2Vec or other model to convert the list to a word vector. What Word2Vec does is to map each word to a unique fixed-size vector and then transform each document into a vector using the average of all words in the document.


```python
%sh /home/ubuntu/databricks/python/bin/pip install nltk
```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout">Requirement already satisfied: nltk in /databricks/python3/lib/python3.5/site-packages (3.4)
Requirement already satisfied: singledispatch in /databricks/python3/lib/python3.5/site-packages (from nltk) (3.4.0.3)
Requirement already satisfied: six in /usr/lib/python3/dist-packages (from nltk) (1.10.0)
You are using pip version 10.0.1, however version 18.1 is available.
You should consider upgrading via the &apos;pip install --upgrade pip&apos; command.
</div>



```python
from nltk.stem.porter import *

# Instantiate stemmer object
stemmer = PorterStemmer()

# Create stemmer python function
def stem(in_vec):
    out_vec = []
    for t in in_vec:
        t_stem = stemmer.stem(t)
        if len(t_stem) > 2:
            out_vec.append(t_stem)       
    return out_vec

# Create user defined function for stemming with return type Array<String>
from pyspark.sql.types import *
stemmer_udf = udf(lambda x: stem(x), ArrayType(StringType()))

# Create new column with vectors containing the stemmed tokens 
df_clean = df_clean.withColumn("vector_stemmed", stemmer_udf("vector_no_stopw"))

df_clean.show()
```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout">+--------------------+---------+--------------------+-------+------+-----+--------------------+--------------------+--------------------+
        creator_name|   userid|             comment|dog_cat|no_pet|label|                text|     vector_no_stopw|      vector_stemmed|
+--------------------+---------+--------------------+-------+------+-----+--------------------+--------------------+--------------------+
#CameraLord™ • Ko...| 412776.0|Keep going yall g...|  false|  true|  0.0|[keep, going, yal...|[keep, going, yal...|[keep, yall, got,...|
#CameraLord™ • Ko...| 890114.0|Who still watchs ...|  false|  true|  0.0|[who, still, watc...|[still, watchs, v...|[still, watch, vi...|
#CameraLord™ • Ko...| 913684.0|          BIG GELATO|  false|  true|  0.0|       [big, gelato]|       [big, gelato]|       [big, gelato]|
#CameraLord™ • Ko...|1000411.0|All Gas No Brake!...|  false|  true|  0.0|[all, gas, no, br...|        [gas, brake]|             [brake]|
#CameraLord™ • Ko...|1000411.0|                Dope|  false|  true|  0.0|              [dope]|              [dope]|              [dope]|
#CameraLord™ • Ko...|1049623.0|damn this beat go...|  false|  true|  0.0|[damn, this, beat...|[damn, beat, go, ...|[damn, beat, craz...|
#CameraLord™ • Ko...|1060242.0|All i heard was m...|  false|  true|  0.0|[all, i, heard, w...|[heard, mumbles, ...| [heard, mumbl, lol]|
#CameraLord™ • Ko...|1327825.0|fresh as fuck lik...|  false|  true|  0.0|[fresh, as, fuck,...|[fresh, fuck, lik...|[fresh, fuck, lik...|
#CameraLord™ • Ko...|1449078.0|Favorite song on ...|  false|  true|  0.0|[favorite, song, ...|[favorite, song, ...|[favorit, song, a...|
#CameraLord™ • Ko...|1507593.0|               die 6|  false|  true|  0.0|            [die, 6]|            [die, 6]|               [die]|
#CameraLord™ • Ko...|1522521.0|I think I.L Will ...|  false|  true|  0.0|[i, think, i, l, ...|[think, l, stop, ...|[think, stop, fuc...|
#CameraLord™ • Ko...|1575340.0|you GD YOU BD YOU...|  false|  true|  0.0|[you, gd, you, bd...|[gd, bd, stone, b...| [stone, best, part]|
#CameraLord™ • Ko...|1704212.0|sounding like 201...|  false|  true|  0.0|[sounding, like, ...|[sounding, like, ...|[sound, like, 201...|
#CameraLord™ • Ko...|1813654.0|Hold up just pads...|  false|  true|  0.0|[hold, up, just, ...|[hold, pads, nusk...|[hold, pad, nuski...|
#CameraLord™ • Ko...|1823504.0|Dope song. Walked...|  false|  true|  0.0|[dope, song, walk...|[dope, song, walk...|[dope, song, walk...|
#CameraLord™ • Ko...|1950851.0|Billy: Excuse me ...|  false|  true|  0.0|[billy, excuse, m...|[billy, excuse, o...|[billi, excus, op...|
#CameraLord™ • Ko...|2284744.0|I also see you go...|  false|  true|  0.0|[i, also, see, yo...|[also, see, got, ...|[also, see, got, ...|
#CameraLord™ • Ko...|2307162.0|I allways like th...|  false|  true|  0.0|[i, allways, like...|[allways, like, i...|[allway, like, in...|
  100 Bigfoot Nights| 671840.0|I dont have have ...|  false| false|  0.0|[i, dont, have, h...|[dont, time, nons...|[dont, time, nons...|
        100% Madison|1204930.0|Nice vid bro! you...|  false| false|  0.0|[nice, vid, bro, ...|[nice, vid, bro, ...|[nice, vid, bro, ...|
+--------------------+---------+--------------------+-------+------+-----+--------------------+--------------------+--------------------+
only showing top 20 rows

</div>



```python
from pyspark.ml.feature import HashingTF, IDF
from pyspark.sql.functions import col,size,count,when,isnan
from pyspark.sql import *
from functools import reduce

df_clean.na.drop()
hashingTF = HashingTF(inputCol="vector_stemmed", outputCol="tf", numFeatures=200)
featurizedData = hashingTF.transform(df_clean)
featurizedData.na.drop()

featurizedData.withColumn('userid', col('userid').cast('float').cast(IntegerType()))

featurizedData.show()
```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout">+--------------------+---------+--------------------+-------+------+-----+--------------------+--------------------+--------------------+--------------------+
        creator_name|   userid|             comment|dog_cat|no_pet|label|                text|     vector_no_stopw|      vector_stemmed|                  tf|
+--------------------+---------+--------------------+-------+------+-----+--------------------+--------------------+--------------------+--------------------+
#CameraLord™ • Ko...| 412776.0|Keep going yall g...|  false|  true|  0.0|[keep, going, yal...|[keep, going, yal...|[keep, yall, got,...|(200,[7,9,134,136...|
#CameraLord™ • Ko...| 890114.0|Who still watchs ...|  false|  true|  0.0|[who, still, watc...|[still, watchs, v...|[still, watch, vi...|(200,[0,2,29,75,1...|
#CameraLord™ • Ko...| 913684.0|          BIG GELATO|  false|  true|  0.0|       [big, gelato]|       [big, gelato]|       [big, gelato]|(200,[100,198],[1...|
#CameraLord™ • Ko...|1000411.0|All Gas No Brake!...|  false|  true|  0.0|[all, gas, no, br...|        [gas, brake]|             [brake]|    (200,[18],[1.0])|
#CameraLord™ • Ko...|1000411.0|                Dope|  false|  true|  0.0|              [dope]|              [dope]|              [dope]|    (200,[85],[1.0])|
#CameraLord™ • Ko...|1049623.0|damn this beat go...|  false|  true|  0.0|[damn, this, beat...|[damn, beat, go, ...|[damn, beat, craz...|(200,[5,63,130,16...|
#CameraLord™ • Ko...|1060242.0|All i heard was m...|  false|  true|  0.0|[all, i, heard, w...|[heard, mumbles, ...| [heard, mumbl, lol]|(200,[30,107,157]...|
#CameraLord™ • Ko...|1327825.0|fresh as fuck lik...|  false|  true|  0.0|[fresh, as, fuck,...|[fresh, fuck, lik...|[fresh, fuck, lik...|(200,[78,130,192,...|
#CameraLord™ • Ko...|1449078.0|Favorite song on ...|  false|  true|  0.0|[favorite, song, ...|[favorite, song, ...|[favorit, song, a...|(200,[28,71,150],...|
#CameraLord™ • Ko...|1507593.0|               die 6|  false|  true|  0.0|            [die, 6]|            [die, 6]|               [die]|   (200,[129],[1.0])|
#CameraLord™ • Ko...|1522521.0|I think I.L Will ...|  false|  true|  0.0|[i, think, i, l, ...|[think, l, stop, ...|[think, stop, fuc...|(200,[3,15,18,45,...|
#CameraLord™ • Ko...|1575340.0|you GD YOU BD YOU...|  false|  true|  0.0|[you, gd, you, bd...|[gd, bd, stone, b...| [stone, best, part]|(200,[35,140,163]...|
#CameraLord™ • Ko...|1704212.0|sounding like 201...|  false|  true|  0.0|[sounding, like, ...|[sounding, like, ...|[sound, like, 201...|(200,[10,24,25,27...|
#CameraLord™ • Ko...|1813654.0|Hold up just pads...|  false|  true|  0.0|[hold, up, just, ...|[hold, pads, nusk...|[hold, pad, nuski...|(200,[51,78,125,1...|
#CameraLord™ • Ko...|1823504.0|Dope song. Walked...|  false|  true|  0.0|[dope, song, walk...|[dope, song, walk...|[dope, song, walk...|(200,[5,44,45,53,...|
#CameraLord™ • Ko...|1950851.0|Billy: Excuse me ...|  false|  true|  0.0|[billy, excuse, m...|[billy, excuse, o...|[billi, excus, op...|(200,[7,12,30,50,...|
#CameraLord™ • Ko...|2284744.0|I also see you go...|  false|  true|  0.0|[i, also, see, yo...|[also, see, got, ...|[also, see, got, ...|(200,[7,47,59,115...|
#CameraLord™ • Ko...|2307162.0|I allways like th...|  false|  true|  0.0|[i, allways, like...|[allways, like, i...|[allway, like, in...|(200,[39,82,130],...|
  100 Bigfoot Nights| 671840.0|I dont have have ...|  false| false|  0.0|[i, dont, have, h...|[dont, time, nons...|[dont, time, nons...|(200,[105,135,157...|
        100% Madison|1204930.0|Nice vid bro! you...|  false| false|  0.0|[nice, vid, bro, ...|[nice, vid, bro, ...|[nice, vid, bro, ...|(200,[32,54,67,68...|
+--------------------+---------+--------------------+-------+------+-----+--------------------+--------------------+--------------------+--------------------+
only showing top 20 rows

</div>



```python
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout"></div>



```python
idf = IDF(inputCol="tf", outputCol="features")
idfModel = idf.fit(featurizedData)

```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout"></div>



```python
rescaledData = idfModel.transform(featurizedData)
rescaledData.select("label", "features").show()
```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout">+-----+--------------------+
label|            features|
+-----+--------------------+
  0.0|(200,[7,9,134,136...|
  0.0|(200,[0,2,29,75,1...|
  0.0|(200,[100,198],[4...|
  0.0|(200,[18],[3.3287...|
  0.0|(200,[85],[4.1888...|
  0.0|(200,[5,63,130,16...|
  0.0|(200,[30,107,157]...|
  0.0|(200,[78,130,192,...|
  0.0|(200,[28,71,150],...|
  0.0|(200,[129],[3.503...|
  0.0|(200,[3,15,18,45,...|
  0.0|(200,[35,140,163]...|
  0.0|(200,[10,24,25,27...|
  0.0|(200,[51,78,125,1...|
  0.0|(200,[5,44,45,53,...|
  0.0|(200,[7,12,30,50,...|
  0.0|(200,[7,47,59,115...|
  0.0|(200,[39,82,130],...|
  0.0|(200,[105,135,157...|
  0.0|(200,[32,54,67,68...|
+-----+--------------------+
only showing top 20 rows

</div>



```python
pet = rescaledData.filter("label=1.0")
pet_train, pet_test = pet.randomSplit([0.8, 0.2])
nopet = rescaledData.filter("label=0.0")
sampleRatio = float(pet.count()) / float(nopet.count())
sample_nopet = nopet.sample(False, sampleRatio)
df_sample = pet.unionAll(sample_nopet)
sample_nopet_train, sample_nopet_test = sample_nopet.randomSplit([0.8, 0.2])

df_train = pet_train.unionAll(sample_nopet_train)
df_test = pet_test.unionAll(sample_nopet_test)
print ('training size',df_train.count())
print ('testing size',df_test.count())
```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout">training size 3128
testing size 746
</div>



```python
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder, TrainValidationSplit

lr = LogisticRegression(maxIter=10,featuresCol='features', labelCol='label')

paramGrid = ParamGridBuilder() \
    .addGrid(lr.regParam, [0.01,0.1]) \
    .build()

evaluator=BinaryClassificationEvaluator()
crossval = CrossValidator(estimator = lr,
                          estimatorParamMaps=paramGrid,
                          evaluator=evaluator,
                          numFolds=5)


cvModel = crossval.fit(df_train)
best_model = cvModel.bestModel
trainingSummary = best_model.summary
```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout"></div>



```python
path = "/FileStore/tables/"

best_model.save(path + 'best_model')
```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout"></div>



```python
prediction_train = best_model.transform(df_train)
prediction_test = best_model.transform(df_test)
accuracy_train = prediction_train.filter(prediction_train.label == prediction_train.prediction).count()/float(df_train.count())
accuracy_test = prediction_test.filter(prediction_test.label == prediction_test.prediction).count()/float(df_test.count())

print('Training set areaUnderROC: ' + str(evaluator.evaluate(prediction_train)))
print('Testing set areaUnderROC ' + str(evaluator.evaluate(prediction_test)))
print('Training set accuracy: ' + str(accuracy_train))
print('Testing set accuracy ' + str(accuracy_test))
```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout">Training set areaUnderROC: 0.973024096720583
Testing set areaUnderROC 0.9551711002019709
Training set accuracy: 0.9386189258312021
Testing set accuracy 0.9155495978552279
</div>


#### 2. Classify All The Users
We can now apply the cat/dog classifiers to all the other users in the dataset.


```python
prediction = best_model.transform(rescaledData)

total_pet_owner = prediction.filter("prediction = 1.0").count()
total_population = df.select("userid").distinct().count()
pet_owner_ratio = float(total_pet_owner)/float(total_population)
print('total_pet_owner :',total_pet_owner)
print('total_population :',total_population)
print('pet_owner_ratio :',pet_owner_ratio)
```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout">total_pet_owner : 31524
total_population : 240181
pet_owner_ratio : 0.1312510148596267
</div>


#### 3. Get insigts of Users


```python
from pyspark.mllib.linalg import Vector, Vectors
from pyspark.ml.clustering import LDA
from pyspark.ml.feature import CountVectorizer, CountVectorizerModel

pet_owner = prediction.filter("prediction = 1.0").select('userid','vector_stemmed')

cv = CountVectorizer(inputCol="vector_stemmed", outputCol="features",
                     minTF=2, # minium number of times a word must appear in a document
                     minDF=4) # minimun number of documents a word must appear in

countVectorModel = cv.fit(pet_owner)

countVectors = (countVectorModel
                .transform(pet_owner)
                .select("userid", "features").cache())

print(len(countVectorModel.vocabulary))  # how many documents, vocab size

numTopics = 10 # number of topics

lda = LDA(k = numTopics,
          maxIter = 50 # number of iterations
          )

ldaModel = lda.fit(countVectors)


# Print topics and top-weighted terms
topics = ldaModel.describeTopics(maxTermsPerTopic=20)
vocabArray = countVectorModel.vocabulary

ListOfIndexToWords = udf(lambda wl: list([vocabArray[w] for w in wl]))
FormatNumbers = udf(lambda nl: ["{:1.4f}".format(x) for x in nl])

topics.select(ListOfIndexToWords(topics.termIndices).alias('words')).show(truncate=False, n=numTopics)
```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout">7859
+----------------------------------------------------------------------------------------------------------------------------------------+
words                                                                                                                                   |
+----------------------------------------------------------------------------------------------------------------------------------------+
[name, vet, white, eye, boo, half, lion, zoo, sugar, ferret, black, ppl, squirrel, bless, wash, ranch, var, kovu, fluffi, disney]       |
[dog, puppi, breed, train, kitti, bull, pit, old, poor, collar, fight, see, attack, ear, servic, shelbi, plu, sonic, wolf, guard]       |
[guy, eat, food, game, snake, leg, call, python, door, sound, egg, coyot, side, ass, meat, ball, box, pig, max, she]                    |
[love, get, hors, one, like, video, anim, peopl, dont, know, want, time, see, make, realli, say, look, watch, help, day]                |
[thank, http, got, good, vlog, plz, pitbul, com, youtu, book, american, spider, www, red, youtub, bear, saw, watch, share, ant]         |
[tree, littl, bee, bir, durian, that, pull, honey, ama, preciou, offic, worth, venom, diy, content, alien, ship, todd, fall, mango]     |
[cat, kitten, bite, year, adopt, thing, hunt, big, cage, two, rescu, deer, cute, feed, rat, toy, bottl, fox, hedgehog, video]           |
[que, mucho, para, con, por, una, pero, todo, awesom, como, loki, kss, pelicula, ron, marvel, era, guppi, vengador, howl, eso]          |
[pleas, bird, fuck, great, human, hand, treat, save, frog, bunni, die, vid, bit, amaz, miss, real, film, feather, like, size]           |
[like, look, play, stung, robin, tail, anh, danc, chees, mine, birthday, super, wasp, giant, sylvest, taylor, crab, car, send, rottweil]|
+----------------------------------------------------------------------------------------------------------------------------------------+

</div>


#### 4. Identify Creators With Cat And Dog Owners In The Audience


```python
from pyspark.sql.functions import countDistinct
tmp = prediction.filter("prediction = 1.0")
tmp.groupBy('creator_name').agg(countDistinct('userid')).sort('count(DISTINCT userid)',ascending= False).show()
```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout">+--------------------+----------------------+
        creator_name|count(DISTINCT userid)|
+--------------------+----------------------+
            The Dodo|                  2460|
    Brave Wilderness|                  2283|
        Robin Seplut|                  1700|
       Brian Barczyk|                  1162|
Hope For Paws - O...|                  1144|
  Taylor Nicole Dean|                  1092|
           Vet Ranch|                   975|
    Cole &amp; Marmalade|                   840|
     Gohan The Husky|                   771|
     Viktor Larkhill|                   745|
Gone to the Snow ...|                   722|
   Talking Kitty Cat|                   543|
        Paws Channel|                   491|
          stacyvlogs|                   481|
  Think Like A Horse|                   449|
Zak Georges Dog T...|                   391|
       RaleighLink14|                   364|
         Info Marvel|                   360|
            ViralHog|                   294|
  The Pet Collective|                   284|
+--------------------+----------------------+
only showing top 20 rows

</div>


#### 5. Analysis and Future work


```python

According to the work (using only 5% of data because the databricks crashed often with the whole data), around 13% of the total user who commented on Youtube in this dataset are dog or cat owners. The potential topics they are interested in are include vet, white, eye, boo, half, lion, zoo, sugar, ferre, etc. Videos related to these topics could be promoted to these cat or dog owners. Also, 'The Dodo', 'brave wilderness' and 'Robin Seplut' are the top three creators with largest distinct cat or dog owner audience population (based only on the 5% data). Ads targeting cat or dog owners will potentially have the biggest payback cooperating with these creators.

For future work, this work could be improved in the following aspects: 
  1. Use all of the data.
  2. Specify the dog and cat owners based on more information.

```
