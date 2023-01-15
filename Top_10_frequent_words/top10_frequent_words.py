from pyspark import SparkContext

sc = SparkContext("local", "Word Count")

stop_words=['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 
                'you', 'your', 'yours', 'yourself','yourselves', 'he', 'him', 'his','himself',
                     'she', 'her', 'hers', 'herself', 'it', 'its', 'itself', 'they', 'them', 'their',
                'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', 'these',
                    'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has',
                        'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if',
                        'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about',
                         'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above',
                          'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 
                             'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how',
                                 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 
                                         'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 
                                        'very', 'can', 'will', 'just', 'don', 'should', 'now']


Rdd_file=sc.textFile("/root/file.txt")

Final_RDD = Rdd_file\
                    .flatMap(lambda line : line.split())\
                     .filter(lambda word: word.lower() not in stop_words)\
                      .map(lambda word : (word,1))\
                       .reduceByKey(lambda a,b:a+b)\
                        .map(lambda Tuple:(Tuple[1],Tuple[0]))\
                        .sortByKey(ascending=False)


for count,word in Final_RDD.take(10):
    print(f"word {word} ==> {count}")


