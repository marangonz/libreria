from pyspark.sql import SparkSession
import json

if __name__ == "_main_":
    spark = SparkSession\
        .builder\
        .appName("books")\
        .getOrCreate()

    print("read dataset.csv ... ")
    path_books="dataset.csv"
    df_books = spark.read.csv(path_books,header=True,inferSchema=True)
    df_books.createOrReplaceTempView("_books")
    query='DESCRIBE_books'
    spark.sql(query).show(20)

    query="""SELECT Title FROM_books WHERE Publisher == "Penguin" """
    df_books_title = spark.sql(query)
    df_books_title.show(20)
    results = df_books_title.toJSON().collect()
    df_books_title.write.mode("overwrite").json("results")
    with open('results/data.json', 'w') as file:
        json.dump(results, file)
        spark.stop()
