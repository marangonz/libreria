from pyspark.sql import SparkSession
import json
import os

if __name__ == "__main__":  # Corrección del bloque if __name__ == "__main__"
    spark = SparkSession\
        .builder\
        .appName("books")\
        .getOrCreate()

    print("read books.csv ... ")
    path_books = "books.csv"
    df_books = spark.read.csv(path_books, header=True, inferSchema=True)

    # Si las columnas tienen espacios extra o caracteres especiales, se pueden renombrar
    df_books = df_books.withColumnRenamed("Title", "title") \
                       .withColumnRenamed("Author", "author") \
                       .withColumnRenamed("Genre", "genre") \
                       .withColumnRenamed("Height", "height") \
                       .withColumnRenamed("Publisher", "publisher")

    # Crear vista temporal para consultas SQL
    df_books.createOrReplaceTempView("books")
    
    # Ver las primeras filas y la estructura
    query = 'DESCRIBE books'
    spark.sql(query).show(20)

    # Consulta: Filtrar libros del género "data_science" y ordenarlos por altura
    query = """SELECT title, author, genre, height FROM books WHERE genre="data_science" ORDER BY height"""
    df_books_data_science = spark.sql(query)
    df_books_data_science.show(20)

    # Consulta: Filtrar libros publicados por "Penguin" y mostrar los más altos
    query = 'SELECT title, author, publisher, height FROM books WHERE publisher="Penguin" ORDER BY height DESC'
    df_books_penguin = spark.sql(query)
    df_books_penguin.show(20)

    # Consulta: Contar libros por género
    query = 'SELECT genre, COUNT(*) as count FROM books GROUP BY genre'
    df_books_by_genre = spark.sql(query)
    df_books_by_genre.show()

    # Asegurarse de que la carpeta 'results' exista antes de escribir en ella
    if not os.path.exists('results'):
        os.makedirs('results')

    # Guardar resultados de la consulta de data_science en formato JSON con Spark
    df_books_data_science.write.mode("overwrite").json("results/books_data_science.json")

    # Convertir el DataFrame de 'data_science' a formato JSON y guardarlo con Python
    results = df_books_data_science.toJSON().collect()
    with open('results/data_science_books.json', 'w') as file:
        json.dump(results, file)

    # Detener la sesión de Spark
    spark.stop()

