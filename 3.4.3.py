import pymongo
import re
from collections import Counter
from wordcloud import WordCloud
import matplotlib.pyplot as plt

def main():
    collection = getCollectionMongoDB()
    generarNube(collection, "Argentina")
    generarNube(collection, "United States")


def getCollectionMongoDB():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["test"]
    collection = db["allTweets"]
    return collection


def generarNube(collection, pais):
    tweets = list(obtenerTweetsPorPais(collection, pais))
    texto = procesarTexto(tweets)
    palabras_mas_usadas = contarPalabras(texto).most_common(20)
    
    diccionario = {clave: valor for clave, valor in palabras_mas_usadas}
    wordcloud = WordCloud(width=800, height=400, background_color='white')
    wordcloud.generate_from_frequencies(diccionario)
    
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.title(f'Nube de Palabras para {pais}')
    plt.axis('off')
    plt.show()


def obtenerTweetsPorPais(collection, pais):
    return collection.find({"pais": pais})


def procesarTexto(tweets):
    # Lista de diccionario de tweets
    text = " ".join(tweet["text"] for tweet in tweets if "text" in tweet)
    text = text.lower()
    
    # Filtros
    text = re.sub(r"http\S+|www\S+|https\S+", '', text) # Limpia los links
    text = re.sub(r'\@\w+|\#','', text) # Elimina menciones de usuarios y hashtags
    text = re.sub(r'[^A-Za-z0-9\s]+', '', text) # Elimina caracteres sueltos
    text = re.sub(r'\brt\b', '', text)  # Eliminar la palabra "RT"
    
    return text


def contarPalabras(texto):
    # Dividir el texto en palabras
    palabras = texto.split()
    
    # Eliminar palabras que tienen menos de 2 letras
    palabras = [palabra for palabra in palabras if len(palabra) > 2]
    
    # Contar las palabras usando Counter
    contador = Counter(palabras)
    
    return contador


if __name__ == "__main__":
    main()
