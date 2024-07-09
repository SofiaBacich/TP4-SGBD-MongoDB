import pymongo
import pandas as pd
import geopandas as gp
import matplotlib.pyplot as plt

#Es necesario correr primero el ejer3.4.1py para tener la coleccion "allTweets" filtrada y acomodada

def main():
    df = getDataFrame() 
    print(df)
    
    #tweets
    getGeoDataGraphic(df,
                    "Tweets Mundiales",
                    ['_id', 'cantidadTweets', 'countryCode'], 
                    '_id', 
                    'cantidadTweets')
    
    plt.show()
    

def getDataFrame():
    collection = getCollectionMongoDB()
    list = []

    pipeline = [
	    {
            "$group": {
                        "_id": "$pais", 
                        "cantidadTweets": {"$sum": 1}, 
                        "countryCode":{"$first":"$countryCode"}  #De esta forma obtenemos el countryCode del primer documento de cada grupo
                    } 
        },
   	    {"$sort": {"count": -1} }
    ]
    
    documents = collection.aggregate(pipeline)

    for diccionario in documents:
        list.append(diccionario)

    return pd.DataFrame.from_dict(list) #Conseguimos el dataframe con el cual poder trabajar


def getGeoDataGraphic(df, title, columns, mergeCol, geomCol): #Creamos el data frame con los subdominios parseados
    world = gp.GeoDataFrame.from_file('ne_10m_admin_0_countries.shp')

    world = world.rename(columns = {'SOVEREIGNT':mergeCol})  #Mismo nombre para la columna name o pais

    world.loc[world[mergeCol] == "United States of America", mergeCol] = "United States" #Caso especial, USA con diferentes nombres

    df = df.astype({geomCol:int})       #Nos aseguramos que los datos sean int
 
    df_merged = world.merge(df[columns], on = mergeCol, how = 'left') #Mergeo de data frames

    ax = df_merged.plot(column=geomCol, cmap = "turbo", legend=True, missing_kwds={'color': 'lightgrey', 'hatch':'//', 'label':'Missing Values'}) #Grafico

    ax.set_title(title)


def getCollectionMongoDB():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["test"]
    collection = db["allTweets"]
    return collection


if __name__ == "__main__":
    main()