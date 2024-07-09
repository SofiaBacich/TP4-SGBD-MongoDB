import pymongo
import psycopg2
import re

def main():
    determinarOrigen()


def determinarOrigen():
    countriesDict = getCountriesDict()
    citiesDict = getCitiesDict()

    collection = getCollectionMongoDB()

    cantidadNoDeterminados = 0

    #deletes(collection)

    #casosBase(countriesDict, collection)

    #casosBaseDetallados(countriesDict, collection)
    
    #casosEspeciales(countriesDict, collection)

    #ciudadesBase(citiesDict, collection)

    #ciudadesEspeciales(citiesDict, collection)

    #borrarNoDeterminados(collection)
  
    print("____________________________________________________________________________________________________________________")
    dataMongoDB = collection.find({"pais":""}).sort({"user.location":-1})
    for data in dataMongoDB:
        cantidadNoDeterminados += 1
        print("Location: ", data["user"]["location"], "Pais: ", data["pais"])
   
    print("Cantidad NO determinados: ", cantidadNoDeterminados)


def deletes(collection):
    #Borramos todos los registros que no posean user.location
    collection.delete_many({"user.location" : None})

    dataMongoDB = collection.find().sort({"user.location":1}) 

    for data in dataMongoDB:
        id = data["id"]
        location = re.sub(r"[,/.-]|\s+","", data["user"]["location"])
        
        tieneBasura = re.search(r"\W+", location)
        tieneNumeros = re.search(r"\d+", location)

        #Borramos todos los registros que tengan basura o numeros
        if tieneBasura or tieneNumeros:
            collection.delete_one( { "id":id } )
            print(location)
        #Borramos todos los registros cuya longitud sea mayor a 30 o menor a 2
        elif len(location) > 30 or len(location) < 2:
            collection.delete_one( { "id":id } )
            print(location)
        

def casosBase(countriesDict, collection):
    dataMongoDB = collection.find({"pais":None})

    for data in dataMongoDB:
        id = data["id"]
        pais = ""
        countryCode = ""

        ubicacion = re.split(r"[,/-]", data["user"]["location"])
      
        for info in ubicacion:
            if info in countriesDict:
                pais = info
                countryCode = countriesDict[info]
                print(pais, countryCode)
        
        collection.update_one( { "id":id } , { "$set": {"pais":pais, "countryCode":countryCode} } )


def casosBaseDetallados(countriesDict, collection):
    dataMongoDB = collection.find({"pais":""}).sort({"user.location":1})     #Trabajamos solo con los que todavia no estan definidos

    for data in dataMongoDB:
        id = data["id"]
        pais = ""
        countryCode = ""

        ubicacion = re.split(r"[,/-]", data["user"]["location"])
      
        for info in ubicacion:
            info = re.sub(r"^\s+|$\s+", "", info).capitalize()

            if info in countriesDict:
                pais = info
                countryCode = countriesDict[info]
                print(pais, countryCode)
        
        collection.update_one( { "id":id } , { "$set": {"pais":pais, "countryCode":countryCode} } )


def casosEspeciales(countriesDict, collection):
    dataMongoDB = collection.find({"pais":""}).sort({"user.location":1})      #Trabajamos solo con los que todavia no estan definidos
    
    for data in dataMongoDB:
        id = data["id"]
        pais = ""
        countryCode = ""

        ubicacion = re.split("[,/-]", data["user"]["location"])

        for info in ubicacion:
            info = re.sub(r"^\s+|$\s+", "", info).capitalize()

            if info == "Usa" or info == "The united states of america" or info == "United states of america" or info == "U.s.a." or info == "Us" or info == "U.s.":
                info = "United States"
            elif info == "México":
                info = "Mexico"
            elif info == "Uk" or info == "U.k." or info == "England":
                info = "United Kingdom"
            elif info == "Brasil":
                info = "Brazil"
            elif info == "Panamá":
                info = "Panama"
            elif info == "Perú":
                info = "Peru"
            elif info == "España":
                info = "Spain"

            if info in countriesDict:
                pais = info
                countryCode = countriesDict[info]
                print(pais, countryCode)
        
        collection.update_one( { "id":id } , { "$set": {"pais":pais, "countryCode":countryCode} } )


def ciudadesBase(citiesDict, collection):
    dataMongoDB = collection.find({"pais":""}).sort({"user.location":1})     #Trabajamos solo con los que todavia no estan definidos
    
    for data in dataMongoDB:
        id = data["id"]
        pais = ""
        countryCode = ""

        ubicacion = re.split("[,/-]", data["user"]["location"])

        for info in ubicacion:
            if info in citiesDict:
                pais = citiesDict[info][0]
                countryCode = citiesDict[info][1]
                print(pais, countryCode)
        
        collection.update_one( { "id":id } , { "$set": {"pais":pais, "countryCode":countryCode} } )


def ciudadesEspeciales(citiesDict, collection):
    dataMongoDB = collection.find({"pais":""}).sort({"user.location":1})     #Trabajamos solo con los que todavia no estan definidos
    
    for data in dataMongoDB:
        id = data["id"]
        pais = ""
        countryCode = ""

        ubicacion = re.split("[,/-]", data["user"]["location"])

        for info in ubicacion:
            info = re.sub(r"^\s+|$\s+", "", info).capitalize()

            if info == "Cdmx":
                info = "Ciudad de méxico"
            elif info == "Ciudad autónoma de buenos aire" or info == "Ca" or info == "C.a.b.a":
                info = "Caba"
            elif info == "Nyc":
                info = "Ny"

            if info in citiesDict:
                pais = citiesDict[info][0]
                countryCode = citiesDict[info][1]
                print(pais, countryCode)
        
        collection.update_one( { "id":id } , { "$set": {"pais":pais, "countryCode":countryCode} } )


def borrarNoDeterminados(collection):
    collection.delete_many({"pais":""})
    collection.delete_many({"pais":None}) 


def getCollectionMongoDB():
    client = connectMongoDB()
    # Seleccionar la base de datos test
    db = client["test"]

    # Seleccionar la colección allTweets
    collection = db["allTweets"]

    return collection


def connectMongoDB():
    # Establecer la conexión con MongoDB
    return pymongo.MongoClient("mongodb://localhost:27017/")

 
def getCountriesDict():
    connection = connectPostgres()
    cursor = connection.cursor()
    countriesDict = {}
    
    cursor.execute("SELECT name, code FROM country ORDER BY name ASC")
    countriesData = cursor.fetchall()

    for data in countriesData:
        countriesDict[data[0]] = data[1]

    connection.close() #cerrar conexion
    return countriesDict


def getCitiesDict():
    connection = connectPostgres()
    cursor = connection.cursor()
    citiesDict = {}
    
    cursor.execute("SELECT country.name, country.code, city.name FROM country INNER JOIN city ON country.code = city.countrycode GROUP BY country.name, country.code, city.name ORDER BY country.name ASC")
    countriesData = cursor.fetchall()
   
    for data in countriesData:
        citiesDict[data[2]] = ( data[0], data[1] )
        # Ciudad: ( Pais : countryCode) 

    citiesDict.update({"Arizona":("United States", "USA")})
    citiesDict.update({"Florida":("United States", "USA")})
    citiesDict.update({"Texas":("United States", "USA")})
    citiesDict.update({"Colorado":("United States", "USA")})
    citiesDict.update({"Ny":("United States", "USA")})
    citiesDict.update({"California":("United States", "USA")})
    citiesDict.update({"Ohio":("United States", "USA")})
    citiesDict.update({"Alabama":("United States", "USA")})
    citiesDict.update({"Córdoba":("Argentina", "ARG")})
    citiesDict.update({"Barracas":("Argentina", "ARG")})
    citiesDict.update({"Catamarca":("Argentina", "ARG")})
    citiesDict.update({"Caba":("Argentina", "ARG")})
    citiesDict.update({"Bogota":("Colombia", "COL")})

    connection.close() #cerrar conexion
    return citiesDict


def connectPostgres():                  # Conectar a la base de datos
    try:
        connection = psycopg2.connect(
            host = 'localhost',
            port = 5432,
            user = 'postgres',
            password = '123',
            database = 'world'
        )
        print("Conexion exitosa")
    except ConnectionError: 
        print("No se pudo conectar a la BD de Postgres. Chequee los valores en la funcion connect")
        connection = ""
    
    return connection



if __name__ == "__main__":
    main()