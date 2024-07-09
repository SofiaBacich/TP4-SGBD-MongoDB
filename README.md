# TP4-SGBD: Análisis de Tweets con MongoDB
<h4> Trabajo práctico 4 de Sistemas de Gestión de Base de Datos. </h4>

## Descripción
<p> Consiste en un trabajo sobre un conjunto de datos muy extenso dado en formato .json (crisis.20190410.json)</p>
<p> Este se importa a una base de datos en MongoDB a partir de la cual se trabaja. </p>

## Detalle de los archivos

#### practica4.pdf
<p> Es consigna. Se recomienda leer para entender el propósito de cada paso </p>

#### crisis.20190410.json
<p> El archivo "crisis.20190410.json" es la muestra de datos completa dada en el enunciado. A partir de este se realizan recortes y análisis dando como resultado dos archivos: "tweets.json" y "allTweets.json" cuyo uso se detalla a continuación. </p> 

#### comandos.txt
<p> Es la resolución de los ejercicios 3.1, 3.2, 3.3 y 3.4, los cuales consisten en querys a correr en MongoSh. Para estos se trabaja con una cantidad reducida de registros a partir de la base de datos "tweets.json" que consiste en los primeros 5000 registros de la base "crisis.20190410.json"</p>

#### 3.4.1.py
<p> Es la resolución del primer punto del ejercicio 3.4. En el cual se hace un gran trabajo sobre el conjunto de datos, analizando aquellos que poseen user.location y clasificando esa ubicación según la base de datos "world.sql". Se exporta la base de datos resultante en "allTweets.json" para poder importarla sin tener que esperar todo este proceso. A partir de esta se realizan los puntos siguientes </p>

#### 3.4.2.py y su carpeta
<p> Este punto requiere la utilización de los archivos existentes para crear un GeoDataFrame que servirá para poder crear un gráfico (Mapa Choropleth) </p>
<p> El gráfico creado puede verse en "Tweets Mundiales.png". Consiste en la cantidad de tweets por país." </p>

#### 3.4.3.py
<p> Es la resolución de dicho ejercicio, donde se realizan nubes de palabras para Argentina y Estados Unidos. </p>
<p> Los gráficos creado puede verse en "Nube de palabras para Argentina.png" y "Nube de palabras para United States.png".  </p>


