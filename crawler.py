#!/usr/local/bin/python3


#importing libraries

#import os
import csv
from flask import Flask
from flask import request
from flask import jsonify
import json
 

import datetime
import json
import urllib.request

import pandas as pd 
from pandas.io.json import json_normalize #package for flattening json in pandas df

import time

from cassandra.cluster import Cluster
import warnings
from cassandra.auth import PlainTextAuthProvider
from cassandra.cqlengine import connection

# Créé une url
def url_builder(city_id,city_name,country):
    user_api = 'Your API User'  # Obtain yours form: http://openweathermap.org/
    unit = 'metric'  # For Fahrenheit use imperial, for Celsius use metric, and the default is Kelvin.
    if(city_name!=""):
        api = 'http://api.openweathermap.org/data/2.5/weather?q=' # "http://api.openweathermap.org/data/2.5/weather?q=Tunis,fr
        full_api_url = api + str(city_name) +','+ str(country)+ '&mode=json&units=' + unit + '&APPID=' + user_api
    else:
        api = 'http://api.openweathermap.org/data/2.5/weather?id='# Search for your city ID here: http://bulk.openweathermap.org/sample/city.list.json.gz
        full_api_url = api + str(city_id) + '&mode=json&units=' + unit + '&APPID=' + user_api
    return full_api_url

# Fait un appel à une API via une url et renvoie les données recues
def data_fetch(full_api_url):
    url = urllib.request.urlopen(full_api_url)
    output = url.read().decode('utf-8')
    raw_api_dict = json.loads(output)#convertir une chaine comme dictionnaire
    url.close()#fermer la connexion au serveur api
    return raw_api_dict

# Convertit une chaîne caractères en Timestamp
def time_converter(time):
    converted_time = datetime.datetime.fromtimestamp(int(time)).strftime('%I:%M %p')
    return converted_time

# Réorganise les données dans une structure précise
def data_organizer(raw_api_dict):
    data = dict(
        city=raw_api_dict.get('name'),
        country=raw_api_dict.get('sys').get('country'),
        temp=raw_api_dict.get('main').get('temp'),
        temp_max=raw_api_dict.get('main').get('temp_max'),
        temp_min=raw_api_dict.get('main').get('temp_min'),
        humidity=raw_api_dict.get('main').get('humidity'),
        pressure=raw_api_dict.get('main').get('pressure'),
        sky=raw_api_dict['weather'][0]['main'],
        sunrise=time_converter(raw_api_dict.get('sys').get('sunrise')),
        sunset=time_converter(raw_api_dict.get('sys').get('sunset')),
        wind=raw_api_dict.get('wind').get('speed'),
        wind_deg=raw_api_dict.get('deg'),
        dt=time_converter(raw_api_dict.get('dt')),
        cloudiness=raw_api_dict.get('clouds').get('all')
    )
    return data

 
# Lit le fichier contenant les villes et leurs identifiants OpenWeather et 
# stocke dans un fichier les données des villes françaises
def getCities(country,limit):
    id_cities=[] 
    with open("city.list.json","r") as file:
        data = json.load(file)
        cities = list(filter(lambda x:x["country"]== country ,data))
         
        for city in cities :
            id_cities.append(city.get('id'))
            if  len(id_cities)==limit :break
    return id_cities
   
#############################################################################

################################################"
def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)
#on force ici a repecter le datframe de pandas lors de la recuperation des données
##definir une fonction qui permet d'etablir une connexion et initialiser le schema du keypsace
def connection():
    
            #auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
            CASSANDRA_HOST= ['Your IP Addresse'] 
            CASSANDRA_PORT=9042
          
            try:
                cluster = Cluster(contact_points=CASSANDRA_HOST, 
                                port=CASSANDRA_PORT#, auth_provider = auth_provider
                                )
                session = cluster.connect()
                session.row_factory = pandas_factory
            
            except ValueError:
                print("Oops!  échec de connexion cluster.  Try again...")
            
            session.execute("CREATE KEYSPACE IF NOT EXISTS Weather "\
                "WITH REPLICATION={'class':'SimpleStrategy','replication_factor':1};")

            session.execute("CREATE TABLE IF NOT EXISTS weather.cities_table (city TEXT,country TEXT,temp DOUBLE,temp_max DOUBLE ,"\
                "temp_min DOUBLE,humidity INT,pressure INT,sky TEXT,sunrise TEXT,sunset TEXT,wind DOUBLE,"\
                "dt TEXT,cloudiness INT,PRIMARY KEY (city));")
            #rows=session.execute('Select * from weather_fr.temperatures;')
            #df_results = rows._current_rows
            #df_results.head()
            
            
            session.default_fetch_size = 10000000 #needed for large queries, otherwise driver will do pagination. Default is 50000.
            return session
# Create a flask
app = Flask(__name__)
@app.route("/")
def hello():
    print("Handling request to home page.")
    return "Hello, Azure Im Weather ETL !"
# Create an API end point
@app.route("/ingest" , methods=['GET'])
def ingest():
    print("Handling request to ETL Processing.")
    try:
        ##paremetre pays à selectionner leur villes
        country = request.args.get('country')
        #nombre maximale de ville à ingester
        print(request.args.get('limit'))
        print(request.args.get('country'))
        limit=int(request.args.get('limit'))
        ##creation d'un connexion
        data_to_str=json.dumps('No Data ')
        session=connection()
        print(session)
        for city in getCities(country,100):
                
                url=url_builder(city,'','')
                #Invocation du API afin de recuperer les données
                data=data_fetch(url)
                #Formatage des données
                data_organized=data_organizer(data)
                # On supprime la clé "wind_deg" car sa valeur est toujours nulle
                if 'wind_deg' in data_organized.keys(): del data_organized["wind_deg"]
                
                # On caste en string le dictionnaire 
                data_to_str = json.dumps(data_organized,ensure_ascii=False)
                # On ajoute des "'" en début et fin de string pour respecter la syntaxe de la fonction JSON
                # On remplace également les "'" par des espaces pour éviter des problèmes de format
                data_to_str = "'"+data_to_str.replace("'"," ")+"'"
                # On effectue l'insertion dans la table avec une query CQL, en utilisant la fonction JSON
                session.execute("INSERT INTO weather.cities_table JSON {}".format(data_to_str))  
    except IOError:
        print('no internet')
    
      # Return a json object containing the features and prediction
    return jsonify(resultat=json.dumps(data_to_str.replace("'"," "), sort_keys=True))

     #time.sleep(60)
    # compile: connect, crée le keyspace et la table

if __name__ == '__main__':
    # Run the app at 0.0.0.0:3333
    app.run(port=3333,host='0.0.0.0')
