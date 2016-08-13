import pandas as pd
import numpy as np
import json

df = pd.read_csv('all_appids.txt',header= None,sep='delimiter')
df.columns = ['raw']

###################################################
####1) Extract selected features from raw_data#####
###################################################

feature_list=['steam_appid','is_free','initial_price',
             'release_date','score','recommendation',
             'linux','mac','windows','header_image','name']
for feature in feature_list:
    df[feature]=""

#d =json.loads(df['raw'][0]).values() # this returns a list
for i in range(df.shape[0]):
    try:
        content = json.loads(df['raw'][i]).values()
    except:
        pass
    #1.steam_appid
    try:
        df['steam_appid'][i] = json.loads(df['raw'][i]).keys()[0]
    except:
        pass
    #2.is free or not
    try:
        df['is_free'][i] = content[0]['data']['is_free']
    except:
        pass
    #3.initial_price
    try:
        df['initial_price'][i] = content[0]['data']['price_overview']['initial']
    except:
        pass
    #4.release_date
    try:
        df['release_date'][i] = content[0]['data']['release_date']['date']
    except:
        pass
    #5.d[0]['data']['metacritic']['score']
    try:
        df['score'][i] = content[0]['data']['metacritic']['score']
    except:
        pass
    #6.'recommendation'
    try:
        df['recommendation'][i] = content[0]['data']['recommendations']['total']
    except:
        pass
    #7.d[0]['data']['platforms']['linux'];['mac'];['windows']
    try:
        df['linux'][i] = content[0]['data']['platforms']['linux']
        df['mac'][i] = content[0]['data']['platforms']['mac']
        df['windows'][i] = content[0]['data']['platforms']['windows']
    except:
        pass
    #8.header_image 
    try:
        df['header_image'][i] = content[0]['data']['header_image']
    except:
        pass
    #9. game_name
    try:
        df['name'][i] = content[0]['data']['name']
    except:
        pass

#######################################
###2) load useful data into MySQL######
#######################################

##2) get final full game information, save it to df_final and save a appid_to_csv.csv version######
##with features:'steam_appid','is_free','initial_price','release_date','score',
##'recommendation','linux','mac','windows','header_image','name'

df= df.replace("",np.NaN)
df_final = df.drop('raw',axis =1)
df_final = df_final.dropna(subset=['steam_appid'])  # remove rows where app_ids are N/A.
df_final.to_csv('appid_to_csv.csv',encoding='utf-8',index=None) # save it as a csv.file

###3) load data into MySQL#####
##There will be two tables in MySQL
#a. Load a simple version of product info into MySQL: steam_appid, initial_price, score
from sqlalchemy import create_engine
import mysql.connector

df_simple_product = df_final[['steam_appid','initial_price','score']]
df_simple_product.to_csv('simple_product_table.csv',index = None,encoding='utf-8')

# create a gameRecommendation database in mySQL, now use python to create a table(product_table) in mySQL
engine = create_engine('mysql+mysqlconnector://root:XXXXXX@127.0.0.1/gameRecommendation')
df_simple_product.to_sql('product_table',con=engine,if_exists='replace',index = False)


#b. create a table user_app_pair(from user_app_pair.csv) in mySQL (user_id, app_id, playtime_forever)
df_2 = pd.read_csv('user_app_pair.csv')
engine = create_engine('mysql+mysqlconnector://root:XXXXXX@127.0.0.1/gameRecommendation')
df_2.to_sql('user_app_pair',con=engine,if_exists='replace',index = False)

##then in MySQL create a new table called product_2:app_id, num_players, avg_playtime
#where query: Create table product_2 as (SELECT app_id, COUNT(user_id) AS num_players, 
##             AVG(playtime_forever) AS avg_playtime FROM user_app_pair GROUP BY app_id);

####Now do data analysis: make query to get a product_final table
config = {
    'user' : 'root',
    'passwd' : '0430068',
    'host' : 'localhost',
    'raise_on_warnings' : True,
    'use_pure' : False,
    'database' : 'gameRecommendation'
    }
con = mysql.connector.connect(**config)
query ='SELECT p.app_id, p.num_players, p.avg_playtime, a.initial_price,\
a.score FROM product_table AS a JOIN product_2 AS p ON a.steam_appid = p.app_id'
data = pd.read_sql(query, con)
data.to_csv('final_product.csv',index = False)

### Or just create a final_table in MySQL
#now final_product table
#query = CREATE table final_product as (SELECT p.app_id, p.num_players, p.avg_playtime, 
         #a.initial_price,a.score FROM product_table AS a JOIN product_2 AS p ON a.steam_appid = p.app_id);


#############################################
###3) Data Analysis get insights from data###
#############################################


from sqlalchemy import create_engine
import mysql.connector
######Top 10 game_id on avg_playtime#########
config = {
    'user' : 'root',
    'passwd' : '0430068',
    'host' : 'localhost',
    'raise_on_warnings' : True,
    'use_pure' : False,
    'database' : 'gameRecommendation'
    }
con = mysql.connector.connect(**config)
query ='SELECT * FROM final_product ORDER BY avg_playtime DESC limit 10'
insight1 = pd.read_sql(query, con)

# get a full table with information
df_full = pd.read_csv('appid_to_csv.csv')
df_name = df_full[['steam_appid','name','header_image']]
df_name.columns = ['app_id','name','header_image']
result1 = insight1.merge(df_name,on ='app_id', how = 'left')
result1.to_csv('result1.csv')


####### Top 10 game_id on num_player############
config = {
    'user' : 'root',
    'passwd' : '0430068',
    'host' : 'localhost',
    'raise_on_warnings' : True,
    'use_pure' : False,
    'database' : 'gameRecommendation'
    }
con = mysql.connector.connect(**config)
query ='SELECT * FROM final_product ORDER BY num_players DESC limit 10'
insight2 = pd.read_sql(query, con)

# get a full table with information
result2 = insight2.merge(df_name,on ='app_id', how = 'left')
result2.to_csv('result2.csv')
