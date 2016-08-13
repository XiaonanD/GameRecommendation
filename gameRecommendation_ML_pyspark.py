
########Use Spark MLlib and Spark SQL to do personalized recommendtion#####
## use pyspark

import json, time
import pandas as pd
import numpy as np
from pyspark.sql import SQLContext
from pyspark.mllib.recommendation import ALS

####get (user_id,appid,playtime_forever)-user_rdd and get (user_id,index) t_rdd####

# load from web-scrawled 'user_new.txt': user's game usage file 
# and get (user_id,appid,playtime_forever)-user_rdd
def parse_user(raw_string):
    user_inventory = json.loads(raw_string)
    user_id,lst_user_inventory = user_inventory.items()[0]
    if not lst_user_inventory == None:
        try:
            return [(user_id, i.get('appid'),i.get('playtime_forever')) for i in lst_user_inventory]
        except:
            return []    
    else:
        return []

user_rdd = sc.textFile('user_new.txt').flatMap(parse_user)

# load from web-scrawled 'user_new.txt': user's game usage file 
# and only get user_id and later will assign index for them. t_rdd
def parse_id_index(raw_string):
    user_inventory = json.loads(raw_string)
    user_id,lst_user_inventory = user_inventory.items()[0]
    if not lst_user_inventory == None:
        return user_id
    else:
        return 

t_rdd = sc.textFile('user_new.txt').map(parse_id_index).zipWithIndex()
dic_id_index = t_rdd.map(lambda (user_id,index) : (index,user_id)).collectAsMap()


# save t_rdd and user_rdd to Dataframe and operate
df1 = sqlContext.createDataFrame(t_rdd,('key','index'))
df2 = sqlContext.createDataFrame(user_rdd,('key','appid','rating'))
df1.registerTempTable('df1')
df2.registerTempTable('df2')

# join two dataframes on user_id(key) and select information we want form a new dataframe
data =df1.join(df2,df1.key == df2.key).select(df1.index,df2.appid,df2.rating)
#how many (index, appid, rating) tuples in total
data.count()  # count: 393512


#################recommendation model training##################
model = ALS.train(data, rank=2, seed=0) #rank a hyperparameter: hidden feature-matrix:k

# after modeling, now to see metric evaluation (Root Mean Square Error)

#predictAll to get predictions ((index, appid),rating)
testdata = data.map(lambda p: (p[0],p[1]))
predictions = model.predictAll(testdata).map(lambda r:((r[0],r[1]),r[2]))
#r[0]: index; r[1]: appid; r[2] play_time: ratings
#combine true rating and prediction together--ratesAndPreds
ratesAndPreds = data.map(lambda r:((r[0],r[1]),r[2])).join(predictions)
# Root Mean Square Error
from math import sqrt
MSE = ratesAndPreds.map(lambda r: (r[1][0]-r[1][1])**2).mean()
RMSE = sqrt(MSE)
print("Root Mean Square Error =" +str(RMSE))


# personalized recommendation, get each user top 10 recommendation game products!
dic_recommend = {}
for index in dic_id_index.keys():
    try:
        lst_recommend = [i.product for i in model.recommendProducts(index,10)]
        user_id = dic_id_index.get(index)
        dic_recommended.update({user_id:lst_recommend})
    except:
        pass

json.dump(dic_recommend, open('recommended_games.json','w'),indent =3)
