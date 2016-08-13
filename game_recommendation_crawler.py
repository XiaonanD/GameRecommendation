##### SteamAPI data crawler####

#######################################
####Crawler 1: get User_information####
#######################################

##get user information: user.txt
##http://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/?key=1B96AE93E39BA4E43F9F78BC6AFEE4FF&steamid=76561198074188133&format=json
## 1) Get txt file and open by pd.DataFrame(use pd.DataFrame is easier to do batch processing)
import pandas as pd
df = pd.read_csv("steam_user_id.txt",header=None)
df.columns = ['steamID'] # give it a col_name

## 2) Do requests API 
import requests
import json
my_key ='XXXXXXXXXX'

def steamAPI(query):
    load = {'key': my_key,
           'steamid': query,
           'format': json}
    r = requests.get('http://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/',
                    params = load)
    return r.json()

## 3) put all info, get data from steamAPI
import time
### Do a batch processing, get each 1000 info and write into a txt. Do it 5 times.
start = time.time()
for num in range(5):
    count = num*1000
    steamLst =[]
    for query in df['steamID'][num*1000:num*1000+1000]:
        if (count%100 ==0):
            print ("%d is working" %count)
        count +=1
        query = str(query)
        q =steamAPI(query)
        steamLst.append(q)
        #print('appid:',q['response']['games'][2]['appid']) # just a test
        time.sleep(1) 
    print ("finish getting %d to %d" %(num*1000,num*1000+999))
    
    #3) write and dump json into a txt file
    name = ('%s_part.txt'%num)
    f = open(name,'w')
    for item in steamLst:
        f.write(json.dumps(item)+'\n')
    f.close()
    print ("now %s written into files."%name)

print
print ("total time: %d seconds"%(time.time()-start))

####combine several txt files into one with orders

file_list = ['0_part.txt','1_part.txt','2_part.txt','3_part.txt','4_part.txt']
f = open('whole.txt','w')

#write each each part into whole
for fname in file_list:
	infile = open(fname,'r')
	f.write(infile.read()+'\n')

f.close()

#get whole.txt into dataframe
df_info = pd.read_csv("whole.txt",header=None,sep='delimiter')
df_info.columns = ['detail']

#get each id's detail information(the value part in whole.txt)
df_info['info']= df_info['detail'].apply(lambda row: json.loads(row).get('response').get('games'))
#now get each UserSteamID and its corresponding detail info.
df_all = pd.concat([df,df_info['info']],axis = 1)

f = open('user.txt','w')
for i in range(df_all.shape[0]):
    f.write(json.dumps({df_all['steamID'][i]:df_all['info'][i]}))
    f.write('\n')
f.close()
    

#########################################
#### crawler 2: get Game_app details ####
#########################################

## get all game_app details: all_appids.txt

###1) Get all information(including all appids) from steamSpyAPI###
url = 'http://steamspy.com/api.php?request=all'
info = requests.get(url)
#a will be a full list of appid.
a= info.keys()
df_id = pd.DataFrame(a)
df_id.columns = ['appID'] # give it a col_name

def steamAPI(query):
    load = {'appids': query,
           'format': json}
    r = requests.get('http://store.steampowered.com/api/appdetails',
                    params = load)
    return r.json()

## 2) put all info, get data from steamAPI, there are 9579 appids  in total.
### Do a batch processing, get each 1000 info and write into a txt. To avoid connection error## 
start = time.time()
# for first 9000 appids:
for num in range(9):
    count = num*1000
    steamLst =[]
    for query in df['appID'][num*1000:num*1000+1000]:
        if (count%100 ==0):
            print ("%d is working" %count)
        count +=1
        query = str(query)
        q =steamAPI(query)
        steamLst.append(q)
        #print('appid:',q['response']['games'][2]['appid']) # just a test
        time.sleep(0.70) 
    print ("finish getting %d to %d" %(num*1000,num*1000+999))
       
    #) write and dump json into a txt file
    name = ('%s_part_appid.txt'%num)
    f = open(name,'w')
    for item in steamLst:
        f.write(json.dumps(item)+'\n')
    f.close()
    print ("now %s written into files."%name)

#for the last 579 appids:
num = 9
count = num*1000
steamLst =[]
for query in df['appID'][num*1000:]:
    if (count%100 ==0):
        print ("%d is working" %count)
    count +=1
    query = str(query)
    q =steamAPI(query)
    steamLst.append(q)
        #print('appid:',q['response']['games'][2]['appid']) # just a test
    time.sleep(0.70) 
print ("finish getting %d to %d" %(num*1000,count))
    
#write and dump json into a txt file
name = ('%s_part_appid.txt'%num)
f = open(name,'w')
for item in steamLst:
f.write(json.dumps(item)+'\n')
f.close()
print ("now %s written into files."%name)
print
print ("total time: %d seconds"%(time.time()-start))

##combine all txts into one final file
file_list = []
for num in range(10):
    name = ('%s_part_appid.txt'%num)
    file_list.append(name)

f = open('all_appids.txt','w')

#write each each part into all_appids.txt
for fname in file_list:
	infile = open(fname,'r')
	f.write(infile.read()+'\n')

f.close()

