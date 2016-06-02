# -*- coding: utf-8 -*-
"""
Created on Mon May 30 16:44:04 2016

@author: xinruyue
"""

from pymongo import MongoClient
import datetime
import csv
import sys

reload(sys)
sys.setdefaultencoding('utf8')


users = MongoClient("10.8.8.111:27017")['onions']['users']
userAttr = MongoClient("10.8.8.111:27017")['cache']['userAttr']

#get vip users
def vip_users(startTime):

    pipeline = [
    {"$match":{"VIPExpirationTime":{"$gte":startTime}}},
    {"$group":{"_id":"None","user":{"$addToSet":"$_id"}}}]
    
    VIPUsers = list(users.aggregate(pipeline))[0]['user']
    return VIPUsers


startTime = datetime.datetime(2016,6,9,16)
userId = vip_users(startTime)

print len(userId)

location = []
for each in userId:
    pipeline = [
    {"$match":{"user":each}},
    {"$group":{"_id":None,"loc":{"$push":"$location"}}}]
    
    result = list(userAttr.aggregate(pipeline))
    if len(result) == 0:
        print each
    else:
        location += result[0]['loc']
        
print len(location)

csvfile = file('VIPUser_map_1.csv','wb')
writer = csv.writer(csvfile)

location.sort()

location_1 = []
map_data = {}
writer.writerow(['loc','num'])
for each in location:
    writer.writerow([each])
    each_1 = each[:3]
    location_1.append(each_1)

print len(location_1)

for each in location_1:
    map_data[each] = location_1.count(each)

for keys,values in map_data.items():
    print keys
    print values
    
csvfile.close()




