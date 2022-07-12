#!/usr/bin/env python
# coding: utf-8

# In[135]:


### from pyspark.sql import SparkSession
from pyspark.sql.functions import col,schema_of_json
import pyspark.sql.functions as F
import random
import json
'''
在第一次編寫的時候不太確定要根據日期排序後來再判斷縣市，後來的想法是如果先判斷好縣市再根據日期排序
所以原本有合併資料集但後來就分開使用了。
'''

spark = SparkSession.builder.getOrCreate()

df_A = spark.read\
        .option('header',True)\
        .option('escape','"')\
        .csv('A_lvr_land_A.csv')
df_B = spark.read\
        .option('header',True)\
        .option('escape','"')\
        .csv('B_lvr_land_A.csv')
df_E = spark.read\
        .option('header',True)\
        .option('escape','"')\
        .csv('E_lvr_land_A.csv')
df_F = spark.read\
        .option('header',True)\
        .option('escape','"')\
        .csv('F_lvr_land_A.csv')
df_H = spark.read\
        .option('header',True)\
        .option('escape','"')\
        .csv('H_lvr_land_A.csv')
'''

這邊本來是要合併
df_test = df_A.union(df_B)
df_test = df_test.union(df_E)
df_test = df_test.union(df_F)
df_test = df_test.union(df_H)
'''


'''
這是轉換用的功能，因為第一次使用pyspark，所以還不太清楚DF轉換JSON的相關操作，查一查後就乾脆自己寫
因為json本身我對於他的理解就是文件，所以也是可以直接寫成文件的，只要規則對。
'''
def to_json(all_list):
    #這邊要判斷行政區為哪一個縣市來給city賦值
    taipei = list(df_A.groupby("鄉鎮市區").count().toLocalIterator())
    taichung = list(df_B.groupby("鄉鎮市區").count().toLocalIterator())
    kaohsiung = list(df_E.groupby("鄉鎮市區").count().toLocalIterator())
    new_taipei = list(df_F.groupby("鄉鎮市區").count().toLocalIterator())
    taoyuan = list(df_H.groupby("鄉鎮市區").count().toLocalIterator())
    #將DF轉成list
    temp_list = list(all_list.toLocalIterator())
    temp_list_len = len(temp_list)
    #找有幾組交易年月日 方便之後配對
    get_all_date = all_list.groupby("交易年月日").count()
    all_date = list(get_all_date.toLocalIterator())
    all_json = []
    json_end = "}]}"
    #日期迴圈，給予每次判斷的日期
    for i in range(len(all_date)):
        get_time = all_date[i][0]
        time_list = list(get_time)
        re_time = f"{time_list[0]}{time_list[1]}{time_list[2]}-{time_list[3]}{time_list[4]}-{time_list[5]}{time_list[6]}"
        json_format = '"date":"%s","events":['%(re_time)
        same_date_data = []
        count = 1
        #資料迴圈，用來判斷日期是否相同
        for k in range(temp_list_len):
            #第一個條件要判斷日期是否相同
            if temp_list[k][7] == all_date[i][0]:
                #這邊判斷結尾要給逗號與否跟是否有]結尾
                if count == all_date[i][1] and i != len(all_date)-1:
                    json_date = '{"district":"%s","building_state":"%s"}],'%(temp_list[k][0],temp_list[k][11])
                    same_date_data.append(json_date)
                elif count != all_date[i][1] and i != len(all_date)-1:
                    json_date = '{"district":"%s","building_state":"%s"},'%(temp_list[k][0],temp_list[k][11])
                    same_date_data.append(json_date)
                elif count != all_date[i][1] and i == len(all_date)-1:
                    json_date = '{"district":"%s","building_state":"%s"},'%(temp_list[k][0],temp_list[k][11])
                    same_date_data.append(json_date)
                elif count == all_date[i][1] and i == len(all_date)-1:
                    json_date = '{"district":"%s","building_state":"%s"}]'%(temp_list[k][0],temp_list[k][11])
                    same_date_data.append(json_date)
                #因為上面已經找出每一組共有幾個，所以用count來判斷是否到最後一組
                count += 1
        #將同一個時間的資料放進去
        for data in same_date_data:
            json_format = json_format + data
        all_json.append(json_format)
    #判斷縣市
    for c in range(len(taipei)):
        if temp_list[0][0] in taipei[c][0]:
            location = "臺北市"
    for c in range(len(taichung)):
        if temp_list[0][0] in taichung[c][0]:
            location = "台中市"
    for c in range(len(kaohsiung)):
        if temp_list[0][0] in kaohsiung[c][0]:
            location = "高雄市"
    for c in range(len(new_taipei)):
        if temp_list[0][0] in new_taipei[c][0]:
            location = "新北市"
    for c in range(len(taoyuan)):
        if temp_list[0][0] in taoyuan[c][0]:
            location = "桃園市"
    
    json_head = '{"city":"%s","time_slots":[{'%(location)
    all_json_str = ''.join(all_json)
    #將字串組合
    json_result = json_head + all_json_str + json_end
    return json_result


all_result = []

#這邊先根據條件去篩選跟排序
taipei_result = df_A.filter((col("建物型態").rlike("住宅大樓") & (df_A.主要用途 == "住家用") & (df_A.總樓層數 >= 13))).sort(df_A.交易年月日.desc())
json_taipei = to_json(taipei_result)


taichung_result = df_B.filter((col("建物型態").rlike("住宅大樓") & (df_B.主要用途 == "住家用") & (df_B.總樓層數 >= 13))).sort(df_B.交易年月日.desc())
json_taichung = to_json(taichung_result)


kaohsiung_result = df_E.filter((col("建物型態").rlike("住宅大樓") & (df_E.主要用途 == "住家用") & (df_E.總樓層數 >= 13))).sort(df_E.交易年月日.desc())
json_kaohsiung = to_json(kaohsiung_result)


new_taipei_result = df_F.filter((col("建物型態").rlike("住宅大樓") & (df_F.主要用途 == "住家用") & (df_F.總樓層數 >= 13))).sort(df_F.交易年月日.desc())
json_new_taipei = to_json(new_taipei_result)


taoyuan_result = df_H.filter((col("建物型態").rlike("住宅大樓") & (df_H.主要用途 == "住家用") & (df_H.總樓層數 >= 13))).sort(df_H.交易年月日.desc())
json_taoyuan = to_json(taoyuan_result)


result_file_1 = 'result-part1.json'
result_file_2 = 'result-part2.json'
#這邊是寫入資料
f = open(result_file_1,'a',encoding='utf-8-sig')
f.write(json_taipei)
f.write(json_taichung)
f.write(json_kaohsiung)
f.close()
f2 = open(result_file_2,'a',encoding='utf-8-sig')
f2.write(json_new_taipei)
f2.write(json_taoyuan)
f2.close()

