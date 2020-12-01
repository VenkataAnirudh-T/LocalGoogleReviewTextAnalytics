# Extracting Data from Places raw data file:
import codecs
import re
from datetime import datetime
import pandas as pd
import reverse_geocoder as rg
import pprint

path="D:\\google_review_data\\places.clean.json\\places.clean.json"
f=codecs.open(path,"r",encoding="UTF-8")

Count=0
temp=''
data=[]
data1=[]
data2=[]
data3=[]

for line in f:

    z=line.encode('ascii').decode('unicode-escape')
    z=z.replace("u'","'")
    z=z.replace('u"',"'")
    z=z.replace('", ',"', ")
    z=z.replace("\n"," ")
    Count+=1
# the below code checks if there are any unnecessary line breaks in the data. It removes those line breaks and joins the data.
    # print(z)
    # if len(z)>1:
    #     if z[-2]=='}':
    #         temp=temp.strip('\r\n') + '' + z
    #         # print(temp)
    #         # z.write(temp,)
    #         temp=""
    #     elif z[-2]!='}':
    #         temp=temp+z.strip('\r\n')
    #         print(temp)
    if(z.find("gPlusPlaceId")>=0):# Here we exract the data with the help of the regular expression
        m = re.search("'name': '(.+?)', 'price'", z)
        n = re.search("'price': (.+?), 'address'", z)
        p = re.search("'hours': (.+?), 'phone'", z)
        q = re.search("'phone': (.+?), 'closed'", z)
        r = re.search("'closed': (.+?), 'gPlusPlaceId'", z)
        s = re.search("'gPlusPlaceId': '(.+?)', 'gps'", z)
        t = re.search("'gps': (.+?)}", z)
        coord=t.group(1).replace("[","").replace("]","")
        now=datetime.now()
        current_time=now.strftime("%H:%M:%S")
        print(Count,current_time)
        if Count <=1000000:# Here we divide the complete data into 4 files as the single list variable is not able to store alot of data. Also once we have converted the required data into data frame we make list empty again to free up the memory.
            try:
                output= m.group(1),n.group(1),p.group(1),q.group(1),r.group(1),s.group(1),coord
                data.append(output)
                if Count==1000000:
                    result = pd.DataFrame(data,columns =['Name','price','hours','phone','closed','gPlusPlaceId','Coordinates'])

                    result=0
                    data=[]
            except:
                print(z)
                break

        elif Count >1000000 and Count<=2000000:
            try:
                output1= m.group(1),n.group(1),p.group(1),q.group(1),r.group(1),s.group(1),coord
                data1.append(output1)
                if Count==2000000:
                    result1 = pd.DataFrame(data1,columns =['Name','price','hours','phone','closed','gPlusPlaceId','Coordinates'])

                    result1=0
                    data1=[]
            except:
                print(z)
                break
        elif Count >2000000 and Count<=3000000:
            try:
                output2= m.group(1),n.group(1),p.group(1),q.group(1),r.group(1),s.group(1),coord
                data2.append(output2)
                if Count==3000000:
                    result2 = pd.DataFrame(data2,columns =['Name','price','hours','phone','closed','gPlusPlaceId','Coordinates'])

                    result2=0
                    data2=[]
            except:

                print(z)
                break
        elif Count >3000000:
            try:
                output3= m.group(1),n.group(1),p.group(1),q.group(1),r.group(1),s.group(1),coord
                data3.append(output3)
            except:

                print(z)
                break

result3 = pd.DataFrame(data3,columns =['Name','price','hours','phone','closed','gPlusPlaceId','Coordinates'])

# """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
# """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

#inner joining of place files with review file by gPlusPlaceId
review=pd.read_csv("ReviewEnglishOnly.csv")

y=pd.merge(review,result,how='inner',on='gPlusPlaceId')


y1=pd.merge(review,result1,how='inner',on='gPlusPlaceId')


y2=pd.merge(review,result2,how='inner',on='gPlusPlaceId')


y3=pd.merge(review,result3,how='inner',on='gPlusPlaceId')


frames=[y,y1,y2,y3]

merged=pd.concat(frames)
merged=merged.drop_duplicates(subset=["gPlusPlaceId"])
columns=['Name','price','hours','phone','closed','gPlusPlaceId','Coordinates']
print(merged[columns])
merged[columns].to_csv('final.csv')

# """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
# """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
# identifying city, State, country from latitute and longitute

pp = pprint.PrettyPrinter(indent=4)
country = []
state=[]
city=[]
count=0
merged[['latitute','Longitute']]= merged.Coordinates.str.split(',',expand=True)

for i,j in zip(merged['latitute'],merged['Longitute']) :

    count+=1

    print(count)
    if i==None or j== None: # if any row is missing either latitute or Longitute we declare Country, State and City for those observations as "UNKWN"
        country1="UNKWN"
        state1="UNKWN"
        city1="UNKWN"
        country.append(country1)
        state.append(state1)
        city.append(city1)
    else:# Here we convert the string variables to float.
        i=float(i)
        j=float(j)

        x=(i,j)
        result = rg.get(x, mode=1)# get the information from the reverse geocoder by providing the latitute and longitute.
        if result['cc']=="" and result['admin1']!="" and result['admin2']!="":
            country1="UNKWN"
            state1=result['admin1']
            city1=result['admin2']
            country.append(country1)
            state.append(state1)
            city.append(city1)
        elif result['admin1']==""and result['cc']!="" and result['admin2']!="":
            country1=result['cc']
            state1="UNKWN"
            city1=result['admin2']
            country.append(country1)
            state.append(state1)
            city.append(city1)
        elif result['admin2']=="" and result['cc']!="" and result['admin1']!="":
            country1=result['cc']
            state1=result['admin1']
            city1="UNKWN"
            country.append(country1)
            state.append(state1)
            city.append(city1)
        elif result['cc']=="" and result['admin1']=="" and result['admin2']!="":
            country1="UNKWN"
            state1="UNKWN"
            city1=result['admin2']
            country.append(country1)
            state.append(state1)
            city.append(city1)
        elif result['cc']=="" and result['admin2']==""and result['admin1']!="":
            country1="UNKWN"
            state1=result['admin1']
            city1="UNKWN"
            country.append(country1)
            state.append(state1)
            city.append(city1)
        elif result['admin2']=="" and result['admin1']==""and result['cc']!="":
            country1=result['cc']
            state1="UNKWN"
            city1="UNKWN"
            country.append(country1)
            state.append(state1)
            city.append(city1)
        elif result['admin2']=="" and result['admin1']=="" and result['cc']=="":
            country1="UNKWN"
            state1="UNKWN"
            city1="UNKWN"
            country.append(country1)
            state.append(state1)
            city.append(city1)
        elif result['cc']!="" and result['admin1']!="" and result['admin2']!="":
            country1=result['cc']
            state1=result['admin1']
            city1=result['admin2']
            country.append(country1)
            state.append(state1)
            city.append(city1)
merged["city"]=city
merged['state']=state
merged['country']=country

# """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
# """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
# we add price range column here
pricerange=[]
# so Basically in google we can see weather the restaurant prices are low or medium or high. it is bascially denoted by the currency character of that country.
# So here the code acutally checks how many times the currency character repeats like lets say the character is only once then low  price, if the character repeats twice its medium price and if the character repeats thrice its expensive.
for i in merged['price']:
    if i=="None" or i == '':
        print("")
        character=0
        pricerange.append(character)
    else:
        character = i.count(i[1])
        print(character)
        print(i[1])
        pricerange.append(character)
        if character>=4:
            print(i)
            break

merged['pricerange']=pricerange
columns=['Name','price','pricerange','hours','gPlusPlaceId','latitute','Longitute','city','state','country']
merged[columns].to_csv("Local_Busines.csv")

