import codecs
import re
import pandas as pd

path="D:\\google_review_data\\users.clean.json\\users.clean.json"

f=codecs.open(path,"r",encoding="UTF-8")

Count=0
temp=''
data=[]
for line in f:
    z=line.encode('ascii').decode('unicode-escape')
    z=z.replace("u'","'")
    z=z.replace('u"',"'")
    z=z.replace('", ',"', ")
    Count+=1

# Check for the consistency in the data like if the line end with last second character as "}" then its a complete line or else there are some Unnecessary line breaks in the data. If there are unnecessary line breaks in the data the code joins such lines by removing those breaks.
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
    # if(z.find("rating")>=0 and z.find("gPlusUserId")>=0):
    # if Count==50000:
    #     break

#Here with the help of regular expression we extract the required data from the raw data file of users.

    if(z.find("gPlusUserId")>=0):
        m = re.search("'userName': '(.+?)', 'jobs'", z)

        r = re.search("'gPlusUserId': '(.+?)'}", z)
        print(Count)
        try:

            output=m.group(1),r.group(1)
            data.append(output)
        except:
            print(z)

users = pd.DataFrame(data,columns =['UserName','gPlusUserId'])

# """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
# """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
# Here we do inner join between the english reviews file and users data extracted.  we perform the Inner join on gPlusUserId.
# we do inner join so that we can get the data of only those users which are present in our sample review file (reviews.csv) Rest all the users whose gPLusUserId is not there in review file will be dropped.

review=pd.read_csv("ReviewEnglishOnly.csv")  #filtered file which has only english observations. filter with lang=='en'
y=pd.merge(review,users,how='inner',on='gPlusUserId')
# After performing the join we drop all the duplicate observations of gPlusUserId. by this we get the data of only distinct users repeated gPLusUserId are dropped.
y=y.drop_duplicates(subset=["gPlusUserId"])
columns=['UserName','gPlusUserId']
y[columns].to_csv("usersfinal.csv")
