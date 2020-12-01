import re
from langdetect import detect
import codecs
import pandas as pd

# Reading the raw data file and replaceing the unnecessary quotation and line breaks in the file.
# In the end we pull up a sample of 1000000 observations from the file"""
reviewsfileloc = "D:\\google_review_data\\reviews.clean.json\\reviews.clean - Copy.txt"

f=codecs.open(reviewsfileloc,"r",encoding="UTF-8")
count=0
#The data has many discripency like having unnecessary qutotaions etc so we first try to understand the pattern the data is in and replace those discripencies.
y=open("sample.txt","w",encoding="UTF-8",newline="")
for line in f:
    z=line.encode('ascii').decode('unicode-escape')
    z=z.replace(": u",":")
    z=z.replace("[u","[")
    z=z.replace(', u',',')
    z=z.replace("\n",'')
    z=z.replace("}","}\n")
    z=z.replace('"','\\"')
    z=z.replace("'rating':",'"rating":')
    z=z.replace("'reviewerName':'",'"reviewerName":"')
    z=z.replace("', '",'", "')
    z=z.replace("':'",'":"')
    z=z.replace("': ['",'": ["')
    z=z.replace("'], '",'"], "')
    z=z.replace("unixReviewTime': ",'unixReviewTime": ')
    z=z.replace(", 'reviewTime",', "reviewTime')
    z=z.replace("reviewText':",'reviewText":')
    z=z.replace("','",'","')
    z=z.replace('.\\", ','.", ')
    z=z.replace('?\\", ','?", ')
    z=z.replace("'categories",'"categories')
    z=z.replace('":\\"','":"')
    z=z.replace('\\", "','", "')
    z=z.replace("Clothing Store',",'Clothing Store",')
    z=z.replace('",\\"','","')
    z=z.replace('\\",\\"','","')
    z=z.replace('\\"],','"],')
    z=z.replace('\\","','","')
    z=z.replace(", 'gPlusPlaceId",', "gPlusPlaceId')
    z=z.replace("categories':",'categories":')
    z=z.replace('": [\\"','": ["')
    z=z.replace("'}",'"}')
    z=z.replace("'reviewerName':",'"reviewerName":')
    z=z.replace(", 'reviewText",', "reviewText')
    z=z.replace('":\\"','":"')
    z=z.replace('\\", "','", "')
    z=z.replace('\\"refrigerator\\"',"'refrigerator'")
    y.write(z)
    count=count+1
#Here we extracting only the first 1000000 observations from the data as the file is too big to execute the code.
    if count==1000000:
        break

y.close()


f.close()

# """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
# """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
# when we see the sample file we notice that many observations are divided into 2-3 line because there are line breaks in the data. So we try to join those observations back into one.

f=codecs.open("sample.txt","r",encoding="UTF-8")
y=open("file_without_line_breaks.txt","w",encoding="UTF-8",newline="\n")
temp=""
count=0
for line in f:
    count=count+1
    if len(line)>1:
        if line[-2]=='}':# A given observation is correct when the last second character is "}"
            temp=temp.strip('\r\n') + '' + line

            y.write(temp,)
            if count==1000000:
                break
            temp=""
        elif line[-2]!='}': # if the given line is not having last second character ending with "}" then it is stored in a Temp variable and strip any line breaks at the end of that line.
            temp=temp+line.strip('\r\n')

# """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
# """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
# Here as the file still has lots of discrepancy in the data we use regular expression to pull out the Data from the Raw data file and store it in the form of Pandas data frame in CSV
# Here we also detect in which language the review text is been given so that in the end we can filter out


x=codecs.open("file_without_line_breaks.txt","r",encoding="UTF-8")
count=0
data=[]
for i in x:
    count = count+1
    print(count)
# when we parse the data we observe that the data is still not in correct format so we use the replace function and replace all the naming conventions in the format we need and make the data in the format needed.
    if(i.find("rating")>=0 and i.find("gPlusUserId")>=0):
        i.replace("'rating'",'"rating"')
        i.replace("'reviewerName'",'"reviewerName"')
        i.replace("'reviewText'",'"reviewText"')
        i.replace("'categories'",'"categories"')
        i.replace("'gPlusPlaceId'",'"gPlusPlaceId"')
        i.replace("'unixReviewTime'",'"unixReviewTime"')
# so we use regular expression here and extract the data with the help of naming convention pattern.
        m = re.search('"rating":(.+?), "reviewerName"', i)
        n = re.search('"reviewerName":(.+?), "reviewText"', i)
        o = re.search('"reviewText":(.+?), "categories"', i)
        p = re.search('"categories":(.+?), "gPlusPlaceId"', i)
        q = re.search('"gPlusPlaceId":(.+?), "unixReviewTime"', i)
        r = re.search('"unixReviewTime":(.+?), "reviewTime', i)
        s = re.search('"reviewTime(.+?)gPlusUserId"', i)
        t = re.search('gPlusUserId":"(.+?)"', i)
        # we use try and except here because for some observations we are not able to identify the language. so we except those observations
        try: #here we use detect() function to detect in which language the review text is in.
            output= m.group(1),n.group(1),o.group(1),detect(o.group(1)),p.group(1),q.group(1),r.group(1),s.group(1),t.group(1)
            data.append(output)
        except:
            language = "error"# if the function is not able to find the language we drop those exceptions.
            print("This row throws and error:", o.group(1))
# write the data in pandas Data frame.
result = pd.DataFrame(data,columns =['Rating', 'ReviewerName', 'ReviewerText','Lang','Categories','gPlusPlaceId','UnixReviewTime','ReviewTime','gPlusUserId'])
result.to_csv("Review_sentiment.csv")

#As the ReviewText column has lots of double quotes in the text data it becomes difficult for the text analysis so we remove all the double quotes from the data with the help of replace function.
result['ReviewerText'] = result['ReviewerText'].replace({'"':''}, regex=True)
result['ReviewerName']=result['ReviewerName'].replace({'"':''}, regex=True)
result['Lang']=result['Lang'].replace({'"':''}, regex=True)
result['Categories']=result['Categories'].replace({'"':''}, regex=True)
result['gPlusPlaceId']=result['gPlusPlaceId'].replace({'"':''}, regex=True)
result['UnixReviewTime']=result['UnixReviewTime'].replace({'"':''}, regex=True)
result['ReviewTime']=result['ReviewTime'].replace({'"':''}, regex=True)
result['gPlusUserId']=result['gPlusUserId'].replace({'"':''}, regex=True)
result = pd.DataFrame(result,columns =['Rating', 'ReviewerName', 'ReviewerText','Lang','Categories','gPlusPlaceId','UnixReviewTime','ReviewTime','gPlusUserId'])
# In the end we write all the clean data in output1.csv file
result.to_csv("Reviews.csv")
# print(data['ReviewerText'])


