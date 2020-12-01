import pandas as pd

review=pd.read_csv("ReviewEnglishOnly.csv")
review['type']="directed"

df = pd.DataFrame(review, columns= ['gPlusUserId','gPlusPlaceId','type'])

network = df.pivot_table(index=['gPlusUserId','gPlusPlaceId','type'], aggfunc='size').reset_index().rename(columns={0:"Weight"})
columns=['gPlusUserId','gPlusPlaceId','type','Weight']
# network=network.rename(columns={'gPlusUserId':'Source','gPlusPlaceId':'Target','type':'Type',0:'Weight'})
network[columns].to_csv("network1.csv",index=False)


import pandas as pd
review=pd.read_csv("network1.csv")
print(review)
# df=(review.groupby('gPlusPlaceId').size())
# df=review.gPlusPlaceId,review.gPlusPlaceId.value_counts()
# df['gPlusPlaceId']=review['gPlusPlaceId']
# columns=['gPlusPlaceId','count']
df=(review.groupby('gPlusPlaceId').size()).reset_index().rename(columns={0:"repeat_Place"})
print(df)
df.to_csv('uniquegplusPlace.csv',index=False)
df=pd.read_csv('uniquegplusPlace.csv')
x=pd.merge(review,df,how='inner',on='gPlusPlaceId')
x.to_csv("merged1.csv", index=False)

# df1=review.gPlusUserId.value_counts()
df1=(review.groupby('gPlusUserId').size()).reset_index().rename(columns={0:"repeat_Users"})
print(df1)
df1.to_csv('uniquegplususers.csv',index=False)
df1=pd.read_csv('uniquegplususers.csv')

y=pd.merge(x,df1,how='inner', on='gPlusUserId')
y.to_csv("merged2.csv", index=False)
y=y.loc[y['repeat_Users']>=2]
y=y.loc[y['repeat_Place']>=2]
y=y.sample(n=1500)
y.to_csv('network.csv',index=False)

# y=review.join(df, on='gPlusUserId', how='inner',lsuffix=0,rsuffix=0)
# y=pd.merge(review,df,how='inner',on='gPlusPlaceId')
# y=pd.merge(y,df1,how='inner',on='gPlusUserId')
# # columns=["gPlusUserId","gPlusPlaceId",'type','0','repeat']
# y=y.loc[y['repeat_Users']>=2]
# y=y.loc[y['repeat_Place']>=2]
#
# columns=["gPlusUserId","gPlusPlaceId",'type','Weight','repeat_Users','repeat_Place']
# y[columns]=y
# # y=y[columns].reset_index().rename(columns={'gPlusUserId':'Source','gPlusPlaceId':'Target','type':'Type','Weight':'Weight','repeat':'Repeat'})
# y=y.sample(n=2000)
# y.to_csv("Network.csv", index=False)
# review = review.pivot_table(index=['gPlusUserId'], aggfunc='size')
#
# columns=["gPlusUserId","gPlusPlaceId","type",'0','repeatitionid',"times"]
# review[columns].to_csv("testing.csv")
