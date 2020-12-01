import pandas as pd

review=pd.read_csv("ReviewEnglishOnly.csv")
review['type']="directed"

df = pd.DataFrame(review, columns= ['gPlusUserId','gPlusPlaceId','type'])

network = df.pivot_table(index=['gPlusUserId','gPlusPlaceId','type'], aggfunc='size').reset_index().rename(columns={0:"Weight"})
columns=['gPlusUserId','gPlusPlaceId','type','Weight']
network[columns].to_csv("network1.csv",index=False)

review=pd.read_csv("network1.csv")
print(review)
df=(review.groupby('gPlusPlaceId').size()).reset_index().rename(columns={0:"repeat_Place"})
print(df)
df.to_csv('uniquegplusPlace.csv',index=False)
df=pd.read_csv('uniquegplusPlace.csv')
x=pd.merge(review,df,how='inner',on='gPlusPlaceId')
x.to_csv("merged1.csv", index=False)
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

