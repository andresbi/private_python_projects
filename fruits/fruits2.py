import pandas as pd 
import requests 
import json 

transactions = pd.read_csv("fruit_transactions.csv", parse_dates=["Date"])
# print(transactions)

url = 'https://www.fruityvice.com/api/fruit/all'
response = requests.get(url)
desired_fruits = pd.read_csv("desired_fruits.csv")["fruit"].str.title()

if response.ok: 
    data = response.json()
    df = pd.DataFrame(data)
    df.set_index('name', inplace=True)
    merged_df = df[df.index.isin(desired_fruits)]
    # print(merged_df)
else: 
    print("something wrong")

person_fruit_attributes = transactions.merge(df, how='left', left_on="Fruit", right_index=True).sort_values("Name")

person_fruit_attributes['Calories'] = person_fruit_attributes['nutritions'].str['calories']
person_fruit_attributes['Sugar'] = person_fruit_attributes['nutritions'].str['sugar']
person_fruit_attributes.drop(['nutritions'],axis = 1,inplace=True)
person_fruit_attributes = person_fruit_attributes[['Name','Date','Fruit','Calories','Sugar']]

# Create the group by object
person = person_fruit_attributes.groupby('Name')
# Create the aggregation
intake_per_person = person[['Sugar','Calories']].sum().sort_values("Sugar", ascending=False)

top_eater = intake_per_person.nlargest(1,columns=['Sugar'])


print(top_eater)