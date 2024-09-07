import pandas as pd 
import requests

url = 'https://www.fruityvice.com/api/fruit/all'
response = requests.get(url)

def extract_values(dictionary):
    Calories = dictionary['calories']
    Sugar = dictionary['sugar']
    return Calories, Sugar

if response.ok:
    #Fruits
    json_data = response.json()
    fruits = pd.DataFrame(json_data)
    fruits.set_index('name', inplace=True)
    fruits[['Calories', 'Sugar']] = fruits['nutritions'].apply(lambda x: pd.Series(extract_values(x)))
    fruits = fruits[["Calories","Sugar"]]
 
    person = pd.read_csv("fruit_transactions.csv", parse_dates=["Date"])
    person['YrMonth'] = person['Date'].dt.strftime('%Y%m')
    person_fruit = person.merge(fruits, how='left', left_on='Fruit', right_index=True)
    person_fruit_agg = person_fruit.groupby(['Name','YrMonth'])
    person_fruit_agg = person_fruit_agg[['Calories','Sugar']].sum()
    # measure = input(" Which Metric do you want to filter by: ")
    print(person_fruit_agg)
    # print(person_fruit_agg.nlargest(1,measure))






