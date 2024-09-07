import requests
import json
import pandas as pd 

url = 'https://www.fruityvice.com/api/fruit/all'

response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    df = pd.DataFrame(data)
    df.set_index("name", inplace=True)

else:
    print("somethings off")

top_calories = df["nutritions"].str["calories"].nlargest(10)
top_sugar = df["nutritions"].str["sugar"].nlargest(10)

joined = pd.concat([top_calories, top_sugar], axis = 'columns',keys=["Calories","Sugar"], join='inner').sort_values("Calories", ascending=False)

print(joined)
