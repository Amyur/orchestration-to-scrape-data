import bs4
import requests
import pandas as pd

#request the webpage
responses = []
for i in range(51, 60, 50):  # 1952
    response = requests.get("https://deportes.mercadolibre.com.co/bicicletas-ciclismo/bicicletas/_Desde_{}_NoIndex_True".format(i))
    responses.append(response)

#bring the html
soups = []
for i in range(len(responses)):
    soup = bs4.BeautifulSoup(responses[i].text, "html.parser")
    soups.append(soup)

#extract titles
titles_tog = []
for i in range(len(soups)):
    titles = soups[i].select("a h2.ui-search-item__title")[:]
    for title in titles:
        titles_tog.append(title.string)

#extract the prices
prices_tog = []
for i in range(len(soups)):
    prices = soups[i].select("div.ui-search-price.ui-search-price--size-medium div.ui-search-price__second-line span.price-tag.ui-search-price__part span.price-tag-amount span.price-tag-fraction")[:]
    for price in prices:
        if price.string == " ":
            prices_tog.append("NA")
        else:
            prices_tog.append(price.string)

#create dataframe
data = pd.DataFrame({"Titles":titles_tog,
                    "Prices":prices_tog})

#normalice data
data["Titles"] = data["Titles"].str.lower()
data["Name"] = data["Titles"].apply(lambda name: name.split(" ")[0])
data["N_titles"] = (data["Titles"].apply(lambda name: name.split(" ")[1:])
                                 .map(lambda names: ' '.join(names)))
data["N_prices"] = data["Prices"].apply(lambda price: price.split(".")).map(lambda prices: ''.join(prices))

#regex
data['rin']=data['Titles'].str.extract(r'rin\s(\d{2})')
data['year']=data['Titles'].str.extract(r'(\d{4})')

marcas = ["specialized","scott","trek",
    "giant","canyon","cannondale",
    "orbea","bmc","pinarello","bianchi",
    "gw","shimano", "suntour", "shaoyang", "vittoria",
    "oxford","colnago","campagnolo","diadora",
    "triestina","yeti","pinarello","ramón","roadmaster","xaurator","drais"
    "ramon"]

data['brand']=data['Titles'].apply(lambda x:x.split(" ")).map(lambda x:list(set(x).intersection(marcas)))
#revisar la intersección ya que descuadra el orden de las marcas
data['brand']=data['brand'].apply(lambda x: "sin marca" if(not x) else x[0])

data.to_json('./export.json', orient='columns')

