import re
import sys
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import numpy as np
#import random


today = datetime.today()
seed = int(f'{today.year}{today.month}{today.day}')

with open('/usr/local/mjs/restaurant_name.dat', 'r') as f:
    lunch_list = [line.strip() for line in f.readlines()]

with open('/usr/local/mjs/cafe_name.dat', 'r') as g:
    cafe_list = [line.strip() for line in g.readlines()]

lunch_list = np.array(lunch_list, dtype=object)

argv = sys.argv

try:
    num = int(argv[1])
except IndexError:
    num = 3

np.random.seed(seed)
selection = np.random.choice(lunch_list, num, replace=False)
selection_cafe = np.random.choice(cafe_list, 1, replace=False)

emoji1 = "\U0001F371"
emoji2 = "\u2615"
emoji3 = "\U0001F3EB"

print('---------------------------------------------------------')
print(f'{emoji1}오늘의 점심 추천 (어은동){emoji1}')
for i, key in enumerate(selection, 1):
    print (f"{i} 순위 : {key}")

print('---------------------------------------------------------')
print(f"{emoji2}오늘의 카페: {selection_cafe[0]}{emoji2}")



url = 'https://www.kaist.ac.kr/kr/html/campus/053001.html?dvs_cd=west'

# request web & parsing
response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')

table = soup.find('table', class_='table')

tds = table.find('tbody').find('tr').find_all('td')

# extract menu from table
def extract_menu(td_tag):
    
    raw_items = [item.strip() for item in td_tag.decode_contents().split('<br/>') if item.strip()]
    
    cleaned_items = []
    for item in raw_items:
        text = BeautifulSoup(item, 'html.parser').text
        cleaned = re.sub(r'\d+(\.\d+)*', '', text).strip()
        if not cleaned:
            continue
        cleaned_items.append(cleaned)
    return cleaned_items

lunch_menu = extract_menu(tds[1])
dinner_menu = extract_menu(tds[2])

# lunch
print('---------------------------------------------------------')
print(f"{emoji3} 서측학식{emoji3}")
for item in lunch_menu:
    print("-", item)

print('---------------------------------------------------------')

