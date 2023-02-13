from bs4 import BeautifulSoup as BS
import requests

def get_tags(html):
    soup = BS(html)
    return soup.find_all()

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36'}
r = requests.get('https://greenatom.ru', headers=headers)
tags = get_tags(r.text)
num_of_attr_tags = 0
for tag in tags:
    if tag.attrs:
        num_of_attr_tags += 1
print(f'Всего тегов(включая закрывающие и открывающие): {len(tags)}')
print(f'Количество тегов с атрибутами: {num_of_attr_tags}')