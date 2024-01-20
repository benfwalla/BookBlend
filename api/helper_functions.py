import json
import os
import warnings
import re

import pandas as pd
from bs4 import BeautifulSoup
import requests
from dotenv import load_dotenv
from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader

# Load environment variables and set pandas options
load_dotenv()
warnings.filterwarnings('ignore')
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', None)

# Set constants for environment variables
HARDCOVER_BEARER_TOKEN = os.getenv('HARDCOVER_BEARER_TOKEN')
BOOKBLEND_API_KEY = os.getenv("BOOKBLEND_API_KEY")

# API key header setup
api_key_header = APIKeyHeader(name="X-API-Key")

def get_api_key(api_key_header: str = Security(api_key_header)) -> str:
    if api_key_header == BOOKBLEND_API_KEY:
        return api_key_header
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="401: Invalid API Key",
    )

def format_date(date_str):
    if pd.isna(date_str) or 'not set' in date_str:
        return pd.NA
    parts = date_str.split()
    if len(parts) == 2:  # Format is 'Month Year'
        return f"{parts[0]} 1, {parts[1]}"  # Inserting default day
    return date_str

def format_and_convert_date(series, date_pattern):
    series = series.str.extract(date_pattern)[0]
    series = series.replace('not set', pd.NA)
    series = series.apply(lambda x: f"{x.split()[0]} 1, {x.split()[1]}" if pd.notna(x) and len(x.split()) == 2 else x)
    return pd.to_datetime(series, errors='coerce')

def get_goodreads_user_books_by_page(user_id, page_num=1):
    url = f'https://www.goodreads.com/review/list/{user_id}?page={page_num}'
    
    # Read the html table
    goodreads = pd.read_html(url, attrs={'id': 'books'}, extract_links='body', displayed_only=False)

    # Process the DataFrame
    user_books = goodreads[0]

    user_books = user_books[['title', 'author', 'pages', 'rating', 'ratings', 'pub', 'rating.1', 'votes', 'started', 'read']]
    user_books['goodreads_id'] = user_books['title'].apply(lambda x: x[1]).str.extract(r'(\d+)')
    
    for column in user_books.columns[:-1]:
        user_books[column] = user_books[column].apply(lambda x: x[0])

    user_books['title'] = user_books['title'].apply(lambda x: x.replace('title ', '', 1)).str.strip()
    user_books['author'] = user_books['author'].apply(lambda x: x.replace('author ', '', 1)).apply(lambda x: x.replace(' *', '', 1)).str.strip()
    user_books['pages'] = pd.to_numeric(user_books['pages'].str.extract(r'(\d+)')[0], errors='coerce')
    user_books['rating'] = pd.to_numeric(user_books['rating'].str.extract(r'(\d+\.\d+)')[0], errors='coerce')
    user_books['ratings'] = pd.to_numeric(user_books['ratings'].str.replace(',', '').str.extract(r'(\d+)')[0], errors='coerce')
    
    user_books.rename(columns={'rating': 'avg_goodreads_rating', 'ratings': 'total_goodreads_ratings'}, inplace=True)

    # I just want "pub" to be the year. But, it can get crazy with a bunch of different date formats
    user_books['pub'] = user_books['pub'].apply(lambda x: x.replace('date pub ', '', 1))
    user_books['pub'] = pd.to_numeric(user_books['pub'].str.extract(r'(?:\b\d{1,2},\s)?(\d{1,4})\b')[0], errors='coerce')

    # 'rating.1' is the rating the user gave the book... in text form (i.e. "did not like it" is a 1)
    user_books.rename(columns={'rating.1': 'user_rating'}, inplace=True)
    user_books['user_rating'] = user_books['user_rating'].str.extract(r'(.+?)\'s rating\s*(.*?)$')[1].str.strip()
    rating_mapping = {
        'did not like it': 1,
        'it was ok': 2,
        'liked it': 3,
        'really liked it': 4,
        'it was amazing': 5
    }
    user_books['user_rating'] = user_books['user_rating'].map(rating_mapping)

    # 'started' is actually the date read. I know, it's weird
    user_books.rename(columns={'started': 'date_read'}, inplace=True)
    user_books['date_read'] = format_and_convert_date(user_books['date_read'], r'date read\s*(.*)')

    # 'read' is actual the date added.
    user_books.rename(columns={'read': 'date_added'}, inplace=True)
    user_books['date_added'] = format_and_convert_date(user_books['date_added'], r'date added\s*(.*)')
    
    # So, the "votes" column is weird. It actually has a "# times read  x" value, which I am using to get the x value, then convert to a boolean
    user_books.rename(columns={'votes': 'read?'}, inplace=True)
    user_books['read?'] = pd.to_numeric(user_books['read?'].str.extract(r'(\d+)')[0], errors='coerce')
    user_books['read?'] = user_books['read?'] > 0

    return user_books

def get_all_goodreads_user_books(user_id):

    page_num = 1
    all_books_df = pd.DataFrame()

    while True:
        print(f'Fetching {user_id}\'s Page {page_num}...')
        books_on_page = get_goodreads_user_books_by_page(user_id, page_num)
        if books_on_page.empty:
            print(f'Page {page_num} is empty.')
            break
        all_books_df = pd.concat([all_books_df, books_on_page], ignore_index=True)
        page_num += 1

    return all_books_df

def get_genres_from_hardcover(goodreads_ids):
    url = "https://hardcover-production.hasura.app/v1/graphql"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {HARDCOVER_BEARER_TOKEN}'
    }
    
    # Convert the Series or list of IDs to the required string format
    ids_string = ', '.join(f'"{id_}"' for id_ in goodreads_ids)

    # Construct the GraphQL query
    query = f"""
    query GetBookByGoodreadsIDs {{
      book_mappings(
        where: {{platform: {{id: {{_eq: 1}}}}, external_id: {{_in: [{ids_string}]}}}}
      ) {{
        external_id
        book {{
          taggings {{
            tag {{
              tag
            }}
          }}
        }}
      }}
    }}
    """

    payload = json.dumps({"query": query, "variables": {}})
    response = requests.post(url, headers=headers, data=payload).json()

    books_json = response['data']['book_mappings']
    flattened_data = []

    # Iterate through each book entry in the JSON
    for entry in books_json:
        book_id = entry['external_id']
        
        # Flatten the taggings into a single string separated by commas
        tags = [tag['tag']['tag'] for tag in entry['book']['taggings']]
        
        # Append the flattened data to the list
        flattened_data.append({'external_id': book_id, 'tags': tags})

    genres_df = pd.DataFrame(flattened_data)

    return genres_df

def combine_goodreads_and_hardcover(goodreads_df, hardcover_df):
    return pd.merge(goodreads_df, hardcover_df, left_on='goodreads_id', right_on='external_id', how='left')

def get_user_info(user_id):
    url = f"https://www.goodreads.com/user/show/{user_id}"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    try:
        canonical_link = soup.find('link', {'rel': 'canonical'})['href']
        user_id = canonical_link.split('/')[-1].split('-')[0]
    except TypeError:
        user_id = ''

    try:
        title = soup.find('title').text
        books_shelved_match = re.search(r'(\d{1,3}(?:,\d{3})*|\d+)\s+books', title)
        books_shelved = books_shelved_match.group(1).replace(',', '')
    except AttributeError:
        books_shelved = ''
      
    try:
        books_read_match = re.search(r'read\s*\(.*?(\d{1,3}(?:,\d{3})*|\d+)\)', soup.text)
        books_read = books_read_match.group(1).replace(',', '')
    except AttributeError:
        books_read = ''

    try:
        currently_reading_count_match = re.search(r'currently-reading&lrm;\s*\((\d{1,3}(?:,\d{3})*|\d+)\)', soup.text)
        currently_reading_count = currently_reading_count_match.group(1).replace(',', '')
    except AttributeError:
        currently_reading_count = ''

    try:
        to_read_count_match = re.search(r'to-read&lrm;\s*\((\d{1,3}(?:,\d{3})*|\d+)\)', soup.text)
        to_read_count = to_read_count_match.group(1).replace(',', '')
    except AttributeError:
        to_read_count = ''

    try:
        full_name = soup.find('meta', {'property': 'og:title'})['content']
    except TypeError:
        full_name = ''

    try:
        first_name = soup.find('meta', {'property': 'profile:first_name'})['content']
    except TypeError:
        first_name = ''

    try:
        last_name = soup.find('meta', {'property': 'profile:last_name'})['content']
    except TypeError:
        last_name = ''

    try:
        username = soup.find('meta', {'property': 'profile:username'})['content']
    except TypeError:
        username = ''

    try:
        friends_match = re.search(r" Friends \((\d+)\)", soup.text)
        friends = friends_match.group(1)
    except AttributeError:
        friends = ''

    return {
        'user_id': user_id,
        'full_name': full_name,
        'first_name': first_name,
        'last_name': last_name,
        'username': username,
        'books_shelved': books_shelved,
        'number_of_friends': friends,
        'books_read': books_read,
        'currently_reading_count': currently_reading_count,
        'to_read_count': to_read_count
    }

# get_goodreads_user_books_by_page(42944663, 1)