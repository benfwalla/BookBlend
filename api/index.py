from fastapi import FastAPI, Response, Security
from api.helper_functions import (combine_goodreads_and_hardcover, 
                                  get_all_goodreads_user_books, 
                                  get_api_key, 
                                  get_genres_from_hardcover,
                                  get_user_info)

app = FastAPI()

@app.get("/api/python")
def api_hello_world(api_key: str = Security(get_api_key)):
    return {"message": "Hello World"}

@app.get("/api/books/{user_id}")
def api_get_user_books(user_id, api_key: str = Security(get_api_key)):
    # Ben Walace's Goodreads user ID is 42944663
    # Lisa Tsinis's Goodreads user ID is 48799880
    # Jackie Law's Goodreads user ID is 10154028
    print(f"Collecting Goodreads Books for {user_id}...")
    goodreads_df = get_all_goodreads_user_books(user_id)
    goodreads_ids = goodreads_df['goodreads_id']

    print(f"Collecting Hardcover genres for {user_id}'s books...")
    hardcover_df = get_genres_from_hardcover(goodreads_ids)

    print(f"Combining Goodreads DF and Hardcover DF for {user_id}...")
    combined_df = combine_goodreads_and_hardcover(goodreads_df, hardcover_df)
    books_json = combined_df.to_json(orient='records', date_format='iso', date_unit='s')

    return Response(books_json, media_type="application/json")

@app.get("/api/users/{user_id}")
def api_get_user_info(user_id, api_key: str = Security(get_api_key)):
    return get_user_info(user_id)