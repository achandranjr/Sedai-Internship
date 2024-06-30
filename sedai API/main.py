# backend.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import sql

# Database connection parameters
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "password"
DB_HOST = "localhost"
DB_PORT = "5432"

# Connect to the database
def get_db_connection():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

TABLE_NAME = "spotify"
# Create table if not exists
def create_table():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS spotify (
            track SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            description TEXT
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

app = FastAPI()

class Song(BaseModel):
    track: str
    album_name: str = None
    artist: str = None
    release_date: str = None
    isrc: str = None
    all_time_rank: str = None
    track_score: str = None
    spotify_streams: str = None
    spotify_playlist_count: str = None
    spotify_playlist_reach: str = None
    spotify_popularity: str = None
    youtube_views: str = None
    tiktok_posts: str = None
    tiktok_likes: str = None
    tiktok_views: str = None
    youtube_playlist_reach: str = None
    apple_music_playlist_count: str = None
    airplay_spins: str = None
    siriusxm_spins: str = None
    deezer_playlist_count: str = None
    deezer_playlist_reach: str = None
    amazon_playlist_count: str = None
    pandora_streams: str = None
    pandora_track_stations: str = None
    soundcloud_streams: str = None
    shazam_counts: str = None
    tidal_popularity: str = None
    explicit_track: str = "0"


@app.on_event("startup")
async def startup():
    create_table()

@app.get("/Songs")
async def get_Songs(Song_name: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Construct and execute the SELECT query
        select_query = sql.SQL("SELECT * FROM {} WHERE \"Track\" = %s").format(sql.Identifier(TABLE_NAME))
        cursor.execute(select_query, (Song_name,))
        
        # Fetch the row
        row = cursor.fetchone()
        
        if row is None:
            raise HTTPException(status_code=404, detail="Song not found")
        
        # Assuming the columns are in order: Track, Artist, ...
        return row  # Add other fields as needed
    
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    
    finally:
        cursor.close()
        conn.close()

@app.post("/Songs")
async def create_Song(Song: Song):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        "INSERT INTO spotify (name, description) VALUES (%s, %s) RETURNING *",
        (Song.name, Song.description)
    )
    new_Song = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return new_Song

@app.put("/Songs/{Song_name}")
async def update_Song(Song_name: str, Song: Song):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        "UPDATE spotify SET name = %s WHERE id = %s RETURNING *",
        (Song.name, Song_name)
    )
    updated_Song = cur.fetchone()
    if updated_Song is None:
        raise HTTPException(status_code=404, detail="Song not found")
    conn.commit()
    cur.close()
    conn.close()
    return updated_Song

@app.delete("/Songs/{Song_name}")
async def delete_item(Song_name: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Construct and execute the DELETE query
        delete_query = sql.SQL("DELETE FROM {} WHERE id = %s RETURNING *").format(sql.Identifier(TABLE_NAME))
        cursor.execute(delete_query, (Song_name,))
        
        # Fetch the deleted row
        deleted_row = cursor.fetchone()
        
        if deleted_row is None:
            raise HTTPException(status_code=404, detail="Item not found")
        
        # Commit the transaction
        conn.commit()
        
        return {"message": "Item deleted successfully", "deleted_item": deleted_row}
    
    except psycopg2.Error as e:
        # Rollback in case of error
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)