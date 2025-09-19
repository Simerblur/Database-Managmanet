"""
Genre Table Builder
Creates normalized genre table from movie data
"""

import pandas as pd
from sqlalchemy import create_engine


def create_genre_table(sales_df, meta_df, connection_string):
    """
    Creates a normalized genre table from sales and metadata
    Returns: genre DataFrame and genre lookup dictionary
    """
    
    print("Building genre table...")
    
    # STEP 1: Collect all unique genres from both datasets
    all_genres = set()
    
    # Get genres from sales data
    for _, movie in sales_df.iterrows():
        genre = movie.get('genre')
        if pd.notna(genre) and genre.strip():
            # Handle multiple genres (comma or slash separated)
            if ',' in genre:
                genres = [g.strip() for g in genre.split(',')]
                all_genres.update(genres)
            elif '/' in genre:
                genres = [g.strip() for g in genre.split('/')]
                all_genres.update(genres)
            else:
                all_genres.add(genre.strip())
    
    # Get genres from metadata (if available)
    if 'genre' in meta_df.columns:
        for _, movie in meta_df.iterrows():
            genre = movie.get('genre')
            if pd.notna(genre) and genre.strip():
                if ',' in genre:
                    genres = [g.strip() for g in genre.split(',')]
                    all_genres.update(genres)
                elif '/' in genre:
                    genres = [g.strip() for g in genre.split('/')]
                    all_genres.update(genres)
                else:
                    all_genres.add(genre.strip())
    
    # Remove empty strings
    all_genres = {genre for genre in all_genres if genre}
    
    # STEP 2: Create genre records with IDs
    genre_records = []
    
    for genre_id, genre_name in enumerate(sorted(all_genres), start=1):
        genre_records.append({
            'GenreId': genre_id,
            'Name': genre_name
        })
    
    # STEP 3: Save to database
    genre_df = pd.DataFrame(genre_records)
    engine = create_engine(connection_string)
    genre_df.to_sql('genre', engine, if_exists='replace', index=False)
    
    print(f"✓ Created genre table with {len(genre_records)} unique genres")
    
    return genre_df


def get_genre_ids_for_movie(genre_string, genre_lookup):
    """
    Helper function: converts genre string to list of genre IDs
    Example: "Action, Comedy" -> "1,3" or [1, 3]
    """
    
    if pd.isna(genre_string) or not genre_string.strip():
        return None
    
    genre_ids = []
    
    # Split by comma or slash
    if ',' in genre_string:
        genres = [g.strip() for g in genre_string.split(',')]
    elif '/' in genre_string:
        genres = [g.strip() for g in genre_string.split('/')]
    else:
        genres = [genre_string.strip()]
    
    # Look up each genre ID
    for genre in genres:
        if genre in genre_lookup:
            genre_ids.append(genre_lookup[genre])
    
    # Return as comma-separated string for database storage
    return ','.join(map(str, genre_ids)) if genre_ids else None