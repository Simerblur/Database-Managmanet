import pandas as pd
from sqlalchemy import create_engine
from builders.genre_builder import create_genre_table


def create_metadata_lookup(meta_data):
    """
    Create a fast lookup dictionary for metadata
    """
    
    lookup = {}
    
    # Go through each movie in metadata and create lookup entries
    for row_num, movie in meta_data.iterrows():
        title = movie.get('title_normalized')
        date = movie.get('RelDate')
        
        if pd.notna(title):
            # Get year from date like "2011-06-01" 
            year = None
            if pd.notna(date):
                try:
                    year = int(str(date).split('-')[0])
                except:
                    pass
            
            # Create two entries: title+year and title-only
            if year:
                lookup[(title, year)] = row_num  # Most specific match
            lookup[title] = row_num  # Backup match
    
    return lookup


def find_metadata_match(sales_movie, lookup):
    """
    Find the best matching metadata for this sales movie
    """
    
    title = sales_movie.get('title_normalized')
    year = sales_movie.get('year')
    
    if not pd.notna(title):
        return None
    
    # Try exact title+year match first
    if pd.notna(year):
        exact_key = (title, int(year))
        if exact_key in lookup:
            return lookup[exact_key]
    
    # Try title-only match as backup
    return lookup.get(title)


def combine_data(sales_movie, meta_row, meta_data, genre_lookup, movie_id):
    """
    Combine sales and metadata into one complete movie record
    """
    
    # Start with sales data
    movie = {
        'movie_id': movie_id,
        'title': sales_movie.get('title'),
        'title_normalized': sales_movie.get('title_normalized'),
        'runtime': sales_movie.get('runtime'),
        'release_year': sales_movie.get('year'),
        'release_date': sales_movie.get('release_date'),
        'metacritic_url': sales_movie.get('url'),
    }
    
    # Add metadata if we found a match
    if meta_row is not None:
        metadata = meta_data.iloc[meta_row]
        movie.update({
            'director': metadata.get('director'),
            'studio': metadata.get('studio'),
            'rating': metadata.get('rating'),
            'critic_score': metadata.get('metascore'),
            'user_score': metadata.get('userscore'),
            'cast': metadata.get('cast'),
            'summary': metadata.get('summary'),
            'awards': metadata.get('awards'),
        })
        # Handle genres from both sources
        movie['genre_ids'] = get_genre_ids(
            sales_movie.get('genre'), 
            metadata.get('genre'), 
            genre_lookup
        )
    else:
        # No metadata found - use sales data only
        movie.update({
            'director': None, 
            'studio': None, 
            'rating': None,
            'critic_score': None, 
            'user_score': None,
            'cast': None,
            'summary': None,
            'awards': None,
        })
        # Handle genres from sales data only
        movie['genre_ids'] = get_genre_ids(
            sales_movie.get('genre'), 
            None, 
            genre_lookup
        )
    
    return movie


def get_genre_ids(sales_genre, meta_genre, genre_lookup):
    """
    Convert genre names to ID numbers like "Action, Comedy" -> "1,3"
    Combines genres from both sales and metadata
    """
    
    all_genres = set()
    
    # Process both genre sources
    for genre_text in [sales_genre, meta_genre]:
        if pd.notna(genre_text) and genre_text.strip():
            # Split multiple genres
            if ',' in genre_text:
                genres = [g.strip() for g in genre_text.split(',')]
            elif '/' in genre_text:
                genres = [g.strip() for g in genre_text.split('/')]
            else:
                genres = [genre_text.strip()]
            all_genres.update(genres)
    
    # Convert to IDs
    ids = [genre_lookup[g] for g in all_genres if g in genre_lookup]
    return ','.join(map(str, ids)) if ids else None


def build_movie_database(sales_data, meta_data, connection):
    """
    Build movie database combining sales and metadata with numerical IDs
    """
        
    print("Building movie database from sales + metadata...")
    
    # Step 1: Create genre table from both sources
    print("Creating genre table...")
    genre_table = create_genre_table(sales_data, meta_data, connection)
    genre_lookup = dict(zip(genre_table['Name'], genre_table['GenreId']))
    
    # Step 2: Create lookup for fast metadata matching
    print("Creating metadata lookup...")
    meta_lookup = create_metadata_lookup(meta_data)
    
    # Step 3: Process all movies with numerical IDs
    print("Processing and matching movies...")
    final_movies = []
    processed_titles = set()  # Simple duplicate tracking
    movie_id_counter = 1  # Start numerical IDs from 1
    
    for i, sales_movie in sales_data.iterrows():        
        # Skip movies without titles
        if not pd.notna(sales_movie.get('title')):
            continue
        
        # Create simple duplicate check using normalized title + year
        title_year_key = f"{sales_movie.get('title_normalized')}_{sales_movie.get('year')}"
        
        if title_year_key in processed_titles:
            continue
        processed_titles.add(title_year_key)
        
        # Use numerical movie ID (this will match box_office_performance table)
        movie_id = movie_id_counter
        
        # Find matching metadata and combine data
        meta_row = find_metadata_match(sales_movie, meta_lookup)
        complete_movie = combine_data(sales_movie, meta_row, meta_data, genre_lookup, movie_id)
        final_movies.append(complete_movie)
        
        movie_id_counter += 1  # Increment for next movie
    
    # Step 4: Save to database
    print(f"Saving {len(final_movies)} movies to database...")
    movies_df = pd.DataFrame(final_movies)
    engine = create_engine(connection)
    
    # Save the data with numerical movie_id
    movies_df.to_sql('movie', engine, if_exists='replace', index=False)
    
    print(f"✓ Movie IDs range from 1 to {len(final_movies)}")
    
    return movies_df


# Main entry point
def create_movie_table(sales_df, meta_df, connection_string):
    """
    Main entry point - combines sales and metadata
    """
    return build_movie_database(sales_df, meta_df, connection_string)