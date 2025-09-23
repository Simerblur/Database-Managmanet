import pandas as pd
from sqlalchemy import create_engine
from builders.genre_builder import create_genre_table


# =============================================================================
# STEP 1: METADATA LOOKUP FUNCTIONS
# =============================================================================

# --- TASK: Create a fast lookup dictionary for metadata ---
def create_metadata_lookup(meta_data):

    lookup = {}
    
    # We iterate through every row in the metadata to create keys for matching.
    for row_num, movie in meta_data.iterrows():
        title = movie.get('title_normalized')
        date = movie.get('RelDate')
        
        if pd.notna(title):
            # We attempt to extract the release year from the full date (e.g., "2011-06-01" becomes 2011).
            year = None
            if pd.notna(date):
                try:
                    year = int(str(date).split('-')[0])
                except:
                    pass
            
            # The lookup is created with two keys:
            # 1. Title + Year: This is the most specific key for precise matching.
            if year:
                lookup[(title, year)] = row_num  
            # 2. Title-only: This acts as a backup in case the release year data is missing or mismatched.
            lookup[title] = row_num  
    
    return lookup


# --- TASK: Find the best matching metadata for a sales record ---
def find_metadata_match(sales_movie, lookup):

    
    title = sales_movie.get('title_normalized')
    year = sales_movie.get('year')
    
    # We must have a normalized title to attempt a match.
    if not pd.notna(title):
        return None
    
    # First, we try the most exact match: Normalized Title + Release Year.
    if pd.notna(year):
        exact_key = (title, int(year))
        if exact_key in lookup:
            return lookup[exact_key]
    
    # If the exact match fails, we fall back to a less strict Title-only match.
    return lookup.get(title)


# =============================================================================
# STEP 2: DATA COMBINATION AND GENRE ID MAPPING
# =============================================================================

# --- TASK: Combine data from both sources into a single movie record ---
def combine_data(sales_movie, meta_row, meta_data, genre_lookup, movie_id):
    
    # We start the record by taking all non-financial data from the Sales table.
    movie = {
        'movie_id': movie_id,
        'title': sales_movie.get('title'),
        'title_normalized': sales_movie.get('title_normalized'),
        'runtime': sales_movie.get('runtime'),
        'release_year': sales_movie.get('year'),
        'release_date': sales_movie.get('release_date'),
        'metacritic_url': sales_movie.get('url'),
    }
    
    # If a matching metadata record was found, we update the movie record with additional details.
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
        # The genre must be handled by combining data from both sources.
        movie['genre_ids'] = get_genre_ids(
            sales_movie.get('genre'), 
            metadata.get('genre'), 
            genre_lookup
        )
    else:
        # If no metadata was found, we fill in all fields with 'None' to ensure the database schema remains consistent.
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
        # Genres are created using the Sales data only.
        movie['genre_ids'] = get_genre_ids(
            sales_movie.get('genre'), 
            None, 
            genre_lookup
        )
    
    return movie


# --- TASK: Convert genre names to numerical IDs ---
def get_genre_ids(sales_genre, meta_genre, genre_lookup):
    
    all_genres = set()
    
    # We check both the Sales and Metadata sources to ensure all genres are captured.
    for genre_text in [sales_genre, meta_genre]:
        if pd.notna(genre_text) and genre_text.strip():
            # Logic to handle different separators (comma or slash) often found in the raw data.
            if ',' in genre_text:
                genres = [g.strip() for g in genre_text.split(',')]
            elif '/' in genre_text:
                genres = [g.strip() for g in genre_text.split('/')]
            else:
                genres = [genre_text.strip()]
            # We use a Set to automatically remove any duplicate genres captured from the two different sources.
            all_genres.update(genres)
    
    # We look up the numerical ID for each unique genre name.
    ids = [genre_lookup[g] for g in all_genres if g in genre_lookup]
    # The final result is a comma-separated string of IDs.
    return ','.join(map(str, ids)) if ids else None


# =============================================================================
# STEP 3: DATABASE ORCHESTRATION
# =============================================================================

# --- TASK: Orchestrate the entire movie database creation process ---
def build_movie_database(sales_data, meta_data, connection):
        
    print("Building movie database from sales + metadata...")
    
    # Step 1: Create the Genre lookup table first, as other tables depend on its IDs.
    print("Creating genre table...")
    genre_table = create_genre_table(sales_data, meta_data, connection)
    # The lookup dictionary allows us to quickly find a Genre ID given a Genre Name.
    genre_lookup = dict(zip(genre_table['Name'], genre_table['GenreId']))
    
    # Step 2: Create a fast lookup for all metadata for efficient matching.
    print("Creating metadata lookup...")
    meta_lookup = create_metadata_lookup(meta_data)
    
    # Step 3: Iterate through all Sales records to process and match each movie.
    print("Processing and matching movies...")
    final_movies = []
    # This set is used to prevent the creation of duplicate movie records.
    processed_titles = set()  
    # Start the unique numerical IDs from 1.
    movie_id_counter = 1  
    
    for i, sales_movie in sales_data.iterrows():        
        # Skip any movie records that lack a title.
        if not pd.notna(sales_movie.get('title')):
            continue
        
        # Create a key for simple duplicate checking (normalized title + year).
        title_year_key = f"{sales_movie.get('title_normalized')}_{sales_movie.get('year')}"
        
        # If this key has already been processed, we skip the record.
        if title_year_key in processed_titles:
            continue
        processed_titles.add(title_year_key)
        
        # This numerical ID is the primary key that links to all other tables (Box Office, Reviews).
        movie_id = movie_id_counter
        
        # Find the matching metadata record and combine all the data fields.
        meta_row = find_metadata_match(sales_movie, meta_lookup)
        complete_movie = combine_data(sales_movie, meta_row, meta_data, genre_lookup, movie_id)
        final_movies.append(complete_movie)
        
        movie_id_counter += 1  # Increments the ID for the next unique movie.
    
    # Step 4: Save the final compiled table to the database.
    print(f"Saving {len(final_movies)} movies to database...")
    movies_df = pd.DataFrame(final_movies)
    engine = create_engine(connection)
    
    # The final table is saved as 'movie', replacing any existing table to ensure a clean build.
    movies_df.to_sql('movie', engine, if_exists='replace', index=False)
    
    print(f"✓ Movie IDs range from 1 to {len(final_movies)}")
    
    return movies_df


# --- TASK: Main entry point for the movie table builder ---
def create_movie_table(sales_df, meta_df, connection_string):
    return build_movie_database(sales_df, meta_df, connection_string)