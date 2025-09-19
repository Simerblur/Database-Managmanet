"""
Simplified movie Table Builder
Combines movie sales data and review data into one organized database table
"""

import pandas as pd
from sqlalchemy import create_engine
from genre_builder import create_genre_table, get_genre_ids_for_movie


def make_movie_id(movie_title, movie_year, release_date=None):
    """
    Creates a unique ID for each movie
    Example: "2021-spiderman" or "20210315-spiderman" if date is available
    """
    # Handle missing data with simple checks
    if not movie_title or pd.isna(movie_title):
        movie_title = "unknown"
    
    if not movie_year or pd.isna(movie_year):
        movie_year = 0
    else:
        movie_year = int(movie_year)
    
    # Try to get month and day from release date
    month, day = "01", "01"
    if release_date and pd.notna(release_date):
        try:
            from dateutil import parser
            parsed_date = parser.parse(str(release_date), fuzzy=True)
            month = f"{parsed_date.month:02d}"
            day = f"{parsed_date.day:02d}"
        except:
            # If date parsing fails, just use defaults
            pass
    
    # Create the ID: YYYYMMDD-title
    return f"{movie_year:04d}{month}{day}-{movie_title}"


def parse_review_date(date_string):
    """
    Parse review date format: "2011-06-01"
    Returns: year, month, day as numbers
    """
    if not date_string or pd.isna(date_string):
        return None, None, None
    
    try:
        # Split date string like "2011-06-01"
        date_parts = str(date_string).strip().split('-')
        if len(date_parts) >= 3:
            year = int(date_parts[0])
            month = int(date_parts[1])
            day = int(date_parts[2])
            return year, month, day
    except:
        # If anything goes wrong, return None values
        pass
    
    return None, None, None


def create_movie_directory(review_data):
    """
    Creates a directory to quickly find movies by title and year
    Like a phone book but for movies!
    
    Returns: dictionary where keys are (title, year) and values are row indexes
    """
    print("Building movie directory for quick lookups...")
    
    movie_directory = {}
    
    # Go through each movie in the review data
    for row_index, movie in review_data.iterrows():
        movie_title = movie.get('title_normalized')
        release_date = movie.get('RelDate')
        
        if movie_title and pd.notna(movie_title):
            # Get the year from the release date
            year, _, _ = parse_review_date(release_date)
            
            # Create entry with both title and year (most specific)
            if year:
                directory_key = (movie_title, year)
                movie_directory[directory_key] = row_index
            
            # Also create entry with just title (backup option)
            if movie_title not in movie_directory:
                movie_directory[movie_title] = row_index
    
    title_year_combinations = len([key for key in movie_directory.keys() if isinstance(key, tuple)])
    print(f"✓ Created directory with {title_year_combinations} title+year combinations")
    
    return movie_directory


def find_matching_review(sales_movie, movie_directory):
    """
    Finds the best matching review data for a sales movie
    Tries to match by title+year first, then just title
    """
    sales_title = sales_movie.get('title_normalized')
    sales_year = sales_movie.get('year')
    
    if not sales_title or pd.isna(sales_title):
        return None
    
    # First try: exact title + year match
    if sales_year and pd.notna(sales_year):
        exact_match_key = (sales_title, int(sales_year))
        if exact_match_key in movie_directory:
            return movie_directory[exact_match_key]
    
    # Second try: just title match
    if sales_title in movie_directory:
        return movie_directory[sales_title]
    
    # No match found
    return None


def combine_movie_data(sales_movie, review_row_index, review_data, genre_lookup):
    """
    Combines sales data with review data for one movie
    Creates a complete movie record with all available information
    """
    # Start with sales data
    movie_record = {
        'movie_id': make_movie_id(
            sales_movie.get('title_normalized'),
            sales_movie.get('year'),
            sales_movie.get('release_date')
        ),
        'title': sales_movie.get('title'),
        'runtime': sales_movie.get('runtime'),
        'release_year': sales_movie.get('year'),
        'release_date': sales_movie.get('release_date'),
        'metacritic_url': sales_movie.get('url'),
        
        # Financial data
        'production_budget': sales_movie.get('production_budget'),
        'worldwide_box_office': sales_movie.get('worldwide_box_office'),
        'domestic_box_office': sales_movie.get('domestic_box_office'),
        'international_box_office': sales_movie.get('international_box_office'),
    }
    
    # Get genres from both sources
    sales_genre = sales_movie.get('genre')
    meta_genre = None
    
    # Add review data if we found a match
    if review_row_index is not None:
        review_movie = review_data.iloc[review_row_index]
        meta_genre = review_movie.get('genre')  # Get genre from metadata
        
        movie_record.update({
            'director': review_movie.get('director'),
            'studio': review_movie.get('studio'),
            'rating': review_movie.get('rating'),
            'critic_score': review_movie.get('metascore'),
            'user_score': review_movie.get('userscore'),
            'review_release_date': review_movie.get('RelDate')
        })
    else:
        # No review data found, set to None
        movie_record.update({
            'director': None,
            'studio': None,
            'rating': None,
            'critic_score': None,
            'user_score': None,
            'review_release_date': None
        })
    
    # Combine genres from both sources
    movie_record['genre_ids'] = combine_genres(sales_genre, meta_genre, genre_lookup)
    
    return movie_record

def build_movie_database(sales_data, review_data, database_connection):
    """
    Main function: Combines sales and review data into one organized database
    
    Steps:
    1. Create genre categories (Action, Comedy, etc.)
    2. Build movie directory for quick lookups
    3. Process each movie and combine its data
    4. Save to database
    5. Analyze results
    """
    print("Starting to build comprehensive movie database...")
    
    # Step 1: Create genre table and get genre lookup
    print("\nStep 1: Setting up movie genres...")
    genre_data = create_genre_table(sales_data, review_data, database_connection)
    genre_lookup = dict(zip(genre_data['Name'], genre_data['GenreId']))
    
    # Step 2: Create movie directory for fast lookups
    print("\nStep 2: Creating movie directory...")
    movie_directory = create_movie_directory(review_data)
    
    # Step 3: Process each movie
    print("\nStep 3: Processing movies...")
    all_movies = []
    processed_movie_ids = set()  # Track what we've already processed
    
    total_movies = len(sales_data)
    processed_count = 0
    
    for _, sales_movie in sales_data.iterrows():
        processed_count += 1
        
        # Show progress every 1000 movies
        if processed_count % 1000 == 0:
            print(f"  Processed {processed_count}/{total_movies} movies...")
        
        # Skip movies without titles
        if not sales_movie.get('title') or pd.isna(sales_movie.get('title')):
            continue
        
        # Create unique movie ID
        movie_id = make_movie_id(
            sales_movie.get('title_normalized'),
            sales_movie.get('year'),
            sales_movie.get('release_date')
        )
        
        # Skip if we already processed this exact movie
        if movie_id in processed_movie_ids:
            continue
        
        processed_movie_ids.add(movie_id)
        
        # Find matching review data
        review_row_index = find_matching_review(sales_movie, movie_directory)
        
        # Combine all data for this movie
        complete_movie = combine_movie_data(
            sales_movie, 
            review_row_index, 
            review_data, 
            genre_lookup
        )
        
        all_movies.append(complete_movie)
    
    # Step 4: Save to database
    print("\nStep 4: Saving to database...")
    movies_dataframe = pd.DataFrame(all_movies)
    database_engine = create_engine(database_connection)
    movies_dataframe.to_sql('movie', database_engine, if_exists='replace', index=False)
    
    # Step 5: Analyze and report results
    print("\nStep 5: Analysis complete!")
    analyze_results(movies_dataframe, genre_data)
    
    return movies_dataframe


def analyze_results(movies_df, genre_df):
    """
    Analyzes the final movie database and reports interesting statistics
    """
    print("="*50)
    print("DATABASE ANALYSIS RESULTS")
    print("="*50)
    
    total_movies = len(movies_df)
    total_genres = len(genre_df)
    
    print(f"✓ Successfully created database with {total_movies} unique movies")
    print(f"✓ Movies are categorized into {total_genres} different genres")
    
    # Check for movies with same title but different years (remakes, reboots, etc.)
    title_groups = movies_df.groupby('title').size()
    duplicate_titles = title_groups[title_groups > 1]
    
    if len(duplicate_titles) > 0:
        print(f"\n✓ Found {len(duplicate_titles)} titles with multiple versions:")
        print("  (This includes remakes, reboots, and franchises)")
        
        # Show some examples
        for title, count in list(duplicate_titles.items())[:10]:
            print(f"  • '{title}': {count} versions")
            
            # Show the different years for this title
            versions = movies_df[movies_df['title'] == title][['movie_id', 'release_year']].head(3)
            for _, version in versions.iterrows():
                print(f"    → {version['movie_id']} ({version['release_year']})")
    
    # Check genre distribution
    multi_genre_movies = movies_df[movies_df['genre_ids'].str.contains(',', na=False)]
    print(f"\n✓ {len(multi_genre_movies)} movies have multiple genres")
    
    # Check data completeness
    movies_with_reviews = movies_df[movies_df['director'].notna()]
    movies_with_budget = movies_df[movies_df['production_budget'].notna()]
    
    print(f"✓ {len(movies_with_reviews)} movies have review data ({len(movies_with_reviews)/total_movies*100:.1f}%)")
    print(f"✓ {len(movies_with_budget)} movies have budget data ({len(movies_with_budget)/total_movies*100:.1f}%)")
    
    print("\n" + "="*50)


def find_duplicate_titles(movies_df, show_top=15):
    """
    Helper function to analyze movies with the same title but different years
    Useful for finding remakes, reboots, and franchise movies
    """
    print("\nDETAILED DUPLICATE ANALYSIS")
    print("="*40)
    
    # Group movies by title
    title_groups = movies_df.groupby('title')
    
    duplicates = []
    for title, group in title_groups:
        if len(group) > 1:
            years = sorted(group['release_year'].dropna().unique())
            duplicates.append({
                'title': title,
                'number_of_versions': len(group),
                'years': years
            })
    
    # Sort by number of versions (most versions first)
    duplicates.sort(key=lambda x: x['number_of_versions'], reverse=True)
    
    print(f"Found {len(duplicates)} titles with multiple versions:")
    print(f"Showing top {show_top}:\n")
    
    for i, dup in enumerate(duplicates[:show_top], 1):
        years_str = ', '.join(map(str, dup['years']))
        print(f"{i:2d}. {dup['title']}")
        print(f"    {dup['number_of_versions']} versions: {years_str}")
        print()
    
    return duplicates

def combine_genres(sales_genre, meta_genre, genre_lookup):
    """
    Combines genres from both sales and metadata sources
    Returns: comma-separated string of genre IDs
    """
    all_genres = set()
    
    # Process sales genre
    if pd.notna(sales_genre) and sales_genre.strip():
        if ',' in sales_genre:
            sales_genres = [g.strip() for g in sales_genre.split(',')]
        elif '/' in sales_genre:
            sales_genres = [g.strip() for g in sales_genre.split('/')]
        else:
            sales_genres = [sales_genre.strip()]
        all_genres.update(sales_genres)
    
    # Process metadata genre
    if pd.notna(meta_genre) and meta_genre.strip():
        if ',' in meta_genre:
            meta_genres = [g.strip() for g in meta_genre.split(',')]
        elif '/' in meta_genre:
            meta_genres = [g.strip() for g in meta_genre.split('/')]
        else:
            meta_genres = [meta_genre.strip()]
        all_genres.update(meta_genres)
    
    # Convert to genre IDs
    genre_ids = []
    for genre in all_genres:
        if genre in genre_lookup:
            genre_ids.append(genre_lookup[genre])
    
    return ','.join(map(str, genre_ids)) if genre_ids else None


# Main function to use this module
def create_movie_table(sales_df, meta_df, connection_string):
    """
    Main entry point - keeps the same interface as the original code
    This allows the simplified version to work as a drop-in replacement
    """
    return build_movie_database(sales_df, meta_df, connection_string)