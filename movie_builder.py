import pandas as pd
from sqlalchemy import create_engine
from genre_builder import create_genre_table


def make_movie_id(title, year, date=None):
    # Create unique ID like "2021-spiderman" 
    # Handle missing data
    title = title if pd.notna(title) else "unknown"
    year = int(year) if pd.notna(year) else 0
    
    # Try to get month/day from date
    month = day = "01"
    if pd.notna(date):
        try:
            from dateutil import parser
            parsed = parser.parse(str(date))
            month, day = f"{parsed.month:02d}", f"{parsed.day:02d}"
        except:
            pass  # Use defaults if date parsing fails
    
    # Return formatted ID
    return f"{year:04d}{month}{day}-{title}"


def create_movie_lookup(review_data):
    # Create a fast lookup dictionary for movies
    # Like a phone book: give me title+year, get back the row number
    
    lookup = {}
    
    # Go through each movie and create lookup entries
    for row_num, movie in review_data.iterrows():
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


def find_review_match(sales_movie, lookup):
    # Find the best matching review for this sales movie
    
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


def combine_data(sales_movie, review_row, review_data, genres):
    # Combine sales and review data into one complete movie record
    
    # Start with sales data
    movie = {
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
        'production_budget': sales_movie.get('production_budget'),
        'worldwide_box_office': sales_movie.get('worldwide_box_office'),
        'domestic_box_office': sales_movie.get('domestic_box_office'),
        'international_box_office': sales_movie.get('international_box_office'),
    }
    
    # Add review data if we found a match
    if review_row is not None:
        review = review_data.iloc[review_row]
        movie.update({
            'director': review.get('director'),
            'studio': review.get('studio'),
            'rating': review.get('rating'),
            'critic_score': review.get('metascore'),
            'user_score': review.get('userscore'),
        })
    else:
        # No review data found
        movie.update({
            'director': None, 'studio': None, 'rating': None,
            'critic_score': None, 'user_score': None,
        })
    
    # Handle genres from both sources
    movie['genre_ids'] = get_genre_ids(
        sales_movie.get('genre'), 
        review.get('genre') if review_row else None, 
        genres
    )
    
    return movie


def get_genre_ids(sales_genre, review_genre, genre_lookup):
    # Convert genre names to ID numbers like "Action, Comedy" -> "1,3"
    
    all_genres = set()
    
    # Process both genre sources
    for genre_text in [sales_genre, review_genre]:
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


def build_movie_database(sales_data, review_data, connection):
    # Main function: combines everything into one database
        
    # Step 1: Create genre table
    genre_table = create_genre_table(sales_data, review_data, connection)
    genre_lookup = dict(zip(genre_table['Name'], genre_table['GenreId']))
    
    # Step 2: Create lookup for fast matching
    lookup = create_movie_lookup(review_data)
    
    # Step 3: Process all movies
    final_movies = []
    processed_ids = set()  # Track duplicates
    
    for i, sales_movie in sales_data.iterrows():        
        # Skip movies without titles
        if not pd.notna(sales_movie.get('title')):
            continue
        
        # Create unique ID and skip duplicates
        movie_id = make_movie_id(
            sales_movie.get('title_normalized'),
            sales_movie.get('year'),
            sales_movie.get('release_date')
        )
        
        if movie_id in processed_ids:
            continue
        processed_ids.add(movie_id)
        
        # Find matching review and combine data
        review_row = find_review_match(sales_movie, lookup)
        complete_movie = combine_data(sales_movie, review_row, review_data, genre_lookup)
        final_movies.append(complete_movie)
    
    # Step 4: Save to database
    movies_df = pd.DataFrame(final_movies)
    engine = create_engine(connection)
    movies_df.to_sql('movie', engine, if_exists='replace', index=False)
    
    
    return movies_df


# Main entry point
def create_movie_table(sales_df, meta_df, connection_string):
    # Keep same interface as original code
    return build_movie_database(sales_df, meta_df, connection_string)