import pandas as pd
from sqlalchemy import create_engine

def create_genre_table(sales_df, meta_df, connection_string):
    
    print("Building genre table...")
    
    # Create an empty set to store all unique genres
    # Sets automatically remove duplicates for us
    unique_genres = set()
    
    # PART 1: Get genres from sales data
    # Loop through each movie in sales data
    for index, movie_row in sales_df.iterrows():
        
        # Get the genre column for this movie
        genre_text = movie_row.get('genre')
        
        # Only process if genre exists and isn't empty
        if pd.notna(genre_text) and genre_text.strip():
            
            # Some movies have multiple genres like "Action, Comedy"
            # We need to split them up
            if ',' in genre_text:
                # Split by comma and clean up spaces
                individual_genres = [g.strip() for g in genre_text.split(',')]
                # Add all genres to our set
                unique_genres.update(individual_genres)
            
            # Some movies use slash like "Action/Adventure"
            elif '/' in genre_text:
                # Split by slash and clean up spaces  
                individual_genres = [g.strip() for g in genre_text.split('/')]
                # Add all genres to our set
                unique_genres.update(individual_genres)
            
            # Single genre movies
            else:
                # Just add the single genre
                unique_genres.add(genre_text.strip())
    
    # PART 2: Get genres from metadata (same process)
    if 'genre' in meta_df.columns:
        for index, movie_row in meta_df.iterrows():
            
            genre_text = movie_row.get('genre')
            
            if pd.notna(genre_text) and genre_text.strip():
                
                # Handle comma-separated genres
                if ',' in genre_text:
                    individual_genres = [g.strip() for g in genre_text.split(',')]
                    unique_genres.update(individual_genres)
                
                # Handle slash-separated genres
                elif '/' in genre_text:
                    individual_genres = [g.strip() for g in genre_text.split('/')]
                    unique_genres.update(individual_genres)
                
                # Single genres
                else:
                    unique_genres.add(genre_text.strip())
    
    # Remove any empty strings that might have snuck in
    unique_genres = {genre for genre in unique_genres if genre}
    
    # PART 3: Create the final table
    # Make a list to hold our genre records
    genre_table_data = []
    
    # Give each genre a number (ID) starting from 1
    # Sort genres alphabetically so IDs are consistent
    for genre_number, genre_name in enumerate(sorted(unique_genres), start=1):
        
        # Create a record for each genre
        genre_record = {
            'GenreId': genre_number,    # The ID number
            'Name': genre_name          # The genre name
        }
        
        # Add this record to our table
        genre_table_data.append(genre_record)
    
    # PART 4: Save to database
    # Convert our list of records into a DataFrame (table)
    final_genre_table = pd.DataFrame(genre_table_data)
    
    # Connect to database and save our table
    database_connection = create_engine(connection_string)
    final_genre_table.to_sql('genre', database_connection, if_exists='replace', index=False)
    
    print(f"✓ Created genre table with {len(genre_table_data)} unique genres")
    
    # Return the table so other code can use it
    return final_genre_table


def get_genre_ids_for_movie(genre_text, genre_lookup_dict):
    
    # This function converts "Action, Comedy" into "1,3" (the ID numbers)
    
    # If no genre provided, return nothing
    if pd.isna(genre_text) or not genre_text.strip():
        return None
    
    # List to collect the ID numbers
    genre_id_list = []
    
    # Split the genres (same logic as before)
    if ',' in genre_text:
        # Handle comma-separated genres
        individual_genres = [g.strip() for g in genre_text.split(',')]
    elif '/' in genre_text:
        # Handle slash-separated genres  
        individual_genres = [g.strip() for g in genre_text.split('/')]
    else:
        # Single genre
        individual_genres = [genre_text.strip()]
    
    # Look up the ID number for each genre
    for genre_name in individual_genres:
        if genre_name in genre_lookup_dict:
            # Find the ID for this genre and add it to our list
            genre_id_list.append(genre_lookup_dict[genre_name])
    
    # Convert ID numbers to text format for database storage
    # Example: [1, 3, 5] becomes "1,3,5"
    if genre_id_list:
        return ','.join(map(str, genre_id_list))
    else:
        return None