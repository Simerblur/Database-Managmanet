import pandas as pd
from sqlalchemy import create_engine

# =============================================================================
# STEP 1: CREATE THE GENRE TABLE (ID LOOKUP)
# =============================================================================
# --- TASK: Scan source data and create table of unique genres ---
def create_genre_table(sales_df, meta_df, connection_string):
    
    print("Building genre table...")
    
    # --- TASK: Initialize a container for unique genres ---
    # We use a **set** (a mathematical collection) because it automatically prevents and removes any duplicate genre names that we find during the scanning process.
    unique_genres = set()
    
    # =============================================================================
    # PART 1: EXTRACT GENRES FROM SALES DATA
    # =============================================================================
    # We iterate through every movie record in the Sales data file.
    for index, movie_row in sales_df.iterrows():
        
        # Get the genre column for this movie
        genre_text = movie_row.get('genre')
        
        # We only process the record if the genre text is available and not empty.
        if pd.notna(genre_text) and genre_text.strip():
            
            # --- LOGIC: Handle multiple genres separated by a comma ---
            # Checks for lists like "Action, Comedy" and splits them into individual names.
            if ',' in genre_text:
                # We split the string by the comma and clean up any extra whitespace around the names.
                individual_genres = [g.strip() for g in genre_text.split(',')]
                # All extracted genres are added to our set.
                unique_genres.update(individual_genres)
            
            # --- LOGIC: Handle multiple genres separated by a slash ---
            # Checks for formats like "Action/Adventure" that sometimes appear in the raw data.
            elif '/' in genre_text:
                # We split the string by the slash and clean up any extra whitespace.
                individual_genres = [g.strip() for g in genre_text.split('/')]
                # Add all genres to our set.
                unique_genres.update(individual_genres)
            
            # --- LOGIC: Handle single genre movies ---
            else:
                # If there's no separator, we just add the single genre name.
                unique_genres.add(genre_text.strip())
    
    # =============================================================================
    # PART 2: EXTRACT GENRES FROM METADATA (Same process as Part 1)
    # =============================================================================
    # We repeat the extraction process for the Metadata file to ensure we capture every possible genre name from all sources.
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
    
    # --- TASK: Final cleanup ---
    # Removes any empty strings that may have been collected during the process before creating the table.
    unique_genres = {genre for genre in unique_genres if genre}
    
    # =============================================================================
    # PART 3: CREATE THE FINAL GENRE TABLE STRUCTURE
    # =============================================================================
    # Initialize a list to hold the structured records (ID and Name) for our new table.
    genre_table_data = []
    
    # We sort the genres alphabetically and then assign a sequential **GenreId** (starting from 1) to ensure the IDs are consistent on every run.
    for genre_number, genre_name in enumerate(sorted(unique_genres), start=1):
        
        # Create a structured record for each genre.
        genre_record = {
            'GenreId': genre_number,    # The unique numerical ID.
            'Name': genre_name          # The genre name itself.
        }
        
        # Add this record to our list of table data.
        genre_table_data.append(genre_record)
    
    # =============================================================================
    # PART 4: SAVE THE TABLE TO THE DATABASE
    # =============================================================================
    # The list of structured records is converted into a Pandas DataFrame (our final table).
    final_genre_table = pd.DataFrame(genre_table_data)
    
    # Connect to the database and save the table. We use 'if_exists='replace'' to overwrite any old data.
    database_connection = create_engine(connection_string)
    final_genre_table.to_sql('genre', database_connection, if_exists='replace', index=False)
    
    print(f"✓ Created genre table with {len(genre_table_data)} unique genres")
    
    # We return the DataFrame so the Movie Builder can use it immediately to map IDs.
    return final_genre_table


# =============================================================================
# STEP 2: HELPER FUNCTION: CONVERT NAMES TO IDs
# =============================================================================
# --- TASK: Convert genre names to numerical IDs for a single movie ---
def get_genre_ids_for_movie(genre_text, genre_lookup_dict):
    
    # This is a helper function that translates a text string of genres (e.g., "Action, Comedy") 
    # into a string of numerical IDs (e.g., "1,3") using the full Genre table dictionary.
    
    # If no genre text is provided or if it's empty, we return nothing.
    if pd.isna(genre_text) or not genre_text.strip():
        return None
    
    # Initialize a list to store the numerical IDs as we find them.
    genre_id_list = []
    
    # --- LOGIC: Split the genre string by comma or slash ---
    # We apply the same splitting logic used in the table creation function to handle various formats.
    if ',' in genre_text:
        # Handle comma-separated genres.
        individual_genres = [g.strip() for g in genre_text.split(',')]
    elif '/' in genre_text:
        # Handle slash-separated genres.
        individual_genres = [g.strip() for g in genre_text.split('/')]
    else:
        # Handle a single genre name.
        individual_genres = [genre_text.strip()]
    
    # --- LOGIC: Look up and collect IDs ---
    # We check the numerical ID for each genre name in the provided dictionary.
    for genre_name in individual_genres:
        if genre_name in genre_lookup_dict:
            # When found, the numerical ID is added to our list.
            genre_id_list.append(genre_lookup_dict[genre_name])
    
    # --- FINAL STEP: Format for database storage ---
    # The final list of IDs (e.g., [1, 3, 5]) is converted into a single comma-separated string (e.g., "1,3,5") for storage in the Movie table.
    if genre_id_list:
        return ','.join(map(str, genre_id_list))
    else:
        # If no valid genres were found, return nothing.
        return None