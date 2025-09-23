import pandas as pd
from sqlalchemy import create_engine

# =============================================================================
# STEP 1: CREATE MOVIE ID LOOKUP KEY
# =============================================================================
# --- TASK: Define function to generate unique ID mapping ---
def create_movie_lookup_from_sales(sales_df):
    """
    This helper function creates a dictionary that assigns a unique, sequential number 
    (the **movie_id**) to each film. This process is crucial because the Movie table 
    and the Box Office table must use the exact same unique ID for every movie.
    """
    movie_lookup = {}
    processed_titles = set()
    movie_id_counter = 1
    
    # We iterate through every movie in the Sales data to assign an ID.
    for _, row in sales_df.iterrows():
        # First, we skip any records that are missing a title.
        if not pd.notna(row.get('title')):
            continue
        
        # We create a unique key by combining the **normalized title** and **release year**.
        # This ensures that different movies with the same title from different years are treated as unique.
        title_year_key = f"{row.get('title_normalized')}_{row.get('year')}"
        
        # This check prevents duplicate records (e.g., if the same movie title appears twice in the source data) from receiving a new ID.
        if title_year_key in processed_titles:
            continue
        processed_titles.add(title_year_key)
        
        # We map the unique key to the next available numerical ID.
        movie_lookup[title_year_key] = movie_id_counter
        movie_id_counter += 1
    
    return movie_lookup

# =============================================================================
# STEP 2: BUILD BOX OFFICE PERFORMANCE TABLE
# =============================================================================
# --- TASK: Main function to process financial data and save to database ---
def create_box_office_performance_table(sales_df, connection_string):
    """
    This function processes the raw financial data, uses the standardized movie IDs, 
    and saves the final table to the database.
    """
    
    print("Creating box office performance table with numerical IDs...")
    
    # We first generate the standardized movie IDs, as this must match the main Movie table creation process.
    movie_lookup = create_movie_lookup_from_sales(sales_df)
    
    # We initialize lists to hold the structured records.
    box_office_data = []
    performance_id = 1
    
    # We loop through the sales data again to extract the financial metrics.
    for _, row in sales_df.iterrows():
        # Skip movies without titles, maintaining consistency with the lookup logic.
        if not pd.notna(row.get('title')):
            continue
        
        # Recreate the unique key (normalized title + year) to retrieve the correct ID.
        title_year_key = f"{row.get('title_normalized')}_{row.get('year')}"
        
        # Skip the movie if its unique key isn't in our lookup, ensuring we only process non-duplicate records.
        if title_year_key not in movie_lookup:
            continue
        
        # Retrieve the standardized numerical movie ID.
        movie_id = movie_lookup[title_year_key]
        
        # Create a structured record containing all relevant box office and financial metrics.
        # We use 'or 0' to convert any missing or non-numeric values to zero, which is necessary for later calculations like ROI.
        box_office_record = {
            'performance_id': performance_id,
            'movie_id': movie_id,  # This is the standardized numerical ID.
            'worldwide_box_office': row.get('worldwide_box_office', 0) or 0,
            'domestic_box_office': row.get('domestic_box_office', 0) or 0,
            'international_box_office': row.get('international_box_office', 0) or 0,
            'production_budget': row.get('production_budget', 0) or 0,
            'opening_weekend': row.get('opening_weekend', 0) or 0,
            'theatre_count': row.get('theatre_count', 0) or 0
        }
        
        box_office_data.append(box_office_record)
        performance_id += 1
    
    # --- TASK: Convert data to DataFrame and save to database ---
    # The list of structured records is converted into a Pandas DataFrame (table).
    box_office_performance = pd.DataFrame(box_office_data)
    
    # We connect to the database and save the DataFrame under the table name 'box_office_performance'.
    engine = create_engine(connection_string)
    box_office_performance.to_sql('box_office_performance', engine, if_exists='replace', index=False)
    
    print(f"✓ Created box_office_performance table with {len(box_office_performance)} records")
    print(f"  Movie IDs range from 1 to {max(box_office_performance['movie_id'])}")
    
    return box_office_performance