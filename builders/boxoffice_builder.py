import pandas as pd
from sqlalchemy import create_engine

def create_movie_lookup_from_sales(sales_df):
    """
    Create a lookup dictionary that maps (title_normalized, year) to sequential movie_id
    This MUST match the exact same logic as movie_builder.py
    """
    movie_lookup = {}
    processed_titles = set()
    movie_id_counter = 1
    
    for _, row in sales_df.iterrows():
        # Skip movies without titles (same logic as movie_builder)
        if not pd.notna(row.get('title')):
            continue
        
        # Create duplicate check using normalized title + year (same as movie_builder)
        title_year_key = f"{row.get('title_normalized')}_{row.get('year')}"
        
        if title_year_key in processed_titles:
            continue
        processed_titles.add(title_year_key)
        
        # Map this title+year combination to a numerical ID
        movie_lookup[title_year_key] = movie_id_counter
        movie_id_counter += 1
    
    return movie_lookup

def create_box_office_performance_table(sales_df, connection_string):
    """
    Creates box office performance table with numerical movie IDs that match movie table
    """
    
    print("Creating box office performance table with numerical IDs...")
    
    # Step 1: Create the same movie ID lookup as movie_builder will use
    movie_lookup = create_movie_lookup_from_sales(sales_df)
    
    # Step 2: Clean the financial data
    box_office_data = []
    performance_id = 1
    
    for _, row in sales_df.iterrows():
        # Skip movies without titles
        if not pd.notna(row.get('title')):
            continue
        
        # Create the same duplicate check
        title_year_key = f"{row.get('title_normalized')}_{row.get('year')}"
        
        if title_year_key not in movie_lookup:
            continue  # Skip duplicates
        
        # Get the numerical movie ID
        movie_id = movie_lookup[title_year_key]
        
        # Create box office record
        box_office_record = {
            'performance_id': performance_id,
            'movie_id': movie_id,  # ← Numerical ID that matches movie table
            'worldwide_box_office': row.get('worldwide_box_office', 0) or 0,
            'domestic_box_office': row.get('domestic_box_office', 0) or 0,
            'international_box_office': row.get('international_box_office', 0) or 0,
            'production_budget': row.get('production_budget', 0) or 0,
            'opening_weekend': row.get('opening_weekend', 0) or 0,
            'theatre_count': row.get('theatre_count', 0) or 0
        }
        
        box_office_data.append(box_office_record)
        performance_id += 1
    
    # Step 3: Create DataFrame and save to database
    box_office_performance = pd.DataFrame(box_office_data)
    
    engine = create_engine(connection_string)
    box_office_performance.to_sql('box_office_performance', engine, if_exists='replace', index=False)
    
    print(f"✓ Created box_office_performance table with {len(box_office_performance)} records")
    print(f"  Movie IDs range from 1 to {max(box_office_performance['movie_id'])}")
    
    return box_office_performance