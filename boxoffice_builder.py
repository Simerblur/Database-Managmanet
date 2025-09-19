import pandas as pd
from sqlalchemy import create_engine

def create_box_office_performance_table(sales_df, connection_string):
    """
    Creates box office performance table from sales data
    Ready for future foreign key relationships with movies
    """
    
    # Clean the data (your existing logic - perfect!)
    worldwide_box_office = sales_df['worldwide_box_office'].fillna(0)
    domestic_box_office = sales_df['domestic_box_office'].fillna(0)
    international_box_office = sales_df['international_box_office'].fillna(0)
    production_budget = sales_df['production_budget'].fillna(0)
    opening_weekend = sales_df['opening_weekend'].fillna(0)
    theatre_count = sales_df['theatre_count'].fillna(0)
    
    # Create movie IDs using the same logic as your movie_builder
    # This will make connecting them super easy later!
    movie_ids = []
    for _, row in sales_df.iterrows():
        # Use same ID creation logic as your movie_builder
        title = str(row.get('title_normalized', 'unknown'))
        year = int(row.get('year', 0)) if pd.notna(row.get('year')) else 0
        
        # Simple ID format for now (you can enhance this later)
        movie_id = f"{year:04d}-{title}"
        movie_ids.append(movie_id)
    
    # Create box office performance data (your structure + movie_id)
    box_office_performance = pd.DataFrame({
        'performance_id': range(1, len(sales_df) + 1),
        'movie_id': movie_ids,  # ← This will make connections easy later!
        'worldwide_box_office': worldwide_box_office,
        'domestic_box_office': domestic_box_office,
        'international_box_office': international_box_office,
        'production_budget': production_budget,
        'opening_week': opening_weekend,
        'theatre_count': theatre_count
    })
    
    # Save to database (your existing logic - works great!)
    engine = create_engine(connection_string)
    box_office_performance.to_sql('box_office_performance', engine, if_exists='replace', index=False)
    
    print(f"Created box_office_performance table with {len(box_office_performance)} records")
    
    return box_office_performance


# Optional: Helper function to create movie IDs consistently across all builders
def create_movie_id_from_sales_row(row):
    """
    Helper function to create consistent movie IDs across all your builders
    Use this same logic in movie_builder.py for consistency
    """
    title = str(row.get('title_normalized', 'unknown'))
    year = int(row.get('year', 0)) if pd.notna(row.get('year')) else 0
    
    return f"{year:04d}-{title}"

    """
    Your original version - works perfectly as-is!
    The upgrade script will handle connections later
    """
    worldwide_box_office = sales_df['worldwide_box_office'].fillna(0)
    domestic_box_office = sales_df['domestic_box_office'].fillna(0)
    international_box_office = sales_df['international_box_office'].fillna(0)
    production_budget = sales_df['production_budget'].fillna(0)
    opening_weekend = sales_df['opening_weekend'].fillna(0)
    theatre_count = sales_df['theatre_count'].fillna(0)
    
    # Create box office performance data
    box_office_performance = pd.DataFrame({
        'performance_id': range(1, len(sales_df) + 1),
        # movie_id will be added by upgrade script later
        'worldwide_box_office': worldwide_box_office,
        'domestic_box_office': domestic_box_office,
        'international_box_office': international_box_office,
        'production_budget': production_budget,
        'opening_week': opening_weekend,
        'theatre_count': theatre_count
    })
    
    # Convert to DataFrame and save to database
    engine = create_engine(connection_string)
    box_office_performance.to_sql('box_office_performance', engine, if_exists='replace', index=False)
    
    print(f"Created box_office_performance table with {len(box_office_performance)} records")
    
    return box_office_performance