# Author: Li Lin 
# =============================================================================

import pandas as pd
from sqlalchemy import create_engine

# =============================================================================
# FUNCTION 1: CREATE USERS TABLE (USER METADATA)
# =============================================================================
# --- TASK: Define function to build the main users table ---
def create_user_table(user_reviews_df, connection_string):
    """
    This function processes the raw user review data to create the 'users' table, 
    which contains a single, unique record for every reviewer.
    """
    
    # --- OPERATION: Identify and standardize unique users ---
    # We find all unique reviewer names in the raw data. 
    # Any missing names (NaN) are standardized and grouped under the name 'Anonymous'.
    unique_reviewers = user_reviews_df['reviewer'].fillna('Anonymous').unique()
    
    # --- OPERATION: Create structured DataFrame ---
    # We create a new table structure that assigns a sequential, numerical **user_id** # to each unique reviewer name, starting from 1. This ID is the primary key.
    users_df = pd.DataFrame({
        'user_id': range(1, len(unique_reviewers) + 1),
        'reviewer': unique_reviewers
    })
    
    # --- OPERATION: Save final table to database ---
    # Establish the database connection.
    engine = create_engine(connection_string)
    # Save the DataFrame to the database under the table name 'users'.
    users_df.to_sql('users', engine, if_exists='replace', index=False)
    
    print(f"Created users table with {len(users_df)} users")
    
    return users_df

# =============================================================================
# FUNCTION 2: CREATE USER_REVIEWS TABLE (REVIEW DETAILS)
# =============================================================================
# --- TASK: Define function to build the detailed review table ---
def create_user_reviews_table(user_reviews_df, connection_string):
    """
    This function processes every single review, links it to a user ID, 
    cleans the numerical metrics, and saves the final review table.
    """
    
    # --- OPERATION: Create ID mapping for foreign keys ---
    # We repeat the process of finding unique reviewers and assign them a numerical ID.
    unique_reviewers = user_reviews_df['reviewer'].fillna('Anonymous').unique()
    # This dictionary (user_map) is the key we use to translate the reviewer's name into the correct numerical ID.
    user_map = dict(zip(unique_reviewers, range(1, len(unique_reviewers) + 1)))
    
    # --- OPERATION: Map user IDs to every review ---
    # We create the **user_id** column on the full review set by looking up the name of the reviewer for each record.
    user_reviews_df['user_id'] = user_reviews_df['reviewer'].fillna('Anonymous').map(user_map)
    
    # --- OPERATION: Clean and standardize numerical columns ---
    # Financial and count columns are often messy in raw data. We perform the following steps for cleaning:
    # 1. pd.to_numeric: Force the column to be a number, converting any non-numeric text (like 'N/A') into NaN.
    # 2. fillna(0): Replace the resulting NaN values with zero.
    # 3. astype(int): Ensure the final column is stored as a whole number (integer).
    thumbs_up_clean = pd.to_numeric(user_reviews_df['thumbsUp'], errors='coerce').fillna(0).astype(int)
    thumbs_tot_clean = pd.to_numeric(user_reviews_df['thumbsTot'], errors='coerce').fillna(0).astype(int)
    word_count_clean = pd.to_numeric(user_reviews_df['WC'], errors='coerce').fillna(0).astype(int)
    
    # --- OPERATION: Select and structure final columns ---
    # We compile the final structured DataFrame with the cleaned metrics and the necessary foreign key.
    user_reviews_clean = pd.DataFrame({
        'user_review_id': range(1, len(user_reviews_df) + 1),
        'review_text': user_reviews_df['Rev'].fillna(''),
        'review_score': user_reviews_df['idvscore'],
        'thumbs_up': thumbs_up_clean,
        'total_score': thumbs_tot_clean,
        'word_count': word_count_clean,
        'emotional_tone': user_reviews_df['Tone'],
        'user_id': user_reviews_df['user_id'] # This is the numerical link (foreign key) to the 'users' table.
        
    })
    
    # --- OPERATION: Save final table to database ---
    # Establish the database connection.
    engine = create_engine(connection_string)
    # Save the DataFrame to the database under the table name 'user_reviews'.
    user_reviews_clean.to_sql('user_reviews', engine, if_exists='replace', index=False)
    
    print(f"Created user_reviews table with {len(user_reviews_clean)} reviews")
    
    return user_reviews_clean