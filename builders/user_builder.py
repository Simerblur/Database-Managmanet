import pandas as pd
from sqlalchemy import create_engine

def create_user_table(user_reviews_df, connection_string):
    # Get unique users
    unique_reviewers = user_reviews_df['reviewer'].fillna('Anonymous').unique()
    
    users_df = pd.DataFrame({
        'user_id': range(1, len(unique_reviewers) + 1),
        'reviewer': unique_reviewers
    })
    
    # Convert to DataFrame and save to PostgreSQL
    engine = create_engine(connection_string)
    users_df.to_sql('users', engine, if_exists='replace', index=False)
    
    print(f"Created users table with {len(users_df)} users")
    
    return users_df

def create_user_reviews_table(user_reviews_df, connection_string):    
    # Create user mapping for foreign keys
    unique_reviewers = user_reviews_df['reviewer'].fillna('Anonymous').unique()
    user_map = dict(zip(unique_reviewers, range(1, len(unique_reviewers) + 1)))
    
    # Map user_ids
    user_reviews_df['user_id'] = user_reviews_df['reviewer'].fillna('Anonymous').map(user_map)
    
    # Clean numeric columns - convert non-numeric to NaN first
    thumbs_up_clean = pd.to_numeric(user_reviews_df['thumbsUp'], errors='coerce').fillna(0).astype(int)
    thumbs_tot_clean = pd.to_numeric(user_reviews_df['thumbsTot'], errors='coerce').fillna(0).astype(int)
    word_count_clean = pd.to_numeric(user_reviews_df['WC'], errors='coerce').fillna(0).astype(int)
    
    # Select and clean columns
    user_reviews_clean = pd.DataFrame({
        'user_review_id': range(1, len(user_reviews_df) + 1),
        'review_text': user_reviews_df['Rev'].fillna(''),
        'review_score': user_reviews_df['idvscore'],
        'thumbs_up': thumbs_up_clean,
        'total_score': thumbs_tot_clean,
        'word_count': word_count_clean,
        'emotional_tone': user_reviews_df['Tone'],
        'user_id': user_reviews_df['user_id']
        
    })
    
    # Convert to DataFrame and save to PostgreSQL
    engine = create_engine(connection_string)
    user_reviews_clean.to_sql('user_reviews', engine, if_exists='replace', index=False)
    
    print(f"Created user_reviews table with {len(user_reviews_clean)} reviews")
    
    return user_reviews_clean