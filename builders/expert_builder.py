import pandas as pd
from sqlalchemy import create_engine

# =============================================================================
# FUNCTION 1: CREATE EXPERTS TABLE (EXPERT METADATA)
# =============================================================================
# --- TASK: Define function to build the main experts table ---
def create_expert_table(expert_reviews_df, connection_string):
    
    print("Building experts table...")
    
    # Initialize lists and counters
    experts_data = []
    expert_id_counter = 1
    
    # --- OPERATION: Identify all unique experts ---
    # We strip out any missing (NaN) reviewer names and find every unique reviewer in the dataset.
    unique_reviewers = expert_reviews_df['reviewer'].dropna().unique()
    
    # --- OPERATION: Loop through experts to calculate statistics ---
    for reviewer_name in unique_reviewers:
        # For each expert, we filter the full dataset to get only their reviews.
        expert_reviews = expert_reviews_df[expert_reviews_df['reviewer'] == reviewer_name]
        
        # We create a record containing summary statistics for the expert.
        # This includes the total number of reviews and the average metrics (score and word count).
        expert_record = {
            'ExpertId': expert_id_counter,
            'ReviewerName': reviewer_name,
            'TotalReviews': len(expert_reviews),
            # Calculate the average score, checking first if any scores are present to avoid errors.
            'AverageScore': expert_reviews['idvscore'].mean() if not expert_reviews['idvscore'].isna().all() else None,
            # Calculate the average word count, checking first if any counts are present.
            'AverageWordCount': expert_reviews['WC'].mean() if not expert_reviews['WC'].isna().all() else None
        }
        
        experts_data.append(expert_record)
        expert_id_counter += 1
    
    # --- OPERATION: Save final table to database ---
    # Convert the list of records into a Pandas DataFrame (our final table structure).
    experts_df = pd.DataFrame(experts_data)
    engine = create_engine(connection_string)
    # Save the DataFrame to the database under the table name 'experts'.
    experts_df.to_sql('experts', engine, if_exists='replace', index=False)
    
    print(f"✓ Created experts table with {len(experts_data)} unique experts")
    
    return experts_df


# =============================================================================
# FUNCTION 2: CREATE EXPERT_REVIEWS TABLE (REVIEW DETAILS)
# =============================================================================
# --- TASK: Define function to build the detailed expert reviews table ---
def create_expert_reviews_table(expert_reviews_df, connection_string):
    
    print("Building expertReviews table...")
    
    # --- OPERATION: Map reviewer names to numerical IDs ---
    # This step is critical: we need a way to link the detailed review records to the 'experts' table created previously, using the numerical ExpertId (a foreign key).
    unique_reviewers = expert_reviews_df['reviewer'].dropna().unique()
    reviewer_to_id = {}
    # We assign a sequential ID (starting from 1) to each unique reviewer name.
    for i, reviewer_name in enumerate(unique_reviewers, 1):
        reviewer_to_id[reviewer_name] = i
    
    # Initialize lists and counters for the new review records.
    reviews_data = []
    review_id = 1
    
    # --- OPERATION: Process every review and enrich data ---
    # We iterate through every single review record in the cleaned source data.
    for _, review in expert_reviews_df.iterrows():
        reviewer_name = review.get('reviewer')
        
        # Use the mapping dictionary to find the corresponding numerical **ExpertId** for the reviewer's name. If the name is missing, we use None.
        expert_id = reviewer_to_id.get(reviewer_name) if pd.notna(reviewer_name) else None
        
        # Create a detailed review record.
        review_record = {
            'ReviewId': review_id,
            'ExpertId': expert_id, # This is the numerical link (foreign key) to the 'experts' table.
            'MovieUrl': review.get('url'),
            'ReviewScore': review.get('idvscore'),
            'ReviewDate': review.get('dateP'),
            'ReviewText': review.get('Rev'),
            
            # --- LIWC Analysis Fields ---
            # These columns represent the LIWC (Linguistic Inquiry and Word Count) results, providing psychological and linguistic insights into the text.
            
            # Basic text analysis
            'WordCount': review.get('WC'),
            'WordsPerSentence': review.get('WPS'),
            
            # LIWC Summary scores (main psychological measures)
            'Analytical': review.get('Analytic'),
            'Clout': review.get('Clout'),
            'Authentic': review.get('Authentic'),
            'Tone': review.get('Tone'),
            
            # Key language dimensions
            'FunctionWords': review.get('function'),
            'Pronouns': review.get('pronoun'),
            'PersonalPronouns': review.get('ppron'),
            'Verbs': review.get('verb'),
            'Adjectives': review.get('adj'),
            
            # Psychological processes - Emotions
            'PositiveEmotion': review.get('posemo'),
            'NegativeEmotion': review.get('negemo'),
            'Anxiety': review.get('anx'),
            'Anger': review.get('anger'),
            'Sadness': review.get('sad'),
            
            # Cognitive processes
            'CognitiveProcesses': review.get('cogproc'),
            'Insight': review.get('insight'),
            'Causation': review.get('cause'),
            'Certainty': review.get('certain'),
            'Tentative': review.get('tentat'),
            
            # Time focus
            'PastFocus': review.get('focuspast'),
            'PresentFocus': review.get('focuspresent'),
            'FutureFocus': review.get('focusfuture'),
            
            # Social processes
            'Social': review.get('social'),
            'Family': review.get('family'),
            'Friends': review.get('friend'),
            
            # Personal concerns
            'Work': review.get('work'),
            'Leisure': review.get('leisure'),
            'Money': review.get('money'),
            'Religion': review.get('relig'),
            
            # Informal language
            'InformalLanguage': review.get('informal'),
            'SwearWords': review.get('swear'),
            'Netspeak': review.get('netspeak')
        }
        
        reviews_data.append(review_record)
        review_id += 1
    
    # --- OPERATION: Save final table to database ---
    # Convert the list of structured review records into a Pandas DataFrame.
    expert_reviews_table = pd.DataFrame(reviews_data)
    engine = create_engine(connection_string)
    # Save the DataFrame to the database under the table name 'expert_reviews'.
    expert_reviews_table.to_sql('expert_reviews', engine, if_exists='replace', index=False)
    
    print(f"✓ Created expert_reviews table with {len(reviews_data)} reviews")
    
    return expert_reviews_table