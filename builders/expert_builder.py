import pandas as pd
from sqlalchemy import create_engine


def create_expert_table(expert_reviews_df, connection_string):
    """
    Creates the experts table with unique reviewer information
    Returns: experts DataFrame
    """
    
    print("Building experts table...")
    
    experts_data = []
    expert_id_counter = 1
    
    # Find all unique reviewers
    unique_reviewers = expert_reviews_df['reviewer'].dropna().unique()
    
    for reviewer_name in unique_reviewers:
        # Get all reviews by this expert to calculate stats
        expert_reviews = expert_reviews_df[expert_reviews_df['reviewer'] == reviewer_name]
        
        expert_record = {
            'ExpertId': expert_id_counter,
            'ReviewerName': reviewer_name,
            'TotalReviews': len(expert_reviews),
            'AverageScore': expert_reviews['idvscore'].mean() if not expert_reviews['idvscore'].isna().all() else None,
            'AverageWordCount': expert_reviews['WC'].mean() if not expert_reviews['WC'].isna().all() else None
        }
        
        experts_data.append(expert_record)
        expert_id_counter += 1
    
    # Save to database
    experts_df = pd.DataFrame(experts_data)
    engine = create_engine(connection_string)
    experts_df.to_sql('experts', engine, if_exists='replace', index=False)
    
    print(f"✓ Created experts table with {len(experts_data)} unique experts")
    
    return experts_df


def create_expert_reviews_table(expert_reviews_df, connection_string):
    """
    Creates the expertReviews table with all review data and LIWC analysis
    Returns: expertReviews DataFrame
    """
    
    print("Building expertReviews table...")
    
    # First, we need the expert ID mapping
    # Create a quick lookup of reviewer name to expert ID
    unique_reviewers = expert_reviews_df['reviewer'].dropna().unique()
    reviewer_to_id = {}
    for i, reviewer_name in enumerate(unique_reviewers, 1):
        reviewer_to_id[reviewer_name] = i
    
    reviews_data = []
    review_id = 1
    
    # Process each review
    for _, review in expert_reviews_df.iterrows():
        reviewer_name = review.get('reviewer')
        
        # Get expert ID (use None if reviewer is missing)
        expert_id = reviewer_to_id.get(reviewer_name) if pd.notna(reviewer_name) else None
        
        # Create review record with all the LIWC data
        review_record = {
            'ReviewId': review_id,
            'ExpertId': expert_id,
            'MovieUrl': review.get('url'),
            'ReviewScore': review.get('idvscore'),
            'ReviewDate': review.get('dateP'),
            'ReviewText': review.get('Rev'),
            
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
    
    # Save to database
    expert_reviews_table = pd.DataFrame(reviews_data)
    engine = create_engine(connection_string)
    expert_reviews_table.to_sql('expert_reviews', engine, if_exists='replace', index=False)
    
    print(f"✓ Created expert_reviews table with {len(reviews_data)} reviews")
    
    return expert_reviews_table

