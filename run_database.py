import pandas as pd
from builders.movie_builder import create_movie_table
from builders.user_builder import create_user_table, create_user_reviews_table
from builders.boxoffice_builder import create_box_office_performance_table
from builders.expert_builder import create_expert_table, create_expert_reviews_table

# Load data
sales_df = pd.read_csv('cleanedData/sales_cleaned.csv')
meta_df = pd.read_csv('cleanedData/metadata_cleaned.csv')
user_reviews_df = pd.read_csv('cleanedData/user_reviews_cleaned.csv', low_memory=False)
expert_reviews_df = pd.read_csv('cleanedData/expert_reviews_cleaned.csv', low_memory=False)

# Create movie table
connection_string = 'postgresql://admin@localhost:5432/moviedb'
movies_df = create_movie_table(sales_df, meta_df, connection_string)

# Create user tables
users_df = create_user_table(user_reviews_df, connection_string)
user_reviews_clean = create_user_reviews_table(user_reviews_df, connection_string)

#Create expert tables
experts_df = create_expert_table(expert_reviews_df, connection_string)
expert_reviews_clean = create_expert_reviews_table(expert_reviews_df, connection_string)

# Create box office performance table
box_office_performance = create_box_office_performance_table(sales_df, connection_string)
