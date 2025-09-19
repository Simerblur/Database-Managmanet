import pandas as pd
from movie_builder import create_movie_table
from user_builder import create_user_table, create_user_reviews_table
from boxoffice_builder import create_box_office_performance_table

# Load data
sales_df = pd.read_csv('cleanedData/sales_cleaned.csv')
meta_df = pd.read_csv('cleanedData/metadata_cleaned.csv')
user_reviews_df = pd.read_csv('cleanedData/user_reviews_cleaned.csv', low_memory=False)

# Create movie table
connection_string = 'postgresql://admin@localhost:5432/moviedb'
movies_df = create_movie_table(sales_df, meta_df, connection_string)

# Create user tables
users_df = create_user_table(user_reviews_df, connection_string)
user_reviews_clean = create_user_reviews_table(user_reviews_df, connection_string)

# Create box office performance table
box_office_performance = create_box_office_performance_table(sales_df, connection_string)
