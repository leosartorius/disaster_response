# import libraries
import pandas as pd
from sqlalchemy import create_engine

def load_data(messages_filepath, categories_filepath):
    # load messages dataset
    messages = pd.read_csv('../data/messages.csv');
    # load categories dataset
    categories = pd.read_csv('../data/categories.csv')
    # merge datasets
    df = pd.merge(messages,categories,how='left',on='id')
    # create a dataframe of the 36 individual category columns
    categories = categories['categories'].str.split(';',expand=True)
    # select the first row of the categories dataframe and use this row to extract
    # a list of new column names for categories.
    row = categories.iloc[0]
    category_colnames = row.apply(lambda x: x.split('-')[0])
    # rename the columns of `categories`
    categories.columns = category_colnames
    # change content of the columns
    for column in categories:
        # set each value to be the string after the dash "-"
        categories[column] = categories[column].apply(lambda x: x.split('-')[1]).astype(int)
    # drop the original categories column from `df`
    df.drop(['categories'],axis=1,inplace=True)
    # concatenate the original dataframe with the new `categories` dataframe
    df=pd.concat([df,categories],axis=1)
    return(df)

def clean_data(df):
    # drop duplicates
    df.drop_duplicates(inplace=True)
    # drop nan
    df.dropna(axis=0,inplace=True)
    return(df)

def save_data(df, database_filename):
    engine = create_engine('sqlite:///'+database_filename)
    df.to_sql('message', engine, index=False)
    return(True)  

def run_etl(messages_filepath, categories_filepath,database_filename):
    df = load_data(messages_filepath, categories_filepath)
    df = clean_data(df)
    save_data(df,database_filename)
    return(True)

run_etl('../data/messages.csv','../data/categories.csv','../data/db.db')
