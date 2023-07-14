import os
import zipfile
import requests
import datetime
import logging
import uuid
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Numeric
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from airflow.decorators import dag, task

'''
================
Constants
================
'''
URL="https://opendata.com.pk/dataset/a6a52e2b-c209-4f9f-8b99-eb67ef33d04e/resource/7395e1d0-c02b-4e1d-abb8-84ae52681ffb/download/archive.zip"
RAW_DATA_DIR= "store_raw_data"
CURRENT_CSV=current_date = os.path.join(RAW_DATA_DIR, f'{ datetime.datetime.now().strftime("%Y-%m-%d")}.csv')
DIR_BASE='/content/'
DATA_MODELING_DIR='data_modeling'
DATABASE_URL = 'sqlite:///database.db'
CHUNK_SIZE = 1000

log_file = "logs.txt"
logging.basicConfig(filename=log_file, level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

Base = declarative_base()
'''
================
Classes & Functions
================
'''
class OrdersFact(Base):
    __tablename__ = 'orders_fact'
    order_id = Column(String(255), primary_key=True)
    item_id = Column(String(255), ForeignKey('items_dim.item_id'))
    customer_id = Column(String(255), ForeignKey('customer_dim.customer_id'))
    date_id = Column(Integer, ForeignKey('datetime_dim.date_id'))
    discount_id = Column(String(255), ForeignKey('discount_dim.discount_id'))
    status = Column(String(255))
    price = Column(Numeric)
    qty_ordered = Column(Integer)
    grand_total = Column(Numeric)
    payment_method = Column(String(255))

class ItemsDim(Base):
    __tablename__ = 'items_dim'
    item_id = Column(String(255), primary_key=True)
    sku = Column(String(255))
    category_name = Column(String(255))

class CustomerDim(Base):
    __tablename__ = 'customer_dim'
    customer_id = Column(String(255), primary_key=True)
    country = Column(String(255))
    acquisitions = relationship('CustomerAcquisitionFact', back_populates='customer')

class CustomerAcquisitionFact(Base):
    __tablename__ = 'customer_acquisition_fact'
    acq_id = Column(String(255), primary_key=True)
    customer_id = Column(String(255), ForeignKey('customer_dim.customer_id'))
    date_id = Column(Integer, ForeignKey('datetime_dim.date_id'))
    customer = relationship('CustomerDim', back_populates='acquisitions')

class DiscountDim(Base):
    __tablename__ = 'discount_dim'
    discount_id = Column(String(255), primary_key=True)
    sales_commission_code = Column(String(255))
    discount_amount = Column(Numeric)

class DateTimeDim(Base):
    __tablename__ = 'datetime_dim'
    date_id = Column(Integer, primary_key=True)
    init_date=Column(String(255))
    year = Column(Integer)
    month = Column(Integer)
    day = Column(Integer)
    quarter = Column(String(255))
    dayofweek = Column(Integer)
    weekofyear = Column(Integer)
    dayofyear = Column(Integer)
    isweekend = Column(String(255))


def download_extract_data(url):
    # Create folder if it doesn't exist
    folder_path = RAW_DATA_DIR
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    
    
    try:
        # Download the zip file
        response = requests.get(url)
        response.raise_for_status()

        # Extract the zip contents
        zip_='archive.zip'
        open(zip_, 'wb').write(response.content)
        with zipfile.ZipFile(zip_) as zip_ref:
            zip_ref.extractall(folder_path)
        
        # Find the extracted CSV file
        csv_file = ""
        for file_ in os.listdir(folder_path):
            if file_.endswith(".csv"):
                csv_file = file_
                break
        
        #remove temporary file
        os.remove(zip_)

        if csv_file:
            # Rename the CSV file to the current date
            new_csv_file = CURRENT_CSV
            os.rename(os.path.join(folder_path, csv_file), new_csv_file)
            logging.info(f"CSV file renamed to {current_date}.csv")
        else:
            logging.error("No CSV file found in the extracted contents.")
    except Exception as e:
        logging.exception(f"Error occurred: {str(e)}")


def transform_split_data(FILE_URL):
  def __clean_data(df):
    df.dropna(how = "all")

    #filter columns
    filter_cols=['item_id', 'status', 'created_at', 'sku', 'price', 'qty_ordered',
       'grand_total', 'category_name_1',
       'sales_commission_code', 'discount_amount', 'payment_method', 'Customer Since', 'Customer ID']

    df=df[filter_cols]
    #rename columns
    df.columns=['item_id', 'status', 'created_at', 'sku', 'price', 'qty_ordered',
       'grand_total', 'category_name',
       'sales_commission_code', 'discount_amount', 'payment_method', 'customer_acqusition_date', 'customer_id']

    df['created_at'] = pd.to_datetime(df['created_at'])

    for col in ['price', 'qty_ordered', 'grand_total','discount_amount']:
      if df[col].dtype!=float and df[col].dtype!=int:
        logging.exception(f"The data type of column '{col}' is '{df[col].dtype}'(Wrong data type).")
        raise TypeError(f"The data type of column '{col}' is '{df[col].dtype}'(Wrong data type).")

    return df
  
  def __create_datetime_dim(df):
    start_date = df['created_at'].min()
    end_date = df['created_at'].max()

    # Create a date range using pandas date_range function
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')

    # Create a DataFrame to hold the date dimension data
    date_dim = pd.DataFrame(date_range, columns=['init_date'])

    date_dim['year'] = date_dim['init_date'].dt.year
    date_dim['month'] = date_dim['init_date'].dt.month
    date_dim['day'] = date_dim['init_date'].dt.day
    date_dim['quarter'] = date_dim['init_date'].dt.quarter
    date_dim['dayofweek'] = date_dim['init_date'].dt.dayofweek
    date_dim['weekofyear'] = date_dim['init_date'].dt.weekofyear
    date_dim['dayofyear'] = date_dim['init_date'].dt.dayofyear
    date_dim['isweekend'] = date_dim['dayofweek'].isin([5, 6])

    date_dim['date_id'] = range(1, len(date_dim) + 1)

    
    date_dim['init_date'] = date_dim['init_date'].dt.strftime('%Y-%m-%d')
    df['created_at'] = df['created_at'].dt.strftime('%Y-%m-%d')

    mapping_dict = date_dim.set_index('init_date')['date_id'].to_dict()
    df['date_id'] = df['created_at'].map(mapping_dict).astype('str')
    df['date_id'] = pd.to_numeric(df['date_id'], errors='coerce')
    df = df.dropna(subset=['date_id'])
    df['date_id']=df['date_id'].astype(int)

    df=df.drop('created_at',axis=1)
    
    date_dim.to_csv(f'{DATA_MODELING_DIR}/datetime_dim.csv',index=False)
    return df
  
  def __create_discount_dim(df):
    unique_values = df['sales_commission_code'].drop_duplicates()

    result_df = df[df['sales_commission_code'].isin(unique_values)][['sales_commission_code', 'discount_amount']].drop_duplicates(subset=['sales_commission_code', 'discount_amount'])
    
    result_df['discount_id']=[f'dis_{uuid.uuid4()}' for _ in range(len(result_df))]
    
    mapping_dict = result_df.set_index('sales_commission_code')['discount_id'].to_dict()
    df['discount_id'] = df['sales_commission_code'].map(mapping_dict)
    df=df.drop(['sales_commission_code', 'discount_amount'],axis=1)

    result_df.to_csv(f'{DATA_MODELING_DIR}/discount_dim.csv',index=False)

    return df
  
  def __create_acquistion_dim(df):
    unique_values = df['customer_id'].drop_duplicates()
    result_df = df[df['customer_id'].isin(unique_values)].drop_duplicates(subset=['customer_id'])[['customer_id', 'customer_acqusition_date']]
    
    date_dim=pd.read_csv(f'{DATA_MODELING_DIR}/datetime_dim.csv')
    mapping_dict = date_dim.set_index('init_date')['date_id'].to_dict()
    def convert_date_string(date_str):
      if not isinstance(date_str, str):
        return np.NaN
      date_obj = datetime.datetime.strptime(date_str, "%Y-%m")
      formatted_date_str = datetime.datetime.strftime(date_obj, "%Y-%m-%d")

      return formatted_date_str

    result_df['customer_acqusition_date']=result_df['customer_acqusition_date'].apply(convert_date_string)
    result_df['date_id'] = result_df['customer_acqusition_date'].map(mapping_dict).astype('str')
    
    result_df=result_df.drop(['customer_acqusition_date'],axis=1)

    result_df['acq_id']=[f'acq_{uuid.uuid4()}' for _ in range(len(result_df))]
    result_df.to_csv(f'{DATA_MODELING_DIR}/customer_acqusition_fact.csv',index=False)

    return df

  def __create_customer_dim(df):
    result_df = df['customer_id'].drop_duplicates()
    result_df=result_df.dropna()
    result_df['country']='Pakistan'
    result_df.to_csv(f'{DATA_MODELING_DIR}/customer_dim.csv',index=False)

    return df
  
  def __create_items_dim(df):
    unique_values = df['sku'].drop_duplicates()
    result_df = df[df['sku'].isin(unique_values)].drop_duplicates(subset=['sku'])[['sku', 'category_name']]
    result_df['item_id']=[f'item_{uuid.uuid4()}' for _ in range(len(result_df))]
    
    mapping_dict = result_df.set_index('sku')['item_id'].to_dict()
    df['item_id'] = result_df['sku'].map(mapping_dict).astype('str')

    result_df.to_csv(f'{DATA_MODELING_DIR}/items_dim.csv',index=False)

    return df

  
  def __create_orders_dim(df):
    filter_col=['item_id', 'status', 'price', 'qty_ordered', 'grand_total', 'payment_method',
       'customer_id', 'date_id', 'discount_id']
    df=df[filter_col]

    df['order_id']=[f'ord_{uuid.uuid4()}' for _ in range(len(df))]
    df.to_csv(f'{DATA_MODELING_DIR}/orders_fact.csv',index=False)

    return df
  
  #transform dataframe
  try:
    ecommerce_data=pd.read_csv(FILE_URL)

    #clean data
    ecommerce_data=__clean_data(ecommerce_data)

    if not os.path.exists(DATA_MODELING_DIR):
        os.makedirs(DATA_MODELING_DIR)

    ecommerce_data=__create_datetime_dim(ecommerce_data)

    ecommerce_data=__create_discount_dim(ecommerce_data)

    ecommerce_data=__create_acquistion_dim(ecommerce_data)

    ecommerce_data=__create_customer_dim(ecommerce_data)

    ecommerce_data=__create_items_dim(ecommerce_data)

    ecommerce_data=__create_orders_dim(ecommerce_data)


    return ecommerce_data
  

  except Exception as e:
    logging.exception(f"Error occurred: {str(e)}")

def load_data_to_db():
  def __upload_data(df, table_name):
      # Create engine and session
      if not os.path.exists(DATABASE_URL):
        engine = create_engine(DATABASE_URL)
        Base.metadata.create_all(engine)
      engine = create_engine(DATABASE_URL)
      Session = sessionmaker(bind=engine)
      session = Session()

      try:
          # Chunked uploading
          for i in range(0, len(df), CHUNK_SIZE):
              chunk = df[i:i + CHUNK_SIZE]
              chunk.to_sql(table_name, engine, if_exists='append', index=False)

          session.commit()
          logging.info("Data upload successful!")

      except SQLAlchemyError as e:
          session.rollback()
          logging.exception(f"Error occurred: {str(e)}")
      finally:
          session.close()
  
  for file_ in os.listdir(DATA_MODELING_DIR):
    if file_.endswith('.csv'):
      logging.info(f'Uploading {file_}')
      df=pd.read_csv(f'{DATA_MODELING_DIR}/{file_}')
      __upload_data(df, file_.replace('.csv',''))


'''
================
ETL Logic
================
'''
# Define the DAG
@dag(
    'etl_pipeline',
    description='Author, schedule and monitor data modeling workflows with Airflow',
    schedule=None,  # Set the desired schedule interval
    start_date=datetime.datetime(2023, 7, 1),  # Set the start date
    catchup=False  # Skip catching up past execution dates
)
def taskflow_api():
   @task()
   def extract():
      download_extract_data(URL)
   
   @task()
   def transform():
      transform_split_data(f'{CURRENT_CSV}')
   
   @task()
   def load():
      load_data_to_db()
    
   extract()
   transform()
   load()


taskflow_api()
   

