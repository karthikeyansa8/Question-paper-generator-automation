import psycopg2
import pandas as pd
from sqlalchemy import create_engine

import os
from dotenv import load_dotenv

load_dotenv()

host=os.getenv('host')
db_dev=os.getenv('db_dev')
db=os.getenv('db')
username=os.getenv('db_username')
password=os.getenv('password')
port=os.getenv('port')

# conn = psycopg2.connect(
#    database=db , user=username, password= password, host=host , port= port
# )
# #Creating a cursor object using the cursor() method
# engine = create_engine(f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{db}")



def processQuery(query: str) -> pd.DataFrame:
    conn = psycopg2.connect(
   database=db , user=username, password= password, host=host , port= port
)
    """returns the query as pandas dataframe from database

    Args:
    --------
        query (str): query
    
    Returns:
    ---------
        data: pandas dataframe from query
    """
    table = pd.read_sql(query, con=conn)
    conn.close()
    return table
    

def excute_query(query:str,args={}):
    try:
        conn,cursor = create_connection()

        cursor.execute(query=query,vars=args)
    except Exception as e:
        print(e)
        raise Exception(e)
    finally:
        conn.commit()
        conn.close()

def excute_query_without_commit(cursor,query,arguments={}):
    try:
        cursor.execute(query=query,vars=arguments)
    except Exception as e:
        raise Exception(e)
    
def create_connection():
    conn = psycopg2.connect(
    database=db , user=username, password= password, host=host , port= port
    )
    cursor = conn.cursor()   

    return conn,cursor

def excute_query_and_return_result(query:str,arguments=[]):
    try:
        conn,cursor = create_connection()

        cursor.execute(query=query,vars=arguments)
        return cursor.fetchall()
    except Exception as e:
        print(e)
        raise Exception(e)
    finally:
        conn.commit()
        conn.close()



