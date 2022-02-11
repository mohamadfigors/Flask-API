import os

import pandas as pd
from pypika import Table, Query
from flask import Flask, request
from sqlalchemy import create_engine

app = Flask(__name__)

conn_format = "postgresql://{user}:{pass}@{host}:{port}/{db}"
conn_string = conn_format.format(**{
    "user": os.environ.get("POSTGRES_USER"),
    "pass": os.environ.get("POSTGRES_PASS"),
    "host": os.environ.get("POSTGRES_HOST"),
    "port": os.environ.get("POSTGRES_PORT"),
    "db": os.environ.get("POSTGRES_DB"),
})

conn = create_engine(conn_string)

################
## endpoint 1 ##
################

@app.route("/supermarket/sales/list/branch", methods=['GET'])
def request_sales_branchs():
    query = "SELECT DISTINCT branch FROM supermarket_sales"
    dataframe = pd.read_sql(query, conn)
    result = dataframe['branch']
    result = list(result)
    return {"data": result}

@app.route("/supermarket/sales/list/payment", methods=['GET'])
def request_sales_payments():
    query = "SELECT DISTINCT payment FROM supermarket_sales"
    dataframe = pd.read_sql(query, conn)
    result = dataframe['payment']
    result = list(result)
    return {"data": result}

################
## endpoint 2 ##
################

@app.route("/supermarket/sales/date", methods=["GET"])
def request_sales_date():
    param_date = request.args.get("date")
    
    table = Table("supermarket_sales")
    query = Query.from_(table).select("*")

    if param_date:
        query = query.where(table.date == param_date)

    dataframe = pd.read_sql(str(query), conn)
    result = dataframe.to_dict(orient="records")
    return {"data": result}


################
## endpoint 3 ##
################

@app.route("/supermarket/sales/summary/branch", methods=["GET"])
def request_summary_branch():
    query = """
    SELECT branch, sum(quantity*unit_price) AS total_sales
    FROM supermarket_sales
    GROUP BY branch
    """
    dataframe = pd.read_sql(query, conn)
    result = dataframe.to_dict(orient='records')
    result = list(result)
    return {"data": result}

@app.route("/supermarket/sales/summary/payment", methods=["GET"])
def request_summary_payment():
    query = """
    SELECT payment, sum(quantity*unit_price) AS total_sales
    FROM supermarket_sales
    GROUP BY payment
    """
    dataframe = pd.read_sql(query, conn)
    result = dataframe.to_dict(orient="records")
    return {"data": result}

@app.route("/supermarket/sales/summary/gender", methods=["GET"])
def request_summary_gender():
    query = """
    SELECT gender, sum(quantity*unit_price) AS total_sales
    FROM supermarket_sales
    GROUP BY gender
    """
    dataframe = pd.read_sql(query, conn)
    result = dataframe.to_dict(orient="records")
    return {"data": result}

if __name__ == "__main__":
    app.run()