import datetime
import os

import snowflake.connector
from snowflake.connector import DictCursor
from flask import Blueprint, request, abort, jsonify, make_response

from cryptography.hazmat.primitives import serialization

# Make the Snowflake connection

def connect() -> snowflake.connector.SnowflakeConnection:
    if os.path.isfile("/snowflake/session/token"):
        creds = {
            'host': os.getenv('SNOWFLAKE_HOST'),
            'port': os.getenv('SNOWFLAKE_PORT'),
            'protocol': "https",
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'authenticator': "oauth",
            'token': open('/snowflake/session/token', 'r').read(),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA'),
            'client_session_keep_alive': True
        }
    else:
        private_key = "-----BEGIN PRIVATE KEY-----\n" + os.getenv("PRIVATE_KEY") + "\n-----END PRIVATE KEY-----\n)"

        p_key = serialization.load_pem_private_key(
            bytes(private_key, 'utf-8'),
            password=None
        )


        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption())

        creds = {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'private_key':pkb,
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA'),
            'client_session_keep_alive': True
        }

    return snowflake.connector.connect(**creds)

conn = connect()

# Make the API endpoints
container_connector = Blueprint('container_connector', __name__)

## Top 10 customers in date range
dateformat = '%Y-%m-%d'

@container_connector.route('/customers/top10')
def customers_top10():
    # Validate arguments
    sdt_str = request.args.get('start_range') or '1995-01-01'
    edt_str = request.args.get('end_range') or '1995-03-31'
    try:
        sdt = datetime.datetime.strptime(sdt_str, dateformat)
        edt = datetime.datetime.strptime(edt_str, dateformat)
    except:
        abort(400, "Invalid start and/or end dates.")
    sql_string = '''
        SELECT
            o_custkey
          , SUM(o_totalprice) AS sum_totalprice
        FROM snowflake_sample_data.tpch_sf10.orders
        WHERE o_orderdate >= '{sdt}'
          AND o_orderdate <= '{edt}'
        GROUP BY o_custkey
        ORDER BY sum_totalprice DESC
        LIMIT 10
    '''
    sql = sql_string.format(sdt=sdt, edt=edt)
    try:
        res = conn.cursor(DictCursor).execute(sql)
        return make_response(jsonify(res.fetchall()))
    except:
        abort(500, "Error reading from Snowflake. Check the logs for details.")

## Monthly sales for a clerk in a year
@container_connector.route('/clerk/<clerkid>/yearly_sales/<year>')
def clerk_montly_sales(clerkid, year):
    # Validate arguments
    try: 
        year_int = int(year)
    except:
        abort(400, "Invalid year.")
    if not clerkid.isdigit():
        abort(400, "Clerk ID can only contain numbers.")
    clerkid_str = f"Clerk#{clerkid}"
    sql_string = '''
        SELECT
            o_clerk
          ,  Month(o_orderdate) AS month
          , SUM(o_totalprice) AS sum_totalprice
        FROM snowflake_sample_data.tpch_sf10.orders
        WHERE Year(o_orderdate) = {year}
          AND o_clerk = '{clerkid}'
        GROUP BY o_clerk, month
        ORDER BY o_clerk, month
    '''
    sql = sql_string.format(year=year_int, clerkid=clerkid_str)
    try:
        res = conn.cursor(DictCursor).execute(sql)
        return make_response(jsonify(res.fetchall()))
    except:
        abort(500, "Error reading from Snowflake. Check the logs for details.")


# endpoint for querying the table
@container_connector.route('/customer/<cust_id>')
def get_customer(cust_id):
    sql_string = '''
        SELECT
            C_CUSTOMER_SK,
            C_CUSTOMER_ID,
            C_CURRENT_CDEMO_SK,
            C_CURRENT_HDEMO_SK,
            C_CURRENT_ADDR_SK,
            C_FIRST_SHIPTO_DATE_SK,
            C_FIRST_SALES_DATE_SK,
            C_SALUTATION,
            C_FIRST_NAME,
            C_LAST_NAME,
            C_PREFERRED_CUST_FLAG,
            C_BIRTH_DAY,
            C_BIRTH_MONTH,
            C_BIRTH_YEAR,
            C_BIRTH_COUNTRY,
            C_LOGIN,
            C_EMAIL_ADDRESS,
            C_LAST_REVIEW_DATE
        FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.CUSTOMER
        WHERE C_CUSTOMER_SK = {cust_id};
    '''
    sql = sql_string.format(cust_id=cust_id)
    try:
        res = conn.cursor(DictCursor).execute(sql)
        return make_response(jsonify(res.fetchall()))
    except:
        abort(500, "Error reading from Snowflake. Check the QUERY_HISTORY for details.")
