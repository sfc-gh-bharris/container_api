import os
import logging

from flask import Flask, jsonify, make_response, send_file, request, abort
from flask_caching import Cache
import snowflake.connector
from snowflake.connector import DictCursor

from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization

load_dotenv()
snowflake.connector.paramstyle='qmark'

logging.basicConfig(level=logging.INFO)

app = Flask(__name__)

# use in-memory cache, defaulted to 3min
app.config['CACHE_TYPE'] = 'SimpleCache'
app.config['CACHE_DEFAULT_TIMEOUT'] = 180

cache = Cache(app)

# connect to Snowflake using internal SPCS token
def connect_snow():

    private_key = "-----BEGIN PRIVATE KEY-----\n" + os.getenv("PRIVATE_KEY") + "\n-----END PRIVATE KEY-----\n)"

    p_key = serialization.load_pem_private_key(
        bytes(private_key, 'utf-8'),
        password=None
    )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())

    print("connecting to: " + os.getenv("SNOWFLAKE_ACCOUNT"))

    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        private_key=pkb,
        role=os.getenv("SNOWFLAKE_ROLE"),
        database=os.getenv("SNOWFLAKE_DB"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        warehouse=os.getenv("SNOWFLAKE_WH"),
        session_parameters={'QUERY_TAG': 'api-test'}, 
    )

conn = connect_snow()

# endpoint for querying the table
@app.route('/customer/<cust_id>')
@cache.memoize(timeout=180)
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

@app.route("/")
def tester():
    return send_file("test.html")

@app.errorhandler(404)
def resource_not_found(e):
    return make_response(jsonify(error='Not found!'), 404)

if __name__ == '__main__':
    app.run(port=8001, host='0.0.0.0')
