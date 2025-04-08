FROM python:3.8

RUN pip install snowflake-snowpark-python flask flask_caching ngrok snowflake snowflake-connector-python dotenv cryptography

COPY ./src /src

WORKDIR /src

EXPOSE 8001

ENV LOG_LEVEL=DEBUG
ENV SNOWFLAKE_ACCOUNT=<REPLACE_THIS>
ENV SNOWFLAKE_USER=<REPLACE_THIS>
ENV PRIVATE_KEY="<REPLACE_THIS>"
ENV SNOWFLAKE_ROLE=DATA_API_ROLE
ENV SNOWFLAKE_DB=API_TEST
ENV SNOWFLAKE_SCHEMA=DATA
ENV SNOWFLAKE_WH=DATA_API_WH

CMD python container_app.py
