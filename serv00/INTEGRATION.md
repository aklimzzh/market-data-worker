1. pip install "psycopg[binary]>=3.2,<4"
2. Export DATABASE_URL and FUNDFLOW_API_TOKEN in the Serv00 application environment.
3. In the existing Flask app factory/main file:
   from fundflow_api import fundflow_bp
   app.register_blueprint(fundflow_bp)
4. Run sql/serv00_fundflow_schema.sql against the existing PostgreSQL database.
5. Restart the Serv00 Python application.
