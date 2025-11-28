from flask import Flask, render_template, request
import duckdb
import pandas as pd
import os

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
csv_file_path = os.path.join(BASE_DIR, 'dataset', 'User_Card_Data.csv')

@app.route("/")
def home_page():
    return render_template("home-page.html")

@app.route("/user_spend")
def user_spend_page():
    return render_template("user-spend-page.html")

@app.route("/merchant_analysis")
def merchant_analysis_page():
    return render_template("merchant-analysis-page.html")

@app.route("/user_update")
def user_update_page():
    return render_template("user-update-page.html")

@app.route("/user0")
def user0():
    #query_value = request.form.get('client_id')
    conn = duckdb.connect()
    
    sql_query = f"SELECT * FROM read_csv_auto('{csv_file_path}')"
    result_data = conn.sql(sql_query).fetchdf()
    
    html_string_table = result_data.to_string()
    return html_string_table

@app.route("/user1")
def user1():
    return render_template('query-sample-1-page.html', title='SQL Query Sample 1')

@app.route("/user2")
def user2():
    return render_template('query-sample-2-page.html', title='SQL Query Sample 2')

@app.route("/user3")
def user3():
    conn = duckdb.connect()
    
    sql_query = f"SELECT * FROM read_csv_auto('{csv_file_path}')"
    result_data = conn.sql(sql_query).fetchdf()
    
    html_web_table = result_data.to_html(classes='table table-stripped')
    return render_template('query-sample-3-page.html', table_content=html_web_table, title='SQL Query Sample 3')
    
if __name__ == '__main__':
    app.run( )

