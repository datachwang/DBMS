from flask import Flask, render_template, request, redirect, url_for, session
import duckdb
import pandas as pd
import os

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your_very_secret_key'


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
csv_user_transations_path = os.path.join(BASE_DIR, 'dataset', 'User_transations.csv')
csv_user_card_data_path = os.path.join(BASE_DIR, 'dataset', 'user_card_data.csv')
csv_mcc_with_categories_path = os.path.join(BASE_DIR, 'dataset', 'mcc_with_categories.csv')

csv_user_spending_capability_path = os.path.join(BASE_DIR, 'dataset', 'User_Spending_Capability.csv')
csv_merchant_table_path = os.path.join(BASE_DIR, 'dataset', 'merchant_table.csv')


@app.route("/")
def home_page():
    return render_template("home-page.html")


@app.route("/user_spend")
def user_spend_page():
    return render_template("user-spend-page.html",title='User Spending Capability')

@app.route("/user_spend_input", methods=['POST'])
def user_spend_input():
    user_spend_input = int(request.form.get('client_id'))

    session['user_spend_input'] = user_spend_input
    return render_template("user-spend-page.html",title='User Spending Capability')

@app.route("/user_spend_output")
def user_spend_output():
    user_spend_input = session.pop('user_spend_input', [])
    conn = duckdb.connect()
    
    sql_query = f"SELECT * FROM read_csv_auto('{csv_user_spending_capability_path}') WHERE Client_ID={user_spend_input}"
    result_data = conn.sql(sql_query).fetchdf()
    
    html_web_table = result_data.to_html(classes='table table-stripped')
    return render_template('query-result-page.html', table_content=html_web_table, title='User Spending Capability')


@app.route("/merchant_analysis")
def merchant_analysis_page():
    return render_template("merchant-analysis-page.html",title='Merchant Table')

@app.route("/merchant_analysis_input", methods=['POST'])
def merchant_analysis_input():
    merchant_analysis_input = int(request.form.get('merchant_id'))

    session['merchant_analysis_input'] = merchant_analysis_input
    return render_template("merchant-analysis-page.html",title='Merchant Table')

@app.route("/merchant_analysis_output")
def merchant_analysis_output():
    merchant_analysis_input = session.pop('merchant_analysis_input', [])
    conn = duckdb.connect()
    
    sql_query = f"SELECT * FROM read_csv_auto('{csv_merchant_table_path}') WHERE merchant_id={merchant_analysis_input}"
    result_data = conn.sql(sql_query).fetchdf()
    
    html_web_table = result_data.to_html(classes='table table-stripped')
    return render_template('query-result-page.html', table_content=html_web_table, title='Merchant Table')

    
@app.route("/user_update", methods=["GET", "POST"])
def user_update():
    if request.method == "POST":
        action_type = request.form.get("action_type")

        if action_type == "new_transaction":
            # Read values from the HTML form
            card_id = request.form.get("card_id")
            amount = request.form.get("amount")
            merchant_id = request.form.get("merchant_id")
            mcc = request.form.get("mcc")

            try:
                cur = mysql.connection.cursor()

                # 🔴 YOUR SQL, but with parameters:
                cur.execute("""
                    INSERT INTO user_transations (card_id, date, amount, merchant_id, mcc)
                    VALUES (%s, DATE_FORMAT(NOW(), '%%Y-%%m-%%d %%H:%%i:%%s'), %s, %s, %s);
                """, (card_id, amount, merchant_id, mcc))

                mysql.connection.commit()
                cur.close()

                flash("Transaction successfully inserted!", "success")
            except Exception as e:
                mysql.connection.rollback()
                flash(f"Error inserting transaction: {e}", "danger")

            # After POST, redirect back so refresh doesn’t resubmit
            return redirect(url_for("user_update"))

    # GET → just show the form
    return render_template("user_update.html")


@app.route("/user_card")
def user_card():
    return render_template('user-card-page.html', title='User Card Data')

@app.route("/user_card_input", methods=['POST'])
def user_card_input():
    user_card_input = int(request.form.get('card_id'))

    session['user_card_input'] = user_card_input
    return render_template('user-card-page.html', title='User Card Data')

@app.route("/user_card_output")
def user_card_output():
    user_card_input = session.pop('user_card_input', [])
    conn = duckdb.connect()
    
    sql_query = f"SELECT * FROM read_csv_auto('{csv_user_card_data_path}') WHERE card_id={user_card_input}"
    result_data = conn.sql(sql_query).fetchdf()
    
    html_web_table = result_data.to_html(classes='table table-stripped')
    return render_template('query-result-page.html', table_content=html_web_table, title='User Card Data')


@app.route("/user2")
def user2():
    return render_template('query-sample-2-page.html', title='SQL Query Sample 2')

@app.route("/user2_input", methods=['POST'])
def user2_input():
    user2_input = int(request.form.get('client_id'))

    session['user2_input'] = user2_input
    return render_template('query-sample-2-page.html', title='SQL Query Sample 2')

@app.route("/user2_output")
def user2_output():
    user2_input = session.pop('user2_input', [])
    conn = duckdb.connect()
    
    sql_query = f"""
    SELECT m.Category, SUM(t.amount) AS total_spend
    FROM read_csv_auto('{csv_user_transations_path}') AS t
    JOIN read_csv_auto('{csv_user_card_data_path}') AS c ON t.card_id = c.card_id
    JOIN read_csv_auto('{csv_mcc_with_categories_path}') AS m ON t.mcc = m.MCC_code
    WHERE c.client_id = {user2_input}
    GROUP BY m.Category
    ORDER BY total_spend DESC;
    """ 
    result_data = conn.sql(sql_query).fetchdf()
    
    html_web_table = result_data.to_html(classes='table table-stripped')
    return render_template('query-result-page.html', table_content=html_web_table, title='SQL Query Sample 2')


if __name__ == '__main__':
    app.run( )

