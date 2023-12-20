from flask import Flask, jsonify, request, render_template
from your_script_file import my_script_function, my_script_function3, my_script_function4
from celinha import my_script_function2, my_script_function5
from financeiro import financeiro1, financeiro2
from celinha_natal import my_script_function6


app = Flask(__name__)

@app.route('/')
def index():
    return "Welcome to my Flask app!"

@app.route('/execute_script', methods=['GET'])
def execute_script():
    # Assuming your script is a function named `my_script_function`
    df = my_script_function()
    df2 = my_script_function3()
    table_html = df.to_html(classes='table')
    table_html2 = df2.to_html(classes='table')
    intermitentes_value = my_script_function4()
    return render_template('data_template.html', table1=table_html, table2=table_html2,Intermitentes=intermitentes_value)

@app.route('/execute_script2', methods=['GET'])
def execute_script2():
    # Assuming your script is a function named `my_script_function`
    df = my_script_function2()
    table_html = df.to_html(classes='table')
    ct_total_sum = my_script_function5()
    return render_template('data_template.html', table1=table_html, Total=ct_total_sum)

@app.route('/execute_script3', methods=['GET'])
def execute_script3():
    # Assuming your script is a function named `my_script_function`
    df = financeiro1()
    table_html = df.to_html(classes='table')
    df2 = financeiro2()
    table_html2 = df2.to_html(classes='table')
    return render_template('data_template.html', table1=table_html, table2=table_html2)

@app.route('/execute_script4', methods=['GET'])
def execute_script4():
    # Assuming your script is a function named `my_script_function`
    df = my_script_function6()
    table_html = df.to_html(classes='table')
    return render_template('data_template.html', table1=table_html)

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0')
