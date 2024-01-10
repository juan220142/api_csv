
# Imports
from flask import Flask, request, jsonify 
import mysql.connector
import pandas as pd
# This main route has a general explanation of the api's functionality
app = Flask(__name__)
@app.route('/', methods = ['GET'])
def instructions():
    return "THIS API WORK WITH THE NEXT STRUCTURE: /uploadDeparment is to load deparments.CSV  /uploadJob is to load jobs.CSV  /uploadHiredEmployee is to load Hired_employees.CSV"
# deparment load route
# On this function you are going to find the load of the csv department
@app.route('/uploadDeparment', methods=['POST'])
def upload_deparment():
    # Database connection
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Contraseña.1",
    database="company") 
    mycursor = mydb.cursor()
    # validate that the request contains a file
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'})
    # read the file
    file = request.files['file']
    #validate file type
    if file.filename == '' or not file.filename.endswith('.csv'):
        return jsonify({'error': 'Invalid file'})

    try:
        #deparment cols
        cols = ['id','department']
        csv_file = pd.read_csv(file,names = cols, header = None)
       
        for j, row in csv_file.iterrows():
            sql_script = "INSERT INTO department (id, department) VALUES (%s,%s)"
            value = (row['id'], row['department'])
            mycursor.execute(sql_script,value)
            mydb.commit()
            print(j,row['id'], row['department'])
        mycursor.close()
        mydb.close()
        return jsonify({'success':f'the data on the csv file{file.filename} has been loaded to the database'})

    except Exception as error:
        return jsonify({'error': str(error)})
    
# job load route
# On this route you are going to find the load of the csv job
@app.route('/uploadJob', methods=['POST'])
def upload_jobs():
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Contraseña.1",
    database="company") 
    mycursor = mydb.cursor()
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'})

    file = request.files['file']

    if file.filename == '' or not file.filename.endswith('.csv'):
        return jsonify({'error': 'Invalid file'})

    try:
        cols = ['id','job']
        csv_file = pd.read_csv(file,names = cols, header = None)
       
        for j, row in csv_file.iterrows():
            sql_script = "INSERT INTO job (id, job) VALUES (%s,%s)"
            value = (row['id'], row['job'])
            mycursor.execute(sql_script,value)
            mydb.commit()
            print(j,row['id'], row['job'])
        mycursor.close()
        mydb.close()
        return jsonify({'success':f'the data on the csv file{file.filename} has been loaded to the database'})

    except Exception as error:
        return jsonify({'error': str(error)})

#hired_employees load route
# On this route you are going to find the load of the csv hiredEmployees
@app.route('/uploadHiredEmployee', methods=['POST'])
def upload_hired():
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Contraseña.1",
    database="company") 
    mycursor = mydb.cursor()
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'})

    file = request.files['file']

    if file.filename == '' or not file.filename.endswith('.csv'):
        return jsonify({'error': 'Invalid file'})

    try:
        # Here are two options , 1 drop empty values, that are trash data, or fill that data based on the business requiremnt.
        cols = ['id','name','datetime','department_id','job_id']
        csv_file = pd.read_csv(file,names = cols, header = None)
        csv_file['name'] = csv_file['name'].fillna('no named')
        csv_file = csv_file.dropna()

        print(csv_file)
        for j, row in csv_file.iterrows():
            sql_script = "INSERT INTO hiredEmployee (id,name,datetime,department_id,job_id) VALUES (%s,%s,%s,%s,%s)"
            value = (row['id'], row['name'], row['datetime'], row['department_id'], row['job_id'])
            mycursor.execute(sql_script,value)
            mydb.commit()
            print(j,row['id'], row['name'], row['datetime'], row['department_id'], row['job_id'])
        mycursor.close()
        mydb.close()
        return jsonify({'success':f'the data on the csv file{file.filename} has been loaded to the database'})

    except Exception as e:
        return jsonify({'error': str(e)})
# end point for department quarter analysis
@app.route('/departmentQuarter', methods = ['GET'])
def get_employees_hired():
    # Database connection
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Contraseña.1",
    database="company") 
    mycursor = mydb.cursor()
    # sql query to connect to the Viwe of analysis
    sql_script = "SELECT * FROM employees_department;"
    mycursor.execute(sql_script)
    data = mycursor.fetchall()
    table = [x for x in data]
    mycursor.close()
    mydb.close()
    # query output
    return  jsonify(table)
# endpoint for department hire analysis
@app.route('/departmentHire', methods = ['GET'])
def get_department_hired():
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Contraseña.1",
    database="company") 
    mycursor = mydb.cursor()

    sql_script = "SELECT * FROM department_hire;"
    mycursor.execute(sql_script)
    data = mycursor.fetchall()
    table = [x for x in data]
    mycursor.close()
    mydb.close()
    return  jsonify(table)
if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=True)
