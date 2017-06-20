from flask import Flask,request
from flask import render_template
import time
import json
from random import randint
from cassandra.cluster import Cluster

#get connected to cassandra database
cluster = Cluster()
session = cluster.connect('insight')

app = Flask(__name__)


# initialize the initial page with random data.
@app.route('/')
def dash():
    jresponse = [{'pid': randint(1,99),'userid':1} for i in range(10)]
    return render_template('dashboard.html',uid = jresponse)


# take data from the form and select data from cassandra as recommendation list
@app.route('/', methods=['POST'])
def my_form_post():


    uid = request.form['uid']
    statement = "SELECT * FROM recommendationlist WHERE userid = '%s';"%uid
    response = session.execute(statement)
    if response:

        reclist = response[0].rlist
        rlist1 = reclist[1:-1].split(', ')
        jrespose = [{'pid':int(x),'userid':int(uid)} for x in rlist1]
        return render_template('dashboard.html',uid= jrespose)

# if it's a new user then recommend the curret most popular product 
    else:
        timeforsearch = int(round(time.time() * 1000)) - 3*1000
        statement = "SELECT * FROM popularproduct WHERE timestamp > %d;"%timeforsearch
        response = session.execute(statement)
        response_list = []
        for val in response:
            response_list.append[val]
        jsonresponse = [{'pid': int(x.pid),'uerid':int(uid) } for x in response_list]
        return render_template('dashboard.html',uid = jsonresponse)



@app.route('/realmap')
def realmap():
    return render_template('map.html')


@app.route('/age')
def age():
    return render_template('realage.html')


# generate realtime age data and dumps to json file
@app.route('/agedata')
def agedata():
    response = []
    timeforsearch = int(round(time.time() * 1000)) - 3*1000
    statement = "SELECT * FROM realtimeage WHERE timestamp > %d;"%timeforsearch
    response = session.execute(statement)
    response_list = []
    for val in response:
        response_list.append[val]
    jsonresponse = [{'value': x.count,'code':x.age } for x in response_list]
    data_x = json.dumps(jsonresponse)
    return data_x

# generate realtime state data and dumps to json file
@app.route('/data')
def mapresult():
    response = []
    timeforsearch = int(round(time.time() * 1000)) - 3*1000
    statement = "SELECT * FROM realtimestate WHERE timestamp > %d;"%timeforsearch
    response = session.execute(statement)
    response_list = []
    for val in response:
        response_list.append[val]
    jsonresponse = [{'value': x.count,'code':x.state } for x in response_list]
    data_x = json.dumps(jsonresponse)
    return data_x


# set debug as true and running in port 5000

if __name__ == '__main__':
    app.run(host = "0.0.0.0",debug = True)

