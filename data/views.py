# python -m pip install --upgrade pip

from django.shortcuts import render
from django.http import HttpResponseRedirect
from django.core.files.storage import FileSystemStorage
import csv
import os
from .main import *
import MySQLdb


from sqlalchemy import create_engine
import pandas as pd
import numpy as NP


# from .forms import myForm
from django.db import connection

def output(request):
	render(request,'data/output.html')

def keyerror(request):
	render(request,'data/keyerror.html')

def output2(request):
	render(request,'data/output2.html')

def index(request):
    if request.method == 'POST':
        select = request.POST.get('select')
        print(select)
        if select == "2":
        	print("into select 2")
        	myfile = request.POST.get('csvfile')
        	print(myfile)

        	xx = myfile.split('.')[0]
        	cursor = connection.cursor()
        	sql2 = "DROP TABLE IF EXISTS "+xx;
        	cursor.execute(sql2)
        	THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))
        	filename_path = os.path.join(THIS_FOLDER, myfile)
        	df = pd.read_csv(filename_path, sep=',')
        	engine = create_engine('mysql://root:@localhost/busigence', echo=False)
        	df.to_sql(xx, engine, index=False)
        	sql = "select * from "+xx
        	
        	cursor.execute(sql)
        	row3 = cursor.fetchall()
        	
        	with open(filename_path, "r") as f:
        		reader = csv.reader(f)
        		header = next(reader)
        		rest = [row for row in reader]
        	print(header)
        	length = len(header)
        	print(length)
        	table = []
        	print(row3[1][1])
        	for i in range(len(row3)):
        		row = []
        		for j in range(length):
        			row.append(row3[i][j])
        		table.append(row)
        		# print(table)
        	
        	args = {'header':header,'table':table,'name':xx,'length':length}
        	return render(request, 'data/output.html',args)
        if select == "1":
        	cursor = connection.cursor()
        	tab1 =  request.POST.get('tab1')
        	tab2 =  request.POST.get('tab2')
        	tab1 = tab1.strip()
        	tab2 = tab2.strip()
        	file1 = tab1+".csv"
        	file2 = tab2+".csv"
        	
        	
        	
        	join = request.POST.get('join')
        	join2 = request.POST.get('join2')
        	join3 = request.POST.get('join3')
        	join4 = request.POST.get('join4')
        	join5 = request.POST.get('join5')
        	array = []
        	array.append(join)
        	array.append(join2)
        	array.append(join3)
        	array.append(join4)
        	array.append(join5)
        	
        	print(array)
        	joining = []

        	for x in array:
        		if x != None:
        			print(x)
        			joining.append(x)

        	


        	joinA = joining[0];
        	joinB = joining[1];
        	print("joinA "+joinA);
        	print("joinB "+joinB);

        	transform = request.POST.get('transform')
        	transform2 = request.POST.get('transform2')
        	output = request.POST.get('output')
        	print(join)
        	print(join2)
        	print(transform)
        	print(transform2)
        	print(output)
        	if joinA != joinB:
        		return render(request, 'data/keyerror.html')
        	if transform == "ASC":
        		sorting_type_value = 0
        	if transform == "DESC":
        		sorting_type_value = 1

        	THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))
        	filename_path = os.path.join(THIS_FOLDER, file1)
        	
        	df_a= pd.read_csv(filename_path)
        	filename_path = os.path.join(THIS_FOLDER, file2)
        	df_b= pd.read_csv(filename_path)
			
        	
        	
        	res = Transform_join(df_a,df_b,joinA,"inner")
        	res2 = Transform_sort(res,transform2,sorting_type_value)
        	


        	engine = create_engine('mysql://root:@localhost/busigence', echo=False)
        	res2.to_sql("transform", engine, index=False)

        	cursor.execute("select * from transform")
        	res3 = cursor.fetchall()
        	output = []
        	for i in range(len(res3)):
        		row7 = []
        		for j in range(len(res3[0])):
        			row7.append(res3[i][j])
        		output.append(row7)
        	header2 = []
        	
        	THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))
        	filename_path = os.path.join(THIS_FOLDER, file1)
        	with open(filename_path, "r") as f:
        		reader = csv.reader(f)
        		header = next(reader)
        		header2.append(header)
        		rest = [row for row in reader]
        	#header2.append(header)
        	THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))
        	filename_path = os.path.join(THIS_FOLDER, file2)
        	with open(filename_path, "r") as f:
        		reader = csv.reader(f)
        		header = next(reader)
        		header2.append(header)
        		rest = [row for row in reader]
        	#header2.append(header)

        	print(header2)
        	cursor.execute("drop table transform")


        	

        	
        	return render(request, 'data/output2.html',{'res2':output,'header':header2})



        	# sql = "SELECT * FROM "+ tab1 +" a INNER JOIN "+ tab2+ " b on a."+joinA+"=b."+joinB+" ORDER BY "+transform2+" "+transform;
        	# print(sql)
        	
        	# cursor.execute(sql)
        	# row5 = cursor.fetchall()
        	# result = []
        	# header2 = []
        	# path1 = tab1+".csv"
        	# print(path1)
        	# THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))
        	# filename_path = os.path.join(THIS_FOLDER, file1)
        	# with open(filename_path, "r") as f:
        	# 	reader = csv.reader(f)
        	# 	header = next(reader)
        	# 	header2.append(header)
        	# 	rest = [row for row in reader]
        	# #header2.append(header)
        	# THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))
        	# filename_path = os.path.join(THIS_FOLDER, file2)
        	# with open(filename_path, "r") as f:
        	# 	reader = csv.reader(f)
        	# 	header = next(reader)
        	# 	header2.append(header)
        	# 	rest = [row for row in reader]
        	# #header2.append(header)

        	# print(header2)
        	
        	# for i in range(len(row5)):
        	# 	row6 = []
        	# 	for j in range(len(row5[0])):
        	# 		row6.append(row5[i][j])
        	# 	result.append(row6)

        	#print(result)



        	# return render(request, 'data/output2.html',{'result':result,'header':header2})
    	
    else:
     	cursor = connection.cursor()
     	cursor.execute("show databases")
     	row = cursor.fetchall()
     	cursor.execute("show tables")
     	row2 = cursor.fetchall()
     	databases = []
     	tables = []
     	schema = []
     	dist =  {}

     	
     	
     	
     	for x in row2:
     		tables.append(x[0])
     	for x in row:
     		databases.append(x[0])
     	for table in tables:
     		sql = "SHOW columns FROM "+table
     		cursor.execute(sql);
     		row3 = cursor.fetchall()
     		row4 = []
     		
     		for fields in row3:
     			
     			row4.append(fields[0])
     			
     			#print(row4)
     		schema.append(row4)
     		dist[table] = row4


     		
     	print(schema)
     	print(dist)
     	
     	
     	args = {'databases':databases,'tables':tables,'schema':schema,'dist':dist}
     	

    
    return render(request, 'data/index.html',args)