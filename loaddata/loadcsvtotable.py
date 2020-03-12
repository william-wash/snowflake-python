# Snowflake Sample tester python
# 2020-03-05	WWASH	Initial Cut
#
# Version 1.0
################################################################
# NOTE: This script is for educational purposes only.
# It is not meant to be run on production systems without
# modification and testing.
# Snowflake Computing assumes no responsibility for use of
# this script.
################################################################

import os
import csv
import ast
import re
import getpass
import snowflake.connector

# config, including a virtual warehouse (whouse) for running your queries
uname='admin'
acct='demo123'
whouse='DEV_WH'
datab='CUSTOMERS'
schema='LASERSHIP'
urole='SYSADMIN'
stagename="LASERSHIP_STAGE"
print ("Connecting to Snowflake account " + acct )
pword = getpass.getpass(prompt=" Password for " + uname + ": ")


ctx = snowflake.connector.connect(
   user=uname,
   password=pword,
   account=acct,
   warehouse=whouse,
   database=datab,
   schema=schema,
   role=urole
   )
print ("Connected")


def dataType(val, current_type):
	try:
	# Evaluates numbers to an appropriate type, and strings an error
		t = ast.literal_eval(val)
		#print (type(t))
	except ValueError:
		return 'TEXT'
	except SyntaxError:
		return 'TEXT'
	#print (type(t))
	if type(t) is int and current_type not in ['FLOAT', 'TEXT']:
		return 'NUMBER'
	if type(t) is float:
		return 'FLOAT'
	else:
		return 'TEXT'

def tableddl (filepath, tablename):
	f = open(filepath, 'r')
	reader = csv.reader(f)

	longest, headers, type_list = [], [], []

	for row in reader:
		if len(headers) == 0:
			headers = row
			for col in row:
				longest.append(0)
				type_list.append('')
		else:
			for i in range(len(row)):
			# NA is the csv null value
				if type_list[i] == 'varchar' or row[i] == 'NA':
					pass
				else:
					var_type = dataType(row[i], type_list[i])
					type_list[i] = var_type
	if len(row[i]) > longest[i]:
		longest[i] = len(row[i])
	f.close()

	statement = 'CREATE OR REPLACE TABLE ' + tablename + '_STG ('

	for i in range(len(headers)):
		if type_list[i] == 'varchar':
			statement = (statement + '\n{} varchar({}),').format(re.sub('[^0-9a-zA-Z]+', '_',headers[i].lower()), str(longest[i]))
		else:
			statement = (statement + '\n' + '{} {}' + ',').format(re.sub('[^0-9a-zA-Z]+', '_',headers[i].lower()), type_list[i])

	statement = statement[:-1] + ');'
	return statement


for root, dirs, files in os.walk("/home/snowflake/python/loadcsvtotable", topdown=True):
	for name in files:
		path = root.split("/")
		if path[-1] == "incoming":
			print("Processing File: " + os.path.join(root, name))
			tablename=path[-2]
			ct = tableddl(os.path.join(root, name),tablename)
			print (ct)
			cur = ctx.cursor()
			print ("Creating table " + tablename + " if it does not exist")
			cur.execute(ct)
			putstmt = "PUT file://" + os.path.join(root, name) + " @LASERSHIP_STAGE;"
			print ("Staging File with command: " + putstmt)
			cur.execute(putstmt)
			copystmt="COPY INTO " + tablename + "_STG FROM @LASERSHIP_STAGE/" + name + ".gz FILE_FORMAT=(TYPE=csv SKIP_HEADER=1);"
			print ("Staging Complete, staging data into table with command: " + copystmt)
			cur.execute(copystmt)
			print ("Copying complete, creating final table if it does not exist");
			ctstmt = "CREATE TABLE IF NOT EXISTS " + tablename + " LIKE " + tablename + "_STG;"
			cur.execute(ctstmt)
			print ("Create complete, moving from stage table to final table");
			cur.execute("INSERT INTO " + tablename + " SELECT * FROM " + tablename + "_STG;")
			print ("Moving complete, moving file to processed folder")
			print ("Source: " + os.path.join(root, name))
			print ("Target: " +  os.path.join(root.replace("incoming","processed"), name))
			os.rename(os.path.join(root, name), os.path.join(root.replace("incoming","processed"), name))
			print("Processing Complete: " + os.path.join(root, name))
