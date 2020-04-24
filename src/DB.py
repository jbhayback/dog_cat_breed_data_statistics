import sqlalchemy as db
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database

import pandas as pd

class DB():

	def __init__(self, user, password, host):
		self.user = user
		self.password = password
		self.host = host

	def getEngine(self, dbName):
		try:
			# Establish database connection
			engine = db.create_engine(str('postgresql://' + self.user + ':' + self.password + '@' + self.host + '/' + dbName))
			# Create db if engine for input dbName is not existing
			if not database_exists(engine.url):
				create_database(engine.url)

		except SQLAlchemyError as error:
			raise error
			print("Error while creating db to PostgreSQL", error)
			return None

		finally:
			return engine

	# def createDB(self, dbName):
	# 	engine = getEngine(dbName)
	# 	connection = engine.connect()
	# 	if database_exists(engine.url):
	# 		connection.close()

	def storeData(dbName, tbName, dataFrame):
		connection = getEngine(dbName).connect()
		try:
			# Ensure that existing table with the same tbName will be dropped prior to storage
			drop_table_query = "DROP TABLE if exists " + tbName
			connection.execute(drop_table_query)
			dataFrame.to_sql(tbName, connection)

		except SQLAlchemyError as error:
			print("Error while storing data to PostgreSQL", error)
			raise error

		finally:
			# Closing db connection
			connection.close()
