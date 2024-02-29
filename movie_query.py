#Robert Panerio
#CS 352 - Database in Python
#March 29, 2022

import mysql.connector
from csv import *
import json

#connects to the database
db = mysql.connector.connect(
		host="localhost",
		user="root",
		password="Chibaken_1109"
)

#drop (if existing), create, and use the database
dbCursor = db.cursor()
dbCursor.execute("DROP DATABASE movieDatabase")
dbCursor.execute("CREATE DATABASE movieDatabase")
dbCursor.execute("USE movieDatabase")



def createRelation():
	'''
	This function creates the tables with their corressponding attributes for the databases.
	Multilvalued attributes are normalized into 3nf. 
	Having a join table of that particular table to the main (movieTable) table
	'''
	dbCursor.execute("CREATE TABLE movieTable (budget INT(4), \
												homepage VARCHAR(255), \
												movie_id INT(4), \
												original_language VARCHAR(255), \
												original_title VARCHAR(255), \
												overview TEXT(10000), \
												popularity DOUBLE, \
												release_date VARCHAR(255), \
												revenue BIGINT(8), \
												runtime INT(4), \
												status VARCHAR(255), \
												tagline VARCHAR(255), \
												title VARCHAR(255), \
												vote_average FLOAT, \
												vote_count INT(4),\
												PRIMARY KEY (movie_id))")

	dbCursor.execute("CREATE TABLE genres (genres_id INT(4), name VARCHAR(255), PRIMARY KEY (genres_id))")
	dbCursor.execute("CREATE TABLE genres_movie (gen_mov_id INT(4) auto_increment ,genres_id INT(4), movie_id INT(4),\
												PRIMARY KEY (gen_mov_id),\
												FOREIGN KEY (genres_id) REFERENCES genres(genres_id),\
												FOREIGN KEY (movie_id) REFERENCES movieTable(movie_id))")

	dbCursor.execute("CREATE TABLE keywords (keywords_id INT(4), name VARCHAR(255), PRIMARY KEY (keywords_id))")
	dbCursor.execute("CREATE TABLE keywords_movie (key_mov_id INT(4) auto_increment, keywords_id INT(4), movie_id INT(4),\
												PRIMARY KEY (key_mov_id),\
												FOREIGN KEY (keywords_id) REFERENCES keywords(keywords_id),\
												FOREIGN KEY (movie_id) REFERENCES movieTable(movie_id))")

	dbCursor.execute("CREATE TABLE production_companies (production_companies_id  INT(4), name VARCHAR(255),PRIMARY KEY (production_companies_id))")
	dbCursor.execute("CREATE TABLE production_companies_movie (prodcom_mov_id INT(4) auto_increment, production_companies_id INT(4), movie_id INT(4),\
												PRIMARY KEY (prodcom_mov_id),\
												FOREIGN KEY (production_companies_id) REFERENCES production_companies(production_companies_id),\
												FOREIGN KEY (movie_id) REFERENCES movieTable(movie_id))")

	dbCursor.execute("CREATE TABLE production_countries (production_countries_id VARCHAR(255), name VARCHAR(255), PRIMARY KEY (production_countries_id))")
	dbCursor.execute("CREATE TABLE production_countries_movie (prodcoun_mov_id INT(4) auto_increment, production_countries_id VARCHAR(255), movie_id INT(4),\
												PRIMARY KEY (prodcoun_mov_id),\
												FOREIGN KEY (production_countries_id) REFERENCES production_countries(production_countries_id),\
												FOREIGN KEY (movie_id) REFERENCES movieTable(movie_id))")


	dbCursor.execute("CREATE TABLE spoken_languages (spoken_languages_id VARCHAR(255), name VARCHAR(255), PRIMARY KEY (spoken_languages_id))")
	dbCursor.execute("CREATE TABLE spoken_languages_movie (spoklang_mov_id INT(4) auto_increment ,spoken_languages_id VARCHAR(255), movie_id INT(4),\
												PRIMARY KEY (spoklang_mov_id),\
												FOREIGN KEY (spoken_languages_id) REFERENCES spoken_languages(spoken_languages_id),\
												FOREIGN KEY (movie_id) REFERENCES movieTable(movie_id))")

createRelation()
def ins_pars_data():
	'''
	This function parse and insert the data to the database.
	'''

	#opens the file with the encoding utf-8. for creating the movie table
	with open('tmdb_5000_movies.csv', encoding='utf-8') as movieTable:
		csvRead = DictReader(movieTable)

		nonMul = "budget, homepage, movie_id, original_language, original_title, overview, popularity, release_date, revenue, runtime, status, tagline, title, vote_average, vote_count"
	
		for rowM in csvRead:														#for each row
			dataString = ""

			for colM in rowM:														#for each attributes
				if(len(rowM[colM]) > 0):

					if colM in nonMul:												#if the attribute is not multivalued
						dataString = dataString + ", " + buildDataString(rowM[colM], colM)	

				else:
					#if the data string is empty add null to the dabase instead
					dataString  = dataString + ", " + 'Null'

			#insert the data string into the movie table
			dbCursor.execute("INSERT INTO movieTable("+nonMul+") VALUES("+dataString[2:len(dataString)]+")")

	#opens the file for creating the tables of the multivalued attributes
	with open('tmdb_5000_movies.csv', encoding='utf-8') as relationTableMovie:
		csvRead = DictReader(relationTableMovie)
		spokArr = []
		spok = 1

		mulAtt = "genres, keywords, production_countries, production_companies, spoken_languages"
		for row in csvRead:
			movie_col_Rel = ""
			
			for col in row:
				if(col == 'id'): 													#saves the movie id
					movie_col_Rel = row[col]

				if(len(row[col]) > 0):												#checking the length of the data
					if col in mulAtt:												#if the col is multivalued attribute
						tempStr = row[col][1:len(row[col])-1]						#removes the brackets of the data string

						if(len(tempStr) > 1):										#making sure the length of the data string
							tempStr += ","
							tempArr = [char + "}" for char in tempStr.split("},") if char] #creating array of json strings
							
							for jsonStr in tempArr:									#looping through the array of json string
								strData = json.loads(jsonStr)						#loads the json string
								jsonName = buildDataString(strData["name"], col)	#modify strings with special characters

								#checking the right id of the 'col' attribute
								if(col == "production_countries"):
									jsonID = "'" + strData["iso_3166_1"] + "'"
								elif(col == "spoken_languages"):
									jsonID = "'" + strData["iso_639_1"] + "'"
								else:	
									jsonID = str(strData["id"])

								jsonName = "'" + jsonName + "'" if len(jsonName) != 0 else 'Null'

								#first insert the data string to the table made for the multivalued attributes and making sure that there are no duplicates
								#second inserts the relationship table of that particular multivalued attribute and movieTable
								dbCursor.execute("INSERT IGNORE INTO "+col+"("+col+"_id,name) VALUES("+jsonID+", "+jsonName+")")
								dbCursor.execute("INSERT INTO "+col+"_movie("+col+"_id, movie_id) VALUES("+jsonID+", "+row['id']+")")

def buildDataString(strAtt, attr):
	'''
	This function modify the strings.
	adding '\' before special characters like apostrophe and double-quotes
	'''
	temp = ""

	#array of non multivalued attributes
	attrStr = ['homepage', 'original_language', 'original_title', 'overview', 'release_date', 'status', 'tagline', 'title']
	
	#looking for the special character
	for char in strAtt:
		if(char == '\'' or char == '\"'):
			temp = temp + "\\" + char
		else:
			temp += char
	#checking attr is non multivalued attributes
	if attr in attrStr:
		temp = "'" + temp +"'"

	return temp


ins_pars_data()

# Query 1
# dbCursor.execute("SELECT avg(budget) FROM movieTable")
# rows = dbCursor.fetchall()
# for row in rows:
# 	print(row)

# Query 2
# query2 = "SELECT title, name FROM  movieTable \
# NATURAL JOIN production_companies \
# NATURAL JOIN production_companies_movie \
# NATURAL JOIN (\
# 	SELECT movie_id FROM \
# 	production_countries_movie WHERE production_countries_id = 'US') as T"
# dbCursor.execute(query2)
# rows = dbCursor.fetchall()
# for row in rows:
# 	print(row)

# Query 3
# dbCursor.execute("SELECT DISTINCT title, revenue FROM movieTable ORDER BY revenue DESC LIMIT 0,5")
# rows = dbCursor.fetchall()
# for row in rows:
# 	print(row)

# Query 4 
# query4 = "SELECT title, genres.name FROM movieTable \
# NATURAL JOIN genres_movie \
# NATURAL JOIN genres \
# NATURAL JOIN (\
# 	SELECT movie_id FROM genres \
# 	NATURAL JOIN genres_movie WHERE name IN ('Science Fiction', 'Mystery') GROUP BY movie_id HAVING COUNT(*) > 1) as T"
# dbCursor.execute(query4)
# rows = dbCursor.fetchall()
# for row in rows:
# 	print(row)

# Query 5
# dbCursor.execute("SELECT title, popularity FROM movieTable WHERE popularity > (SELECT AVG(popularity) FROM movieTable)")
# rows = dbCursor.fetchall()
# for row in rows:
# 	print(row)
db.commit()