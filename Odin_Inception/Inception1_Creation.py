# %% Admin
import sqlite3
from os import listdir,remove
from Helper.Source import connect_to_db

# Directory Location
DBDirectory= "D:\\DB_Odin"

# %% DB Creation
if "Odin.db" in listdir(DBDirectory):
    Existence= input("Database Odin already exists, proceed to recreate? [Yes/No]")
    if Existence== "Yes":
        remove(DBDirectory+"\\Odin.db")
        OdinConnect = sqlite3.connect('D:\\DB_Odin\\Odin.db')
        OdinConnect.close()
        print("Odin succesfully created in: {}".format(DBDirectory+"\\Odin.db"))

else:
    OdinConnect = sqlite3.connect('D:\\DB_Odin\\Odin.db')
    OdinConnect.close()
    print("Odin succesfully created in: {}".format(DBDirectory + "\\Odin.db"))

# %% Table Creations
SQLStr_SubmissionTracking="""CREATE TABLE Submission_Tracking (
                            ID_Subreddit text, ID_Submission text,
                            LastFetched text, NumComments integer,
                            Score real, UpvoteRatio real, IsClosed integer)"""
SQLStr_Subreddit_Info ="""CREATE TABLE Subreddit_Info (
                            ID_Subreddit text,Name text)"""
SQLStr_Submission_Info= """CREATE TABLE Submission_Info (
                            ID_Submission text, CreatedDate text,
                            Fullname text, Title text, URL text)"""
SQLStr_Comment_Information= """CREATE TABLE Comment_Information (
                ID_Comment text, ID_ParentID text, 
                created_utc text, stickied integer, 
                ID_Submission text)"""
SQLStr_Comment= """CREATE TABLE Comment ( 
                ID_Comment text, body text)"""

Conn_Odin=connect_to_db()
c= Conn_Odin.cursor()

# LHS
c.execute(SQLStr_SubmissionTracking)
c.execute(SQLStr_Subreddit_Info)
c.execute(SQLStr_Submission_Info)

# RHS
c.execute(SQLStr_Comment_Information)
c.execute(SQLStr_Comment)

Conn_Odin.commit()
Conn_Odin.close()


# %% Checking the Database
Conn_Odin= connect_to_db()
Conn_Odin.close()