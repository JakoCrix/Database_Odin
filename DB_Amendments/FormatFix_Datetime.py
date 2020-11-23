########################
# There was an issue with Odin where some of the date columns had multiple formats within. This is because of
# how we established the database in Odin_Inception. Most of the error formats are the at the beginning of the DB
# TODO: Possible worth having a look at Inception files to determine where the error came from.
# This file helps to correct this errors and commit the changes back into Odin.
########################
# %% Admin
import pandas as pd
from Helper.Source import connect_to_db
Conn_Odin= connect_to_db()

CheckMe_TableName= "Comment_Information"
CheckMe_ColName  = "created_utc"

# %% Data Extraction
# Submission_Info
Submission_Info_Raw= pd.read_sql_query("SELECT rowid, {} FROM {}".format(CheckMe_ColName,CheckMe_TableName),Conn_Odin)
Conn_Odin.close()

Submission_Info_Fixed= Submission_Info_Raw.copy()
Submission_Info_Fixed["FixMe"]= Submission_Info_Fixed[CheckMe_ColName].str.contains("/")

def UglyDates_Makeover(text):
    if "/" not in text:
        return text

    FirstIndice= text.find("/")

    # Processing
    str_year= text[FirstIndice+4:FirstIndice+8]
    str_month=text[FirstIndice+1:FirstIndice+3]
    str_day=text[:FirstIndice]
    if len(str_day)==1: str_day="0"+str_day

    str_datetime= text[text.find(" ") + 1:text.find(":")] + ":" + text[-2:]
    if (text.find(":")-text.find(" "))==2:
        str_datetime= " 0"+str_datetime
    elif (text.find(":")-text.find(" "))==3:
        str_datetime= " "+str_datetime

    return str_year+"-"+str_month+"-"+str_day+str_datetime
Submission_Info_Fixed[CheckMe_ColName]=Submission_Info_Fixed[CheckMe_ColName].apply(UglyDates_Makeover)

# Checking
pd.to_datetime(Submission_Info_Raw[CheckMe_ColName], format=("%Y-%m-%d %H:%M"))


# %% Database Insertion
Submission_Info_Fixed2= Submission_Info_Fixed[Submission_Info_Fixed["FixMe"]==True].reset_index()

Conn_Odin= connect_to_db()
cursor   = Conn_Odin.cursor()

for i in range(len(Submission_Info_Fixed2)):
    print("Fixing row {} of {} rows".format(i+1,
                                            len(Submission_Info_Fixed2)))
    cursor.execute("Update {} set {} = '{}' where rowid = {}".format(
        CheckMe_TableName,
        CheckMe_ColName,
        Submission_Info_Fixed2[CheckMe_ColName][i],
        Submission_Info_Fixed2["rowid"][i])
    )
    Conn_Odin.commit()

Conn_Odin.close()



