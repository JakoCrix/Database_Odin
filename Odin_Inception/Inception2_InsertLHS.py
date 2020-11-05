# %% Admin
import pandas as pd

from Helper.Odin_Insertion import OdinInsert_SubmissionTracking, OdinInsert_SubredditInfo, OdinInsert_SubmissionInfo
from Helper.Source import connect_to_db

# %% Database Insertion- LHS
SubmissionFile_List= ["SubmissionFile_20201014.csv",
                      "SubmissionFile_20201015.csv",
                      "SubmissionFile_20201016.csv"]
Conn_Odin= connect_to_db()

# %% Extraction
for SubmissionFile in SubmissionFile_List:
    # SubmissionFile= SubmissionFile_List[0]
    print("____"*20)
    print("Inserting file "+SubmissionFile+" into Odin Database")
    SubmissionFile_Df= pd.read_csv("C:\\Users\\Andrew\\Documents\\GitHub\\PersonalCookbook_Python\\"+
                                   "Projects_RedditCrawler\\Database_Odin\\TempFiles\\"+SubmissionFile)

    # DataInsertion
    OdinInsert_SubmissionTracking(Conn_Odin, SubmissionFile_Df)
    OdinInsert_SubredditInfo(Conn_Odin, SubmissionFile_Df)
    OdinInsert_SubmissionInfo(Conn_Odin, SubmissionFile_Df)

Conn_Odin.close()

