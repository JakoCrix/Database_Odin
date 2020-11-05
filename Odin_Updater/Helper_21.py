# %% Admin
import pandas as pd
from datetime import datetime
from os import listdir
import re

# %% Identifying Submission files to process
def SubmissionFiles_Relevance(Conn_Odin,
                              Path_DailyFiles= "D:\\DB_Odin\\DailyFiles\\"):
    # File Check
    SubmissionFile_List= listdir(Path_DailyFiles)
    SubmissionFile_ListDate= [re.sub("""SubmissionFile_|.csv""", "", x) for x in SubmissionFile_List]
    SubmissionFile_ListDate= [datetime.strptime(x, "%Y%m%d") for x in SubmissionFile_ListDate]

    # Database Check
    LatestDate_Raw = pd.read_sql_query("""SELECT max(LastFetched) as LastDate FROM Submission_Tracking""", Conn_Odin)
    LatestDate_Raw2= datetime.strptime(LatestDate_Raw.LastDate[0], "%Y-%m-%d %H:%M")

    SubmissionFile_ListExtract=[]
    for i in range(len(SubmissionFile_ListDate)):
        if SubmissionFile_ListDate[i]>LatestDate_Raw2:
            SubmissionFile_ListExtract.append(SubmissionFile_List[i])

    return(SubmissionFile_ListExtract)

