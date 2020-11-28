# %% Admin
import pandas as pd
from datetime import datetime
from os import listdir
import re

# %% Identifying Submission files to process
def SubmissionFiles_Relevance(Conn_Odin, Path_DailyFiles= "Submissions_LiveTracker\\"):
    # Conn_Odin= Conn_Odin; Path_DailyFiles= "Submissions_LiveTracker\\"

    # File Check
    SubmissionFile_List= listdir(Path_DailyFiles)
    SubmissionFile_ListDate= [re.sub("""SubmissionFile_|.csv""", "", x) for x in SubmissionFile_List]
    SubmissionFile_ListDate= [datetime.strptime(x, "%Y%m%d_%H%M") for x in SubmissionFile_ListDate]

    # Database Check
    LatestDate_Raw = pd.read_sql_query("""SELECT LastFetched as LastDate FROM Submission_Tracking""", Conn_Odin)
    LatestDate_Raw["LastDate_Datetime"]= pd.to_datetime(LatestDate_Raw["LastDate"])
    LatestDate_Raw2= LatestDate_Raw["LastDate_Datetime"].max().to_pydatetime()

    SubmissionFile_ListExtract=[]
    for i in range(len(SubmissionFile_ListDate)):
        if SubmissionFile_ListDate[i]>LatestDate_Raw2:
            SubmissionFile_ListExtract.append(SubmissionFile_List[i])

    return(SubmissionFile_ListExtract)

