# %% Admin
import pandas as pd
from Helper.Source import connect_to_db

# %% Function Creations
def AllSubmissions_Table(conn_Object):
    # conn_Object= conn_Odin

    ## Extraction
    TempQuery= "SELECT ST.ID_Subreddit, ST.ID_Submission, ST.LastFetched, ST.Numcomments, ST.IsClosed " \
                "FROM Submission_Tracking ST"
    SubmissionsTracking_Raw = pd.read_sql_query(TempQuery, conn_Object)
    SubmissionsTracking=SubmissionsTracking_Raw.copy()
    SubmissionsTracking["LastFetched"]= pd.to_datetime(SubmissionsTracking["LastFetched"]) # TODO Need to fix datetime issue!
    SubmissionsTracking2= SubmissionsTracking.sort_values('LastFetched').groupby("ID_Submission").last().reset_index()
    SubmissionsTracking3 = SubmissionsTracking2.drop('LastFetched', 1)

    TempQuery_SubmissionInfo= "SELECT SI.ID_Submission, SI.CreatedDate, SI.Title, SI.URL FROM Submission_Info SI"
    SubmissionInfo_Raw = pd.read_sql_query(TempQuery_SubmissionInfo, conn_Object)
    Submissions_All = pd.merge(left=SubmissionsTracking3, right=SubmissionInfo_Raw, on="ID_Submission", how="outer")

    TempQuery_SubredditInfo = "SELECT SI.ID_Subreddit, SI.Name as Subreddit FROM Subreddit_Info SI"
    Subreddit_Raw = pd.read_sql_query(TempQuery_SubredditInfo, conn_Object)
    Submissions_All = pd.merge(left=Submissions_All, right=Subreddit_Raw,on="ID_Subreddit", how="outer")
    Submissions_All = Submissions_All.drop('ID_Subreddit', 1)

    ## Merging and Cleaning
    Submissions_All["CreatedDate"]= pd.to_datetime(Submissions_All["CreatedDate"])

    Submissions_All_Final=Submissions_All[["Subreddit", "ID_Submission", "IsClosed",
                                           "CreatedDate","Title",
                                           "NumComments","URL"]]
    return(Submissions_All_Final)

