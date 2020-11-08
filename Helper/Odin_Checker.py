import pandas as pd

def OdinChecker_Rows(conn_Object):
    Rows_Comment= pd.read_sql_query("select COUNT(1) as rows from Comment", conn_Object)["rows"][0]
    Rows_CommentInformation= pd.read_sql_query("select COUNT(1) as rows from Comment_Information", conn_Object)["rows"][0]
    Rows_SubmissionInfo= pd.read_sql_query("select COUNT(1) as rows from Submission_Info", conn_Object)["rows"][0]
    Rows_SubmissionTracking= pd.read_sql_query("select COUNT(1) as rows from Submission_Tracking", conn_Object)["rows"][0]
    Rows_SubredditInfo= pd.read_sql_query("select COUNT(1) as rows from Subreddit_Info", conn_Object)["rows"][0]

    return {"Comment": Rows_Comment,
            "Comment_Information": Rows_CommentInformation,
            "Submission_Info":Rows_SubmissionInfo,
            "Submission_Tracking": Rows_SubmissionTracking,
            "Subreddit_Info": Rows_SubredditInfo}
