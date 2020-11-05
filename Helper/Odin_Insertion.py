# %% Admin
import pandas as pd


# %% Database Odin insert- Submission_Tracking -
def OdinInsert_SubmissionTracking(conn_Object, SubmissionFile_Df):
    # Processing for Df
    SubmissionFile_DfCopy=SubmissionFile_Df.copy()

    # Checking IsClosed Statement
    if "IsClosed" in list(SubmissionFile_DfCopy.columns):
        pass
    else:
        SubmissionFile_DfCopy["IsClosed"]= 0   # Adding additional column for DB management

    # Insertion
    SubmissionFile_Df2= SubmissionFile_DfCopy[["Subreddit_ID","Submission_ID", "Submission_LastFetched",
                                          "Submission_NumComments", "Submission_Score", "Submission_UpvoteRatio",
                                           "IsClosed"]]
    SubmissionFile_Df2.columns = ["ID_Subreddit", "ID_Submission", "LastFetched","NumComments",
                                  "Score","UpvoteRatio", "IsClosed"]

    # Inserting into Df
    SubmissionFile_Df2.to_sql(name="Submission_Tracking", con=conn_Object,
                              if_exists='append', index=False)

    conn_Object.commit()
    print("Succesfully inserted {} rows into Odin table Submission_Tracking".format(str(len(SubmissionFile_Df2))))

# %% Database Odin insert- Subreddit_Info
def OdinInsert_SubredditInfo(conn_Object, SubmissionFile_Df):
    # Processing for Df
    SubmissionFile_Df2= SubmissionFile_Df[["Subreddit_ID","Subreddit_Name"]]
    SubmissionFile_Df2.columns = ["ID_Subreddit", "Name"]
    SubmissionFile_Df3= SubmissionFile_Df2.drop_duplicates()

    # Inserting into Df
    CurrentTable = pd.read_sql_query("SELECT * FROM Subreddit_Info", conn_Object)
    CurrentTable_Exists= CurrentTable["ID_Subreddit"].tolist()
    SubmissionFile_Df4= SubmissionFile_Df3[SubmissionFile_Df3["ID_Subreddit"].isin(CurrentTable_Exists)== False]

    SubmissionFile_Df4.to_sql(name="Subreddit_Info", con=conn_Object,
                              if_exists='append', index=False)

    conn_Object.commit()
    print("Succesfully inserted {} rows into Odin table Subreddit_Info".format(str(len(SubmissionFile_Df4))))

# %% Database Odin insert- Submission_Info
def OdinInsert_SubmissionInfo(conn_Object, SubmissionFile_Df):
    # Processing for Df
    SubmissionFile_Df2= SubmissionFile_Df[["Submission_ID","Submission_CreatedDate",
                                           "Submission_Title", "Submission_URL"]]
    SubmissionFile_Df2.columns = ["ID_Submission", "CreatedDate", "Title","URL"]

    # Inserting into Df
    CurrentTable = pd.read_sql_query("SELECT * FROM Submission_Info", conn_Object)
    CurrentTable_Exists= CurrentTable["ID_Submission"].tolist()
    SubmissionFile_Df3= SubmissionFile_Df2[SubmissionFile_Df2["ID_Submission"].isin(CurrentTable_Exists)== False]

    SubmissionFile_Df3.to_sql(name="Submission_Info", con=conn_Object,
                              if_exists='append', index=False)

    conn_Object.commit()
    print("Succesfully inserted {} rows into Odin table Submission_Info".format(str(len(SubmissionFile_Df3))))

# %% Database Odin insert- Comment_Information -
def OdinInsert_CommentInformation(conn_Object,SubmissionID_Str, CommentFile_Df):
    # SubmissionID_Str="j2gc7k"; CommentFile_Df= Temp_Comments; conn_Object= Conn_Odin
    # Processing for Df
    CommentFile_Df2= CommentFile_Df[["Comment_ID","Comment_IDParent", "Comment_Time",
                                     "Comment_Stickied","Comment_IDSubmission"]]
    CommentFile_Df2.columns = ["ID_Comment", "ID_ParentID","created_utc", "stickied", "ID_Submission"]

    # Inserting into Df
    try:
        CurrentTable = pd.read_sql_query("SELECT ID_Submission, ID_Comment FROM Comment_Information "+"where ID_Submission LIKE '{}'".format(SubmissionID_Str),
                                         conn_Object)
        CurrentTable_Exists= CurrentTable["ID_Comment"].tolist()
        CommentFile_Df3= CommentFile_Df2[CommentFile_Df2["ID_Comment"].isin(CurrentTable_Exists)== False]
    except:
        CommentFile_Df3= CommentFile_Df2

    CommentFile_Df3.to_sql(name="Comment_Information", con=conn_Object,
                              if_exists='append', index=False)

    conn_Object.commit()
    print("Succesfully inserted {} rows into Odin table Comment_Information".format(str(len(CommentFile_Df3))))

# %% Database Odin insert- Comment -
def OdinInsert_Comment(conn_Object, CommentFile_Df):
    # SubmissionID_Str="j2gc7k"; CommentFile_Df= Temp_Comments; conn_Object= Conn_Odin
    # Processing for Df
    CommentFile_Df2= CommentFile_Df[["Comment_ID","Comment_Body"]]
    CommentFile_Df2.columns = ["ID_Comment", "body"]

    # Inserting into Df
    CommentFile_Df3 = CommentFile_Df2

    CommentFile_Df3.to_sql(name="Comment", con=conn_Object,if_exists='append', index=False)

    conn_Object.commit()
    print("Succesfully inserted {} rows into Odin table Comment".format(str(len(CommentFile_Df3))))



