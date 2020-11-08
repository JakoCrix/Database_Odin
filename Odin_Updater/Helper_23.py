# %% Admin
import pandas as pd
import numpy as np

# %%
def Submissions_ExtractionList(conn_Object, RedditSubmissions_List):
    # conn_Object= Conn_Odin; RedditSubmissions_List= SubmissionFile_ExtractionList
    # Extraction
    OG_Columns= list(RedditSubmissions_List.columns)


    # %% Current Database
    # Extraction
    TempSQL_1 = RedditSubmissions_List.Submission_ID.unique().tolist()
    TempSQL_2="','".join(list(TempSQL_1))
    TempSQL_2= "'"+TempSQL_2+"'"
    TempSQL_Submissions = """SELECT ID_Submission as Submission_ID, 
                             LastFetched as Submission_LastFetched,
                             NumComments as Submission_NumComments
                             FROM Submission_Tracking 
                             WHERE ID_Submission in ({})""".format(TempSQL_2)
    OdinSubmissions= pd.read_sql_query(TempSQL_Submissions, conn_Object)

    # Processing
    OdinSubmissions_LastFetched= OdinSubmissions[["Submission_ID", "Submission_LastFetched"]].\
        groupby(["Submission_ID"]).tail(1).reset_index(drop=True)
    OdinSubmissions_NumComments = OdinSubmissions[["Submission_ID", "Submission_NumComments"]].\
        groupby(["Submission_ID"]).tail(1).reset_index(drop=True)

    OdinSubmissions2= pd.merge(left= OdinSubmissions_LastFetched, right= OdinSubmissions_NumComments,on="Submission_ID")
    OdinSubmissions2.columns= ["Submission_ID","Odin_LastFetched","Odin_Numcomments"]

    # %% NewFile
    NewFile_LastFetched= RedditSubmissions_List[["Submission_ID", "Submission_LastFetched"]].\
        groupby(["Submission_ID"]).tail(1).reset_index(drop=True)
    NewFile_NumComments = RedditSubmissions_List[["Submission_ID", "Submission_NumComments"]].\
        groupby(["Submission_ID"]).tail(1).reset_index(drop=True)

    NewFileSubmissions= pd.merge(left= NewFile_LastFetched, right= NewFile_NumComments,on="Submission_ID")
    NewFileSubmissions.columns= ["Submission_ID","NewFile_LastFetched","NewFile_Numcomments"]

    # %% Merging
    Comparison= pd.merge(OdinSubmissions2, NewFileSubmissions, how= "outer", on="Submission_ID")
    Comparison["ToExtract_NewSubmission"]= [int(i) for i in Comparison["Odin_LastFetched"].isna()]
    Comparison["ToExtract_Same"]= np.where(Comparison["NewFile_Numcomments"] >= Comparison["Odin_Numcomments"],
                                           0,1)
    Comparison["ToExtract"]= Comparison["ToExtract_NewSubmission"]+Comparison["ToExtract_Same"]

    Comparison2= Comparison[Comparison["ToExtract"]>0].copy()

    # Printing Information
    print("Processing 3 Complete- Only Extracting submissions with changes:"+
          "\n- {} submissions still open ".format(len(RedditSubmissions_List["Submission_ID"].unique()))+
          "\n- {} of {} has changes and will be extracted".format(len(Comparison2["Submission_ID"]),
                                                                  len(RedditSubmissions_List["Submission_ID"].unique())
                                                                  )
          )

    return Comparison2["Submission_ID"].tolist()



def OdinExtraction_Comments(conn_Object, RedditSubmissions_List):
    # RedditSubmissions_List= TempExtract_List
    # Extraction
    Temp_RedditSubmissions="','".join(list(RedditSubmissions_List))
    Temp_RedditSubmissions2= "'"+Temp_RedditSubmissions+"'"

    TempSQL_1 = """SELECT ID_Submission, ID_Comment FROM Comment_Information WHERE ID_Submission IN ({})""".format(Temp_RedditSubmissions2)
    CommentID_Submission= pd.read_sql_query(TempSQL_1, conn_Object)

    return CommentID_Submission


def Comments_ExistenceCheck(RedditExtraction_Submission, OdinExtraction_Submission):
    # RedditExtraction_Submission= Temp_Comments; OdinExtraction_Submission= Temp_OdinExtraction
    CommentsToRemove= RedditExtraction_Submission["Comment_ID"].isin(OdinExtraction_Submission["ID_Comment"])==False
    Comments_NonExistence= RedditExtraction_Submission[CommentsToRemove]
    print("_{} of {} comments extracted already exists in Odin".format(len(RedditExtraction_Submission)-len(Comments_NonExistence),
                                                                      len(RedditExtraction_Submission)))

    return Comments_NonExistence
