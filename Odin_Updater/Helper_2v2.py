# %% Admin
import pandas as pd
import numpy as np

# %% To update LHS table
def SubmissionFileModify1_Exclusions(conn_Object, SubmissionFile_csv):
    # %% Admin
    # conn_Object= Conn_Odin; SubmissionFile_csv= SubmissionFile
    OG_Columns = list(SubmissionFile_csv.columns)

    # %% Processing 1- AlreadyClosed
    Processing1_1 = SubmissionFile_csv.copy()

    # Labelling Closed Submissions
    DontExtract = pd.read_sql_query("""SELECT ID_Submission, sum(IsClosed) FROM Submission_Tracking 
                                          group by ID_Submission having sum(IsClosed)>=1""", conn_Object)
    DontExtract_List = DontExtract["ID_Submission"].tolist()
    Processing1_1["Prcs1_AlreadyClosed"] = Processing1_1["Submission_ID"].isin(DontExtract_List).astype(int)

    # Printing and Returning
    print("Processing 1 Complete- Excluding Closed Submissions:" +
          "\n- {} of {} submissions has already been closed".format(sum(Processing1_1["Prcs1_AlreadyClosed"]),
                                                                    len(Processing1_1)) +
          "\n- {} submissions to process ".format(
              len(Processing1_1) - sum(Processing1_1["Prcs1_AlreadyClosed"]))
          )

    Processing1_Final= Processing1_1[OG_Columns + ["Prcs1_AlreadyClosed"]]

    return (Processing1_Final)
def SubmissionFileModify2_PotentialClosures(conn_Object, SubmissionFile_csv):
    # %% Admin
    # conn_Object= Conn_Odin; SubmissionFile_csv= SubmissionFileModify1_Exclusions(Conn_Odin, SubmissionFile)
    print("Processing 2 Started- Modifying Potential Closures IsClosed:")
    OG_Columns= list(SubmissionFile_csv.columns)

    # %% Extraction
    Temp_RedditSubmissions = "','".join(SubmissionFile_csv["Submission_ID"].unique().tolist())
    Temp_RedditSubmissions2 = "'" + Temp_RedditSubmissions + "'"

    TempSQL_IsCloseModifier = """SELECT ID_Subreddit as Subreddit_ID, 
                                 ID_Submission as Submission_ID, 
                                 LastFetched as Submission_LastFetched,
                                 NumComments as Submission_NumComments,
                                 Score as Submission_Score, 
                                 UpvoteRatio as Submission_UpvoteRatio, 
                                 IsClosed, 
                                 'Exists' as DatabaseExistence  
                                 FROM Submission_Tracking 
                                 WHERE ID_Submission IN ({})""".format(Temp_RedditSubmissions2)

    PotentialSubmissionsToClose = pd.read_sql_query(TempSQL_IsCloseModifier, conn_Object)

    SubmissionFile_csv2 = SubmissionFile_csv.copy()
    SubmissionFile_csv2["DatabaseExistence"] = "New"

    SubmissionFile_csv3 = pd.concat([PotentialSubmissionsToClose, SubmissionFile_csv2]).reset_index(drop=True)

    # Deleteme = SubmissionFile_csv3[["Submission_ID", "IsClosed"]].groupby(["Submission_ID"]).agg({"IsClosed": ["count"]}).reset_index()
    # Deleteme.columns= Deleteme.columns.droplevel(1)
    # Deleteme.sort_values("IsClosed")

    # %% Looping
    Temp_LoopList = SubmissionFile_csv3["Submission_ID"].unique().tolist()

    FinalSubmissionFile = pd.DataFrame()
    for Temp_SubmissionID_Indice in range(len(Temp_LoopList)):
        if Temp_SubmissionID_Indice % 250 == 0:
            print("-- Looping and Checking {} of {} submissions.".format(Temp_SubmissionID_Indice,
                                                                        len(Temp_LoopList)))

        Temp_SubmissionID= Temp_LoopList[Temp_SubmissionID_Indice]

        # Temp_SubmissionID= "jgkp92"
        Temp_Df = SubmissionFile_csv3[SubmissionFile_csv3["Submission_ID"] == Temp_SubmissionID].copy()
        Temp_Df["Prcs2_WillClose"]= 0
        # Temp_Df[["Submission_ID", "Submission_LastFetched","Submission_NumComments"]]

        if len(Temp_Df) <= 2:
            FinalSubmissionFile = pd.concat([FinalSubmissionFile, Temp_Df])

        else:
            Temp_LastRow = Temp_Df[Temp_Df["Submission_NumComments"] ==
                                   (Temp_Df["Submission_NumComments"].max())].iloc[0]["Submission_LastFetched"]
            TimeDifference = pd.to_datetime(Temp_Df.iloc[-1]["Submission_LastFetched"]) - pd.to_datetime(Temp_LastRow)

            if TimeDifference > pd.Timedelta(days=3):
                Temp_Df.at[Temp_Df.index[-1], "Prcs2_WillClose"] = 1
            else:
                pass

            FinalSubmissionFile = pd.concat([FinalSubmissionFile, Temp_Df])


    # %% Returning
    FinalSubmissionFile2 = FinalSubmissionFile[FinalSubmissionFile["DatabaseExistence"] == "New"].copy()
    FinalSubmissionFile2 = FinalSubmissionFile2.reset_index(drop=True)

    # Printing Information
    print("Processing 2 Complete- Modifying Potential Closures IsClosed:"+
          "\n-- {} submissions still open ".format(len(SubmissionFile_csv))+
          "\n-- {} of {} will be closed moving forward ".format(sum(FinalSubmissionFile2["Prcs2_WillClose"]),
                                                               len(SubmissionFile_csv))
          )

    # Returning
    FinalSubmissionFile3 = FinalSubmissionFile2[OG_Columns+["Prcs2_WillClose"]]

    return FinalSubmissionFile3

# %% To update RHS table
def Submissions_ExtractionList(conn_Object, RedditSubmissions_List):
    # conn_Object= Conn_Odin; RedditSubmissions_List= SubmissionFile_LHSInsert
    # Extraction
    OG_Columns= list(RedditSubmissions_List.columns)

    # %% Current Database
    # Extraction
    TempSQL_1 = RedditSubmissions_List["Submission_ID"].unique().tolist()
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
