import pandas as pd


# %% Processing 1
def SubmissionFileModify1_Exclusions(conn_Object, SubmissionFile_csv):
    # %% Admin
    # SubmissionFile_csv= SubmissionFile
    OG_Columns= list(SubmissionFile_csv.columns)

    # %% Handling Closed Submissions
    DontExtract = pd.read_sql_query("""SELECT ID_Submission, sum(IsClosed) FROM Submission_Tracking 
                                          group by ID_Submission having sum(IsClosed)>=1""", conn_Object)
    DontExtract_List = DontExtract["ID_Submission"].unique().tolist()
    SubmissionFile_Exclusions2 = SubmissionFile_csv[SubmissionFile_csv["Submission_ID"].isin(DontExtract_List) == False].copy()
    SubmissionFile_Exclusions3 = SubmissionFile_Exclusions2[OG_Columns].copy()

    print("Processing 1 Complete- Excluding Closed Submissions:"+
          "\n- {} of {} submissions have been closed".format(len(SubmissionFile_csv)-len(SubmissionFile_Exclusions3),
                                                             len(SubmissionFile_csv))+
          "\n- {} submissions still open ".format(len(SubmissionFile_Exclusions3))
          )

    return(SubmissionFile_Exclusions3)


# %% Processing 2
def SubmissionFileModify2_PotentialClosures(conn_Object, SubmissionFile_csv,
                                            DfName_SubmissionID, DfName_NumComments):
    # %% Admin
    # SubmissionFile_csv=SubmissionFile_Processed1; DfName_SubmissionID= "Submission_ID"; DfName_NumComments= "Submission_NumComments"
    OG_Columns= list(SubmissionFile_csv.columns)

    OGExistingClosed = SubmissionFile_csv.groupby([DfName_SubmissionID])[['IsClosed']].agg('sum').reset_index()
    OGExistingClosed = sum(OGExistingClosed["IsClosed"] == 1)

    # %% Extraction
    TempSQL_IsCloseModifier = """SELECT ID_Subreddit as Subreddit_ID, 
                                 ID_Submission as Submission_ID, 
                                 LastFetched as Submission_LastFetched,
                                 NumComments as Submission_NumComments,
                                 Score as Submission_Score, 
                                 UpvoteRatio as Submission_UpvoteRatio, 
                                 IsClosed, 
                                 'Exists' as DatabaseExistence  
                                 FROM Submission_Tracking 
                                 WHERE ID_Submission in (SELECT ID_Submission FROM Submission_Tracking 
                                 group by ID_Submission having sum(IsClosed)==0)"""
    PotentialSubmissionsToClose = pd.read_sql_query(TempSQL_IsCloseModifier, conn_Object)

    SubmissionFile_csv2 = SubmissionFile_csv.copy()
    SubmissionFile_csv2["DatabaseExistence"] = "New"

    SubmissionFile_csv3 = pd.concat([PotentialSubmissionsToClose, SubmissionFile_csv2]).reset_index(drop=True)

    # %% Looping
    FinalSubmissionFile = pd.DataFrame()
    Temp_LoopList = SubmissionFile_csv3[DfName_SubmissionID].unique()

    for Temp_SubmissionID in Temp_LoopList:
        # Temp_SubmissionID= "j2gc7k"

        # Looping through Submissions
        Temp_Df = SubmissionFile_csv3[SubmissionFile_csv3[DfName_SubmissionID] == Temp_SubmissionID]

        if len(Temp_Df) <= 2:
            Temp_Df_Copy = Temp_Df.copy()
            FinalSubmissionFile = pd.concat([FinalSubmissionFile, Temp_Df_Copy])

        else:
            Temp_Df_Copy = Temp_Df.copy()

            Temp_Df_Copy["Temp1_NumComments_Change"] = Temp_Df_Copy[DfName_NumComments].diff().fillna(0)
            Temp_Df_Copy["Temp2_Count"] = [1 if element <= 0 else 0 for element in
                                           Temp_Df_Copy["Temp1_NumComments_Change"]]
            Temp_Df_Copy["Temp2_CountCumulative"] = (Temp_Df_Copy['Temp2_Count'] == 1).cumsum()
            Temp_Df_Copy["IsClosed"] = [1 if element >= 3 else 0 for element in Temp_Df_Copy["Temp2_CountCumulative"]]
            Temp_Df_Copy2 = Temp_Df_Copy.drop(['Temp1_NumComments_Change', "Temp2_Count", "Temp2_CountCumulative"], 1)
            FinalSubmissionFile = pd.concat([FinalSubmissionFile, Temp_Df_Copy2])

    # %% Returning
    FinalSubmissionFile2 = FinalSubmissionFile[FinalSubmissionFile["DatabaseExistence"] == "New"].copy()
    FinalSubmissionFile2 = FinalSubmissionFile2.reset_index(drop=True)

    # Final Checks
    FinalClosed = FinalSubmissionFile2.groupby([DfName_SubmissionID])[['IsClosed']].agg('sum').reset_index()
    FinalClosed = sum(FinalClosed["IsClosed"] == 1)

    # Printing Information
    print("Processing 2 Complete- Modifying Potential Closures IsClosed:"+
          "\n- {} submissions still open ".format(len(SubmissionFile_csv[DfName_SubmissionID]))+
          "\n- {} of {} will be closed moving forward ".format(sum(FinalSubmissionFile2["IsClosed"]),
                                                               len(SubmissionFile_csv))
          )

    # Returning
    FinalSubmissionFile3 = FinalSubmissionFile2[OG_Columns]

    return FinalSubmissionFile3
