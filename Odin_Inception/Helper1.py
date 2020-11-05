import pandas as pd

# %% For Inception 4
def Inception_IsClosedModifier(SubmissionDf,
                               DfName_SubmissionID,
                               DfName_NumComments):

    # SubmissionDf=OdinDb_CurrentSubmissions; DfName_SubmissionID= "ID_Submission"; DfName_NumComments= "NumComments"
    OGColumns = list(SubmissionDf.columns)

    # OG Checks
    OGExistingClosed=SubmissionDf.groupby([DfName_SubmissionID])[['IsClosed']].agg('sum').reset_index()
    OGExistingClosed=sum(OGExistingClosed["IsClosed"]==1)

    # Loop
    FinalSubmissionFile = pd.DataFrame()
    Temp_LoopList = SubmissionDf[DfName_SubmissionID].unique()

    for Temp_SubmissionID in Temp_LoopList:
        # Temp_SubmissionID= "j5eczq"

        # Looping through Submissions
        Temp_Df = SubmissionDf[SubmissionDf[DfName_SubmissionID] == Temp_SubmissionID]

        if len(Temp_Df) <= 2:
            Temp_Df_Copy = Temp_Df.copy()
            FinalSubmissionFile = pd.concat([FinalSubmissionFile, Temp_Df_Copy])

        else:
            Temp_Df_Copy = Temp_Df.copy()

            Temp_Df_Copy["Temp1_NumComments_Change"] = Temp_Df_Copy[DfName_NumComments].diff().fillna(0)
            Temp_Df_Copy["Temp2_Count"] = [1 if element <= 0 else 0 for element in Temp_Df_Copy["Temp1_NumComments_Change"]]
            Temp_Df_Copy["Temp2_CountCumulative"] = (Temp_Df_Copy['Temp2_Count'] == 1).cumsum()
            Temp_Df_Copy["IsClosed"] = [1 if element >= 3 else 0 for element in Temp_Df_Copy["Temp2_CountCumulative"]]
            Temp_Df_Copy2 = Temp_Df_Copy.drop(['Temp1_NumComments_Change', "Temp2_Count", "Temp2_CountCumulative"],1)
            FinalSubmissionFile = pd.concat([FinalSubmissionFile, Temp_Df_Copy2])


    FinalClosed = FinalSubmissionFile.groupby([DfName_SubmissionID])[['IsClosed']].agg('sum').reset_index()
    FinalClosed = sum(FinalClosed["IsClosed"] == 1)

    # Printing Information
    print("Through {} unique Submissions: ".format(len(Temp_LoopList))+
          "\n- {} Existing closed Submissions ".format(OGExistingClosed)+
          "\n- {} Finalized closed submissions".format(FinalClosed))

    # Returning
    FinalSubmissionFile2=FinalSubmissionFile[OGColumns]

    return FinalSubmissionFile2
