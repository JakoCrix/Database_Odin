# %% Admin
import pandas as pd
import time
from datetime import datetime
from os import listdir

from Helper.Source import connect_to_db

Path_DailyFiles= "DailyFiles\\"
Conn_Odin = connect_to_db()

# %% Database Check
#  List of files
from Odin_Updater.Helper_21 import SubmissionFiles_Relevance
SubmissionCsv_List = SubmissionFiles_Relevance(Conn_Odin, Path_DailyFiles= Path_DailyFiles)

# %% Extraction Phase
for SubmissionCsv_File in SubmissionCsv_List:
    ###################################################################
    # Admin and Extraction
    Conn_Odin = connect_to_db()
    print("*****"*20)
    print("Extracting Submission File from csv: {}".format(SubmissionCsv_File))
    print("*****" * 20)
    SubmissionFile = pd.read_csv(Path_DailyFiles+SubmissionCsv_File)
    SubmissionFile["IsClosed"]=0

    ###################################################################
    # Processing
    from Odin_Updater.Helper_22 import SubmissionFileModify1_Exclusions, SubmissionFileModify2_PotentialClosures

    SubmissionFile_Processed1 = SubmissionFileModify1_Exclusions(Conn_Odin, SubmissionFile)
    SubmissionFile_Processed2 = SubmissionFileModify2_PotentialClosures(Conn_Odin, SubmissionFile_Processed1,
                                                                        "Submission_ID", "Submission_NumComments")
    # To be Inserted
    SubmissionFile_ExtractionList = SubmissionFile_Processed2[SubmissionFile_Processed2["IsClosed"] == False]

    ###################################################################
    # Database Insertion
    from Odin_Updater.Helper_23 import Submissions_ExtractionList, OdinExtraction_Comments
    from Helper.RedditExtraction_Comments import DG_Comments
    from Helper.Odin_Insertion import OdinInsert_CommentInformation, OdinInsert_Comment
    from Odin_Updater.Helper_23 import Comments_ExistenceCheck

    # Loop Preparations
    Submission_List= Submissions_ExtractionList(Conn_Odin, SubmissionFile_ExtractionList)
    SubmissionComments_Df = OdinExtraction_Comments(Conn_Odin, Submission_List)

     # %% Database Insertion- RHS
    for SubmissionIndex in range(len(Submission_List)):
        # SubmissionIndex=630
        print("____" * 20)
        print("Inserting Comments for Submission {} of {}".format(SubmissionIndex + 1, len(Submission_List)))

        # Extraction
        try:
            Temp_Comments = DG_Comments(Submission_List[SubmissionIndex])
        except TimeoutError:
            print("_Caught a timeout! Sleeping for 10 seconds before retrying. ")
            time.sleep(10)
            Temp_Comments = DG_Comments(Submission_List[SubmissionIndex])
        except Exception:
            print("_Error with extraction, logging as an error and passing. ")
            pass

        # Extraction Cleanup
        Temp_OdinExtraction= SubmissionComments_Df[SubmissionComments_Df["ID_Submission"] == Submission_List[SubmissionIndex]]
        Temp_Comments2= Comments_ExistenceCheck(Temp_Comments, Temp_OdinExtraction)

        # Insertion
        if Temp_Comments2.empty:
            pass
        else:
            OdinInsert_CommentInformation(Conn_Odin, Submission_List[SubmissionIndex], Temp_Comments2)
            OdinInsert_Comment(Conn_Odin, Temp_Comments2)
    print("____" * 20)

    # Database Insertion- LHS
    from Helper.Odin_Insertion import OdinInsert_SubmissionTracking, OdinInsert_SubredditInfo, OdinInsert_SubmissionInfo

    OdinInsert_SubmissionTracking(Conn_Odin, SubmissionFile_ExtractionList)
    OdinInsert_SubredditInfo(Conn_Odin, SubmissionFile_ExtractionList)
    OdinInsert_SubmissionInfo(Conn_Odin, SubmissionFile_ExtractionList)

    Conn_Odin.close()

# %% Logging
from Helper.Odin_Checker import OdinChecker_Rows
Conn_Odin = connect_to_db()
OdinStatus_values= OdinChecker_Rows(Conn_Odin)
OdinStatus2= pd.DataFrame({"Date": str(datetime.now())[:17],
                           "Rows_SubredditInfo": [OdinStatus_values["Subreddit_Info"]],
                           "Rows_SubmissionInfo": [OdinStatus_values["Submission_Info"]],
                           "Rows_SubmissionTracking": [OdinStatus_values["Submission_Tracking"]],
                           "Rows_CommentInformation": [OdinStatus_values["Comment_Information"]],
                           "Rows_Comment": [OdinStatus_values["Comment"]]})

if "DBRows.csv" not in listdir("D:\\\DB_Odin\\Status\\"):
    OdinStatus2.to_csv("D:\\\DB_Odin\\Status\\DBRows.csv", mode="w",header=False, index= False)
else:
    OdinStatus2.to_csv("D:\\\DB_Odin\\Status\\DBRows.csv", mode="a",header=False, index= False)

Conn_Odin.close()