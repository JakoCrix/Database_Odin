# %% Admin
import pandas as pd
import time

import sys
sys.path.append('C:\\Users\\Andrew\\Documents\\GitHub\\Database_Odin')

# %% Database Check
from Helper.Source import connect_to_db
Conn_Odin = connect_to_db()

#  List of files
from Odin_Updater.Helper_21 import SubmissionFiles_Relevance
Path_DailyFiles= "C:\\Users\\Andrew\\Documents\\GitHub\\Database_Odin\\DailyFiles\\"
SubmissionCsv_List = SubmissionFiles_Relevance(Conn_Odin, Path_DailyFiles= Path_DailyFiles)
Conn_Odin.close()

# %% Extraction Phase
for SubmissionCsv_File in SubmissionCsv_List:
    # SubmissionCsv_File= SubmissionCsv_List[0]
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
    from Odin_Updater.Helper_22v2 import *

    # LHS Manipulation
    SubmissionFile_LHSProcess1_1 = SubmissionFileModify1_Exclusions(Conn_Odin, SubmissionFile)
    SubmissionFile_LHSProcess1_2 = SubmissionFile_LHSProcess1_1[SubmissionFile_LHSProcess1_1["Prcs1_AlreadyClosed"]==0].copy()
    SubmissionFile_LHSProcess2_1 = SubmissionFileModify2_PotentialClosures(Conn_Odin, SubmissionFile_LHSProcess1_2)
    SubmissionFile_LHSProcess2_2 = SubmissionFile_LHSProcess2_1.copy()
    SubmissionFile_LHSProcess2_2["IsClosed"] = SubmissionFile_LHSProcess2_1["Prcs2_WillClose"]
    SubmissionFile_LHSProcess2_3 = SubmissionFile_LHSProcess2_2.drop(["Prcs1_AlreadyClosed", "Prcs2_WillClose"], axis = 1).copy()

    SubmissionFile_LHSInsert= SubmissionFile_LHSProcess2_3.copy()   # For LHS DB Insertion

    # RHS Manipulation
    SubmissionFile_RHSProcess3_1 = Submissions_ExtractionList(Conn_Odin, SubmissionFile_LHSInsert)

    SubmissionFile_RHSInsert= SubmissionFile_RHSProcess3_1.copy()   # For RHS DB Submission Insertions

    ###################################################################
    # Database Insertion
    from Odin_Updater.Helper_23 import *
    from Helper.RedditExtraction_Comments import DG_Comments
    from Helper.Odin_Insertion import OdinInsert_CommentInformation, OdinInsert_Comment

    # Loop Preparations
    SubmissionComments_Df = OdinExtraction_Comments(Conn_Odin, SubmissionFile_RHSInsert)

     # Database Insertion- RHS
    for SubmissionIndex in range(len(SubmissionFile_RHSInsert)):
        # SubmissionIndex=0
        print("____" * 20)
        print("Inserting Comments for Submission {} of {}".format(SubmissionIndex + 1, len(SubmissionFile_RHSInsert)))

        # Extraction
        try:
            Temp_Comments = DG_Comments(SubmissionFile_RHSInsert[SubmissionIndex])
        except TimeoutError:
            print("_Caught a timeout! Sleeping for 10 seconds before retrying. ")
            time.sleep(10)
            Temp_Comments = DG_Comments(SubmissionFile_RHSInsert[SubmissionIndex])
        except Exception:
            print("_Error with extraction, logging as an error and passing. ")
            pass

        # Extraction Cleanup
        Temp_OdinExtraction= SubmissionComments_Df[SubmissionComments_Df["ID_Submission"] == SubmissionFile_RHSInsert[SubmissionIndex]]
        Temp_Comments2= Comments_ExistenceCheck(Temp_Comments, Temp_OdinExtraction)

        # Insertion
        if Temp_Comments2.empty:
            pass
        else:
            OdinInsert_CommentInformation(Conn_Odin, SubmissionFile_RHSInsert[SubmissionIndex], Temp_Comments2)
            OdinInsert_Comment(Conn_Odin, Temp_Comments2)
    print("____" * 20)

    # Database Insertion- LHS
    from Helper.Odin_Insertion import OdinInsert_SubmissionTracking, OdinInsert_SubredditInfo, OdinInsert_SubmissionInfo

    OdinInsert_SubmissionTracking(Conn_Odin, SubmissionFile_LHSInsert)
    OdinInsert_SubredditInfo(Conn_Odin,      SubmissionFile_LHSInsert)
    OdinInsert_SubmissionInfo(Conn_Odin,     SubmissionFile_LHSInsert)

    Conn_Odin.close()
