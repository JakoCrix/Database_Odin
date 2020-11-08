# %% Admin
import pandas as pd
import time

from Helper.Source import connect_to_db
from Odin_Updater.Helper_21 import SubmissionFiles_Relevance

Path_DailyFiles= "C:\\Users\\Andrew\\Documents\\GitHub\\Database_Odin\\DailyFiles\\"


# %% List of files
Conn_Odin = connect_to_db()
SubmissionCsv_List = SubmissionFiles_Relevance(Conn_Odin,Path_DailyFiles= Path_DailyFiles)

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
    from Odin_Updater.Helper_22 import SubmissionFileModify1_Exclusions, SubmissionFileModify2_PotentialClosures

    SubmissionFile_Processed1 = SubmissionFileModify1_Exclusions(Conn_Odin, SubmissionFile)
    SubmissionFile_Processed2 = SubmissionFileModify2_PotentialClosures(Conn_Odin,
                                                                        SubmissionFile_Processed1,
                                                                        DfName_SubmissionID="Submission_ID",
                                                                        DfName_NumComments= "Submission_NumComments")
    SubmissionFile_ExtractionList = SubmissionFile_Processed2[SubmissionFile_Processed2["IsClosed"] == False]

    ###################################################################
    # Database Insertion
    from Odin_Updater.Helper_23 import OdinExtraction_Comments

    TempExtract_List = SubmissionFile_ExtractionList["Submission_ID"]
    SubmissionComments_Df = OdinExtraction_Comments(Conn_Odin, TempExtract_List)

    ###################################################################
    # %% Database Insertion- RHS
    # RHS
    from Helper.RedditExtraction_Comments import DG_Comments
    from Helper.Odin_Insertion import OdinInsert_CommentInformation, OdinInsert_Comment
    from Odin_Updater.Helper_23 import Comments_ExistenceCheck

    Submission_List = SubmissionFile_ExtractionList.Submission_ID.unique()

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


    # %% Database Insertion- LHS
    from Helper.Odin_Insertion import OdinInsert_SubmissionTracking, OdinInsert_SubredditInfo, OdinInsert_SubmissionInfo

    OdinInsert_SubmissionTracking(Conn_Odin, SubmissionFile_ExtractionList)
    OdinInsert_SubredditInfo(Conn_Odin, SubmissionFile_ExtractionList)
    OdinInsert_SubmissionInfo(Conn_Odin, SubmissionFile_ExtractionList)

    Conn_Odin.close()






