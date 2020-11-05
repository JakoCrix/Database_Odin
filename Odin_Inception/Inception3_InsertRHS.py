# %% Admin
import pandas as pd
import time

from Helper.RedditExtraction_Comments import DG_Comments
from Helper.Odin_Insertion import OdinInsert_CommentInformation, OdinInsert_Comment
from Helper.Source import connect_to_db

# %% Database Insertion- RHS
Conn_Odin= connect_to_db()
OdinDb_CurrentSubmissions = pd.read_sql_query("""SELECT ID_Submission FROM Submission_Tracking""", Conn_Odin)
Submission_List= OdinDb_CurrentSubmissions.ID_Submission.unique()


# %% Extraction
for SubmissionIndex in range(len(Submission_List)):
# for SubmissionIndex in range(1505, len(Submission_List)):
    # SubmissionIndex=0
    print("____"*20)
    print("Inserting Comments for Submission {} of {}".format(SubmissionIndex+1, len(Submission_List)))

    # Extraction
    try:
        Temp_Comments = DG_Comments(Submission_List[SubmissionIndex])
    except TimeoutError:
        print("_Caught a timeout! Sleeping for 10 seconds before retrying. ")
        time.sleep(10)
        Temp_Comments = DG_Comments(Submission_List[SubmissionIndex])
    except Exception:
        print("_Error with extraction, logging as an error and passing. ")
        # TODO: record the error message
        pass

    # Insertion
    OdinInsert_CommentInformation(Conn_Odin, Submission_List[SubmissionIndex], Temp_Comments)
    OdinInsert_Comment(Conn_Odin,Submission_List[SubmissionIndex], Temp_Comments)


Conn_Odin.close()
