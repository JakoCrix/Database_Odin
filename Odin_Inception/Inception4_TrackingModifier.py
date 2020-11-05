# %% Admin
import pandas as pd
from Helper.Source import connect_to_db


# %% Extraction
Conn_Odin= connect_to_db()
OdinDb_CurrentSubmissions = pd.read_sql_query("""SELECT * FROM Submission_Tracking""", Conn_Odin)


# %% Brute Force Manipulations
from Database_Odin.Odin_Inception.Helper1 import Inception_IsClosedModifier
OdinDb_CurrentSubmissions2=OdinDb_CurrentSubmissions.copy()
OdinDb_CurrentSubmissions3= Inception_IsClosedModifier(OdinDb_CurrentSubmissions2,
                                                       DfName_SubmissionID="ID_Submission",
                                                       DfName_NumComments ="NumComments")

# %% Brute Force Deletions
FinalSubmissionFile = OdinDb_CurrentSubmissions3.copy()
FinalSubmissionFile2 = FinalSubmissionFile.reset_index(drop=True)

c=Conn_Odin.cursor()
c.execute("DELETE FROM Submission_Tracking")
FinalSubmissionFile2.to_sql(name="Submission_Tracking", con=Conn_Odin, if_exists='append', index=False)

Conn_Odin.commit()
Conn_Odin.close()
