########################
# We have some submissions that didn't get updated properly in the tracking table.
# Because of this, they didn't get close properly and are still open.
########################
# %% Admin
import pandas as pd
from Helper.Source import connect_to_db
import datetime
Conn_Odin= connect_to_db()

# Extraction
SubmissionTracking_Raw= pd.read_sql_query("SELECT rowid, * FROM Submission_Tracking", Conn_Odin)

SubmissionTracking2= SubmissionTracking_Raw[["rowid","ID_Submission", "LastFetched", "NumComments", "IsClosed"]].copy()
SubmissionTracking2["LastFetched"]= pd.to_datetime(SubmissionTracking2["LastFetched"])
len(SubmissionTracking2)

# %% Tracking Table Amendment
Submissions_Closed= SubmissionTracking2[SubmissionTracking2["IsClosed"]==1].copy()
Submissions_Closed_List = Submissions_Closed["ID_Submission"].unique().tolist()

SubmissionTracking2= SubmissionTracking2.sort_values(by= ["ID_Submission", "LastFetched"])
SubmissionTracking3= SubmissionTracking2[(SubmissionTracking2["ID_Submission"].isin(Submissions_Closed_List))==False].copy()

GrpbyCheck= SubmissionTracking3[["ID_Submission", "IsClosed"]].groupby(["ID_Submission"]).\
    agg({"IsClosed":["count"]}).reset_index()
GrpbyCheck.columns= GrpbyCheck.columns.droplevel(1)
GrpbyCheck.sort_values("IsClosed")

SubmissionTracking4= SubmissionTracking3.groupby(["ID_Submission"]).\
    agg({"rowid":["last"],
         "LastFetched": ["last"],
         "NumComments": ["last"],
         "IsClosed": ["last"]}).reset_index()
SubmissionTracking4.columns= SubmissionTracking4.columns.droplevel(1)
SubmissionTracking4.sort_values("LastFetched")


# %% Closing Submissions
Date_Limit = pd.Timestamp('2020/11/15')

SubmissionTracking5= SubmissionTracking4.copy()
SubmissionTracking5["TempModified"]= 0
SubmissionTracking5.loc[SubmissionTracking5["LastFetched"] <= Date_Limit, ["IsClosed", "TempModified"]]=1

########################
# Requires Database Intervention! Here, we're updating the values in the database.
########################
Conn_Odin= connect_to_db()
cursor   = Conn_Odin.cursor()

SubmissionTracking6= SubmissionTracking5[SubmissionTracking5["TempModified"]==1].copy()
SubmissionTracking6= SubmissionTracking6.reset_index(drop=True)

for i in range(len(SubmissionTracking6)):
    # i=0
    print("Fixing row {} of {} rows".format(i+1, len(SubmissionTracking6)))
    cursor.execute("Update Submission_Tracking set IsClosed = '{}' where rowid = {}".format(
        SubmissionTracking6["IsClosed"][i],
        SubmissionTracking6["rowid"][i])
    )
    Conn_Odin.commit()

Conn_Odin.close()