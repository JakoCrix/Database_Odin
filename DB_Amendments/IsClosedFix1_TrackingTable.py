########################
# Change in methodology for database update. Instead of waiting for 3 days of no change, the methodology changed to
# For individual submissions, filter by max number of comments and compare that first max comment row to the last row.
# If there is a deviation of 3 days or more, we are closing that submission. 
########################
# %% Admin
import pandas as pd
from Helper.Source import connect_to_db
Conn_Odin= connect_to_db()

# Extraction
SubmissionTracking_Raw= pd.read_sql_query("SELECT rowid, * FROM Submission_Tracking", Conn_Odin)

SubmissionTracking2= SubmissionTracking_Raw[["rowid","ID_Submission", "LastFetched", "NumComments", "IsClosed"]].copy()
SubmissionTracking2["LastFetched"]= pd.to_datetime(SubmissionTracking2["LastFetched"])
len(SubmissionTracking2)


# %% Tracking Table Amendment
# Checking and minor processing
SubmissionGrpBy = SubmissionTracking2[["ID_Submission", "NumComments", "IsClosed"]].\
    groupby(["ID_Submission"]).agg({"NumComments":["count"],
                                    "IsClosed":["sum"]
                                    }).reset_index()
SubmissionGrpBy.columns= SubmissionGrpBy.columns.droplevel(1)
print("{} out of {} submissions have been closed".format(sum(SubmissionGrpBy["IsClosed"]), len(SubmissionGrpBy)))

SubmissionsToRemove= SubmissionGrpBy[SubmissionGrpBy["IsClosed"]==0].ID_Submission.tolist()
SubmissionTracking3= SubmissionTracking2[SubmissionTracking2["ID_Submission"].isin(SubmissionsToRemove)].copy()

# %% Looping
def Replacing_IsClosed(Submission_Indiv):
    # Submission_Indiv= SubmissionTracking2[SubmissionTracking2["ID_Submission"]=="jun666"]

    Submission_Indiv2= Submission_Indiv.sort_values("LastFetched").copy()
    Submission_Indiv2["TempModified"]=0

    Temp_LastRow= Submission_Indiv2[Submission_Indiv2["NumComments"]==(Submission_Indiv2["NumComments"].max())].iloc[0]["LastFetched"]
    TimeDifference= Submission_Indiv2.iloc[-1]["LastFetched"]- Temp_LastRow

    if TimeDifference > pd.Timedelta(days= 3):
        Submission_Indiv2.at[Submission_Indiv2.index[-1], "TempModified"] = 1
        Submission_Indiv2.at[Submission_Indiv2.index[-1], 'IsClosed'] = 1
    else:
        pass
    Submission_Indiv2[["ID_Submission","LastFetched","NumComments","IsClosed"]]

    return Submission_Indiv2

SubmissionTracking4= pd.DataFrame()
SubmissionList = SubmissionTracking3["ID_Submission"].unique().tolist()

for SubmissionID in range(len(SubmissionList)):
    if SubmissionID % 1000 ==0:
        print("-{} of {} submissions have been Processed".format(SubmissionID,len(SubmissionList)))

    TempSubmission_raw= SubmissionTracking3[SubmissionTracking3["ID_Submission"]==SubmissionList[SubmissionID]]
    TempSubmission_processed= Replacing_IsClosed(TempSubmission_raw)

    SubmissionTracking4= pd.concat([SubmissionTracking4, TempSubmission_processed])

print("")
sum(SubmissionTracking4["TempModified"])



########################
# Requires Database Intervention! Here, we're updating the values in the database.
########################
Conn_Odin= connect_to_db()
cursor   = Conn_Odin.cursor()

SubmissionTracking4= SubmissionTracking3[SubmissionTracking3["TempModified"]==True].reset_index()

for i in range(len(SubmissionTracking4)):
    # i=0
    print("Fixing row {} of {} rows".format(i+1, len(SubmissionTracking4)))
    cursor.execute("Update Submission_Tracking set IsClosed = '{}' where rowid = {}".format(
        SubmissionTracking4["IsClosed"][i],
        SubmissionTracking4["rowid"][i])
    )
    Conn_Odin.commit()

Conn_Odin.close()