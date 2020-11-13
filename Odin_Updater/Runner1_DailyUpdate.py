# %% Admin
from datetime import datetime
import pandas as pd
from Helper.Source import connect_to_reddit

Path_DailyFiles= "C:\\Users\\Andrew\\Documents\\GitHub\\Database_Odin\\DailyFiles\\"

# %% Function Creation
def TrackingSubmissions(Subreddit_Name, MinimumComments = 30):
    # Subreddit_Name="stocks"; MinimumComments=25
    reddit=connect_to_reddit()
    subreddit = reddit.subreddit(Subreddit_Name)
    AllPosts = subreddit.new(limit = 2000)

    # Static Information Creations
    TempList_SubredditID=[]
    TempList_Subreddit=[]

    TempList_SubmissionID=[]
    TempList_CreatedDate=[]
    TempList_Title=[]
    TempList_URL= []

    Temp_SubredditName = subreddit.display_name
    Temp_SubredditID = subreddit.id

    # Dynamic Information Creations
    TempList_LastFetched= []                # Insert this date ourself
    TempList_NumComments = []
    TempList_Score=[]
    TempList_UpvoteRatio=[]

    # Extraction
    temp_SubmissionCount = 0
    temp_SubmissionHaveContent = 0
    temp_SubmissionLackContent = 0

    for submission in AllPosts:
        # submission=reddit.submission("j2gc7k")
        temp_SubmissionCount+=1

        try:
            if submission.num_comments >= MinimumComments:
                # Info Extraction
                TempList_SubredditID.append(Temp_SubredditID)       # Subreddit Info
                TempList_Subreddit.append(Temp_SubredditName)                # Takes too long

                TempList_SubmissionID.append(submission.id)                # Submission- Fixed Info
                TempList_CreatedDate.append(
                    datetime.utcfromtimestamp(int(submission.created_utc)).strftime('%Y-%m-%d %H:%M'))
                TempList_Title.append(submission.title)
                TempList_URL.append(submission.url)

                TempList_LastFetched.append(
                    datetime.now().strftime('%Y-%m-%d %H:%M'))          # Submission- Dynamic Info
                TempList_NumComments.append(submission.num_comments)
                TempList_Score.append(submission.score)
                TempList_UpvoteRatio.append(submission.upvote_ratio)

                # Tracking
                temp_SubmissionHaveContent += 1
                if temp_SubmissionCount % 100==0:
                    print("In subreddit {} of {} comments: {} has content and {} lacks content".format(Subreddit_Name,
                                                                                                       temp_SubmissionCount,
                                                                                                       temp_SubmissionHaveContent,
                                                                                                       temp_SubmissionLackContent))
            else:
                # Tracking
                temp_SubmissionLackContent += 1
                if temp_SubmissionCount % 100==0:
                    print("In subreddit {} of {} comments: {} has content and {} lacks content".format(Subreddit_Name,
                                                                                                       temp_SubmissionCount,
                                                                                                       temp_SubmissionHaveContent,
                                                                                                       temp_SubmissionLackContent))
        except:
            pass

    # Saving
    Df = pd.DataFrame({"Subreddit_ID": TempList_SubredditID[::-1],
                       "Subreddit_Name": TempList_Subreddit[::-1],

                       "Submission_ID": TempList_SubmissionID[::-1],
                       "Submission_CreatedDate": TempList_CreatedDate[::-1],
                       "Submission_Title": TempList_Title[::-1],
                       "Submission_URL": TempList_URL[::-1],

                       "Submission_LastFetched": TempList_LastFetched[::-1],
                       "Submission_NumComments": TempList_NumComments[::-1],
                       "Submission_Score": TempList_Score[::-1],
                       "Submission_UpvoteRatio": TempList_UpvoteRatio[::-1],
                       })

    return Df

# %% Actual Extraction
SubmissionDf_stocks = TrackingSubmissions(Subreddit_Name= "stocks",MinimumComments = 20)
SubmissionDf_investing = TrackingSubmissions(Subreddit_Name= "investing",MinimumComments = 20)
SubmissionDf_wallstreetbets = TrackingSubmissions(Subreddit_Name= "wallstreetbets",MinimumComments = 20)
SubmissionDf_stockpicks = TrackingSubmissions(Subreddit_Name= "Stock_Picks",MinimumComments = 20)
SubmissionDf_securityanalysis = TrackingSubmissions(Subreddit_Name= "SecurityAnalysis",MinimumComments = 20)
SubmissionDf_pennystocks = TrackingSubmissions(Subreddit_Name= "pennystocks",MinimumComments = 20)

SubmissionDf = pd.DataFrame()
SubmissionDf = SubmissionDf.append(SubmissionDf_stocks)
SubmissionDf = SubmissionDf.append(SubmissionDf_investing)
SubmissionDf = SubmissionDf.append(SubmissionDf_wallstreetbets)
SubmissionDf = SubmissionDf.append(SubmissionDf_stockpicks)
SubmissionDf = SubmissionDf.append(SubmissionDf_securityanalysis)
SubmissionDf = SubmissionDf.append(SubmissionDf_pennystocks)


SubmissionDf.to_csv(Path_DailyFiles+"SubmissionFile_"+\
                    str(datetime.date(datetime.now())).replace("-","")+".csv",
                    index=False) # Temporary Saving

