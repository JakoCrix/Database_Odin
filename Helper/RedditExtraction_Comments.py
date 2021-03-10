# %% Admin
import praw
from datetime import datetime
import pandas as pd
import re

def DG_Comments(SubmissionID_str):
    # %% Admin
    # SubmissionID_str = "ly92v2"

    reddit = praw.Reddit(client_id="d-nH_kadZ1Db4w", client_secret="GyYHEjL8cCnYByCHgb6AMBEjAWo",
                         username="EnigmaticWebcrawler", password="2d6cbbb99b", user_agent="prawtutorialv1")
    submission = reddit.submission(id=SubmissionID_str)

    print("_Scraping {} with title: '{}'".format(submission.id,submission.title))
    StartTime = datetime.now()
    submission.comments.replace_more(limit=150)
    print("_Submission object refreshed taking: {} seconds".format((datetime.now()- StartTime).total_seconds()))

    # %% Variable Creation
    TempList_CommentID = []
    TempList_ParentID = []
    TempList_SubmissionID= []
    TempList_CommentCreationDate=[]
    TempList_Stickied =[]
    TempList_CommentBody = []

    # %% Extraction
    CommentsConsolidated = 0

    for comment in submission.comments.list():
        # comment= reddit.comment(id="g4ln2d0")
        CommentsConsolidated += 1
        if comment.body=="[deleted]":
            pass
        else:
            TempList_CommentID.append(comment.id)
            TempList_ParentID.append(re.sub("^t._", "", comment.parent_id))
            TempList_SubmissionID.append(SubmissionID_str)
            TempList_CommentCreationDate.append(datetime.utcfromtimestamp(int(comment.created_utc)).strftime('%Y-%m-%d %H:%M:%S'))
            TempList_Stickied.append(comment.stickied)

            TempList_CommentBody.append(comment.body)

    # Saving
    Df = pd.DataFrame({"Comment_ID":TempList_CommentID,
                       "Comment_IDParent":TempList_ParentID,
                       "Comment_IDSubmission":TempList_SubmissionID,
                       "Comment_Time": TempList_CommentCreationDate,
                       "Comment_Stickied": TempList_Stickied,
                       "Comment_Body": TempList_CommentBody}
                      )

    print("_Scraping complete, extracted {} of {} comments".format(str(CommentsConsolidated),
                                                                   submission.num_comments)
          )

    return(Df)

# x=CommentExtract_Submission("ipp3e9")
