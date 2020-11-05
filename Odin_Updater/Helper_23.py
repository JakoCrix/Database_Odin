# %% Admin
import pandas as pd

# %%
def OdinExtraction_Comments(conn_Object, RedditSubmissions_List):
    # RedditSubmissions_List= TempExtract_List
    # Extraction
    list(RedditSubmissions_List)
    Temp_RedditSubmissions="','".join(list(RedditSubmissions_List))
    Temp_RedditSubmissions2= "'"+Temp_RedditSubmissions+"'"

    TempSQL_1 = """SELECT ID_Submission, ID_Comment FROM Comment_Information WHERE ID_Submission IN ({})""".format(Temp_RedditSubmissions2)
    CommentID_Submission= pd.read_sql_query(TempSQL_1, conn_Object)

    return CommentID_Submission


def Comments_ExistenceCheck(RedditExtraction_Submission, OdinExtraction_Submission):
    # RedditExtraction_Submission= Temp_Comments; OdinExtraction_Submission= Temp_OdinExtraction
    CommentsToRemove= RedditExtraction_Submission["Comment_ID"].isin(OdinExtraction_Submission["ID_Comment"])==False
    Comments_NonExistence= RedditExtraction_Submission[CommentsToRemove]
    print("_{} of {} comments extracted already exists in Odin".format(len(RedditExtraction_Submission)-len(Comments_NonExistence),
                                                                      len(RedditExtraction_Submission)))

    return Comments_NonExistence
