import sqlite3
import praw

# %% Odin
# For consistency, name the output file Conn_Odin
def connect_to_db(Path_DB="D:\\DB_Odin\\Odin.db"):
    OdinConnect = sqlite3.connect(Path_DB)
    print("Succesfully connected to Odin. ")


    return(OdinConnect)

# %% Reddit
def connect_to_reddit(Path_AuthenticationInfo= "C:\\Users\\Andrew\\Documents\\GitHub\\"):
    # Path_AuthenticationInfo= "C:\\Users\\Andrew\\Documents\\GitHub\\"
    csvFile= open(Path_AuthenticationInfo+"Connections.txt", "r")
    ConnectionDetails= csvFile.read()
    csvFile.close()

    # ClientID
    ClientID_StrStart= ConnectionDetails.find("client_id=\"")+ len("client_id=\"")
    ClientID_StrEnd=   ConnectionDetails.find("\", client_secret")
    ClientID= ConnectionDetails[ClientID_StrStart:ClientID_StrEnd].replace("'","")
    # ClientID
    ClientSecret_StrStart= ConnectionDetails.find("client_secret=\"")+ len("client_secret=\"")
    ClientSecret_StrEnd=   ConnectionDetails.find("\", username")
    ClientSecret= ConnectionDetails[ClientSecret_StrStart:ClientSecret_StrEnd].replace("'","")
    # username
    Username_StrStart= ConnectionDetails.find("username=\"")+ len("username=\"")
    Username_StrEnd=   ConnectionDetails.find("\", password")
    Username= ConnectionDetails[Username_StrStart:Username_StrEnd].replace("'","")
    # Password
    Password_StrStart= ConnectionDetails.find("password=\"")+ len("password=\"")
    Password_StrEnd=   ConnectionDetails.find("\", user_agent")
    Password= ConnectionDetails[Password_StrStart:Password_StrEnd].replace("'","")
    # User Agent
    UserAgent_StrStart= ConnectionDetails.find("user_agent=\"")+ len("user_agent=\"")
    UserAgent_StrEnd=   len(ConnectionDetails)
    UserAgent= ConnectionDetails[UserAgent_StrStart:UserAgent_StrEnd].replace("'","")

    # Connection
    reddit_Connection = praw.Reddit(client_id=ClientID,
                                    client_secret= ClientSecret,
                                    username=Username,
                                    password=Password,
                                    user_agent=UserAgent)
    return(reddit_Connection)