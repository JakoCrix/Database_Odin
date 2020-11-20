#######################################
# Table
# To run this app: python C:\Users\Andrew\Documents\GitHub\Database_Odin\DB_Upkeep\app2.py
#######################################

# %% Admin
import dash
import dash_table
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

import sys
sys.path.append('C:\\Users\\Andrew\\Documents\\GitHub\\Database_Odin')

# %% Data Extraction
from Helper.Source import connect_to_db
from DB_Upkeep.AppHelpers.Helper1_OdinSubmissions import AllSubmissions_Table
conn_Odin= connect_to_db()
SubmissionsTable= AllSubmissions_Table(conn_Odin)

# %% Dash layout
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div([
    html.H3("Welcome to Odin's Upkeep!"),

    html.Label('Subreddit of interest'),
    dcc.Dropdown(id='SubredditOfInterest',
                 options=[{'label': 'Stock_Picks', 'value': 'Stock_Picks'},
                          {'label': 'SecurityAnalysis', 'value': 'SecurityAnalysis'},
                          {'label': 'investing', 'value': 'investing'},
                          {'label': 'stocks', 'value': 'stocks'},
                          {'label': 'wallstreetbets', 'value': 'wallstreetbets'},
                          {'label': 'pennystocks', 'value': 'pennystocks'}],
                 value='stocks'),
    html.Div(id='my-output'),
    html.Br(),

    dash_table.DataTable(
        id='Subreddit_Submissions',
        columns=[{"name": i, "id": i} for i in SubmissionsTable.columns],
        page_action= "none",
        style_cell={'overflow': 'hidden',
                    'textOverflow': 'ellipsis',
                    'maxWidth': 0
                    }
    )
])

@app.callback(
    dash.dependencies.Output(component_id='Subreddit_Submissions', component_property='data'),
    [dash.dependencies.Input(component_id='SubredditOfInterest',component_property= 'value')]
)

def FilterSubmissions(value):
    # input_value= "stocks"
    SubmissionsTable2= SubmissionsTable.copy()
    SubmissionsTable3= SubmissionsTable2[SubmissionsTable2["Subreddit"]==value]
    SubmissionsTable4= SubmissionsTable3.sort_values("CreatedDate", ascending= False)

    return SubmissionsTable4.to_dict("records")

if __name__ == '__main__':
    app.run_server(debug=True)