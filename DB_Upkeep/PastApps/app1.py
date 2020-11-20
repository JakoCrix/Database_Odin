#######################################
# Table
# To run this app: python C:\Users\Andrew\Documents\GitHub\Database_Odin\DB_Upkeep\app1.py
#######################################

# %% Admin
import dash
import dash_table
import sys
sys.path.append('/')

# Data Extraction
from Helper.Source import connect_to_db
from DB_Upkeep.AppHelpers.Helper1_OdinSubmissions import AllSubmissions_Table
conn_Odin= connect_to_db()
SubmissionsTable= AllSubmissions_Table(conn_Odin)

# %% Dash layout
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = dash_table.DataTable(
    data=SubmissionsTable.to_dict('records'),
    columns=[{"name": i, "id": i} for i in SubmissionsTable.columns],
    page_action= "none",
    style_cell={'overflow': 'hidden',
                'textOverflow': 'ellipsis',
                'maxWidth': 0
                },
    tooltip_data=[
        {column: {'value': str(value), 'type': 'markdown'}
         for column, value in row.items()
         } for row in SubmissionsTable.to_dict('rows')
    ],
    tooltip_duration=None
)

if __name__ == '__main__':
    app.run_server(debug=True)