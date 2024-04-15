import dash
from dash import dcc, html, callback
from dash.dependencies import Input, Output
import pandas as pd
import plotly.express as px

df_with_drop_off_city = pd.DataFrame()
df = pd.DataFrame()
lookup = pd.DataFrame()
datasets_dir = "/opt/airflow/data/"

figLayout = {
    'plot_bgcolor': 'rgba(0, 0, 0, 0)',
    'paper_bgcolor': 'rgba(0, 0, 0, 0)',
    'font': {'color': '#FFFFFF'}
}

def tip_percentage_barplot():
    df_with_tip = df[(df["tip_amount"]>0) & (df["passenger_count"]<7)]
    tip_percentage = 100*df_with_tip["passenger_count"].value_counts()/df["passenger_count"].value_counts()
    tip_percentage.dropna(inplace=True)
    fig = px.bar(
        x=tip_percentage.index,
        y=tip_percentage,
        labels={"x": "Number of Passengers", "y": "Tip Percentage (%)"},
        title="Tip Percentage by Number of Passengers",
        color_discrete_sequence=["#3498db"],  # Change bar color if needed
    )

    fig.update_layout(figLayout)

    graph = dcc.Graph(figure=fig, id='tip-percentage-barplot')  # Added an ID here
    return graph

#_____________________________________________________________________________________________________________________________________________________

def get_drop_off_city(row: pd.Series):
        return str(row["do_location_address"]).split(",")[0]
    

def avg_tip_barplot_fig(remove_ewr: bool):
    global df_with_drop_off_city

    # merging with the lookup table to get the name of the cities instead of the encoded values
    df_with_drop_off_city = df.copy()
    df_with_drop_off_city = df_with_drop_off_city.merge(lookup[lookup["Column name"] == "location"], how='left', left_on="do_location", right_on="Imputed Value")
    df_with_drop_off_city = df_with_drop_off_city.rename(columns={"Original value":"do_location_address"})

    # getting the city from the whole location name
    df_with_drop_off_city["drop_off_city"] = df_with_drop_off_city.apply(get_drop_off_city,axis=1)
    
    # computing the avg tip given per citiy
    avg_tip_per_city = df_with_drop_off_city.groupby("drop_off_city")["tip_amount"].mean()

    # remove the EWR if the checkbox is true
    if remove_ewr == True:
        avg_tip_per_city.drop("EWR", inplace=True) 


    fig = px.bar(
        x=avg_tip_per_city.index,
        y=avg_tip_per_city,
        labels={"x": "Location", "y": "Average Tip Amount ($)"},
        title="Average Tip Amount by Location",
        color_discrete_sequence=["#e74c3c"],
    )
    fig.update_layout(figLayout)

    return fig


def avg_tip_barplot():
    graph = dcc.Graph(id="avg_tip_per_city_graph",figure=avg_tip_barplot_fig(False))
    return graph

#____________________________________________________________________________________________________________________________________________

def avg_tip_per_day():
    # Extracting day from the drop off date attribute
    average_tip_by_day  = df.groupby(pd.to_datetime(df['lpep_dropoff_datetime']).dt.day)["tip_amount"].mean()

    fig = px.line(
        x= average_tip_by_day.index,
        y= average_tip_by_day,
        labels={"x": "Day", "y": "Average Tip Amount ($)"},
        title="Average Tip Amount per Day",
        color_discrete_sequence=["#e74c3c"],
    )
    fig.update_layout(figLayout)
    graph = dcc.Graph(figure=fig)
    return graph

#_________________________________________________________________________________________________________________________________

## Define app layout

def create_dashboard(transformed_csv_filename: str) -> None:
    """
    Creates a dashboard to gain insights about the dataset

    Args:
        transformed_csv_filename: filename of the dataset that is required to visualize
    Returns:
        None
    """
    global df
    global lookup

    df = pd.read_csv(datasets_dir + transformed_csv_filename)
    lookup = pd.read_csv(datasets_dir + "lookup_" + transformed_csv_filename)

    # App layout
    app = dash.Dash()

    # Define colors for dark theme
    colors = {
        'background': '#121724',
        'div': "#192040",
        'text': '#FFFFFF',
        'accent': '#FFFFFF',
    }

    divStyle = {
        "background": colors['div'], "border-radius":"20px", "margin":"10px"
    }

    tripsNumberComponent = html.Div(
        [
            html.H2(len(df)),
            html.H3("Taxi trips in Nov 2017")
        ],
        style={**divStyle,"textAlign":"center","background":"linear-gradient(90deg, rgba(2,0,36,1) 0%, rgba(154,28,149,1) 0%, rgba(98,54,133,1) 63%)","padding":"20px"}
    )

    rightVertical = html.Div(
                [
                    html.Div(
                        [
                            html.H2("Islam Mahmoud Diab | 49-0795 | MET", style={'color': colors['accent']}),
                        ],
                        style={'textAlign': 'center'}
                    ),
                    html.Div(
                        [
                            avg_tip_barplot(),
                            dcc.Checklist(
                                id= "checkbox",
                                options={'remove':'Remove EWR'},
                                style={"padding":"10px"}
                            )
                        ],
                        style=divStyle
                    ),
                    html.Div(
                        [
                            avg_tip_per_day(),
                        ],
                        style=divStyle
                    )
                ],
                style={"width":"70%"}
            )
    
    generalStatisticsTitlesStyle = {
        "color":"#6ca2d3"
    }
    generalStatistics = html.Div(
        [
            html.Div([
                html.H4(f"Avgerage number of passengers per trip", style=generalStatisticsTitlesStyle),
                html.H3(df.passenger_count.mean())
            ]),

            html.Div([
                html.H4(f"Most frequent dropoff Location", style=generalStatisticsTitlesStyle),
                html.H3(df_with_drop_off_city.drop_off_city.mode()),
            ]),

            html.Div([
                html.H4(f"Avergare tip amount per trip", style=generalStatisticsTitlesStyle),
                html.H3(df.tip_amount.mean()),
            ])
        ],
        style={**divStyle, "padding": "10px", "height":"28%", "display":"flex", "flexDirection": "column", "justifyContent": "space-between"}
    )

    leftVertical = html.Div(
                [
                    tripsNumberComponent,
                    generalStatistics,
                    html.Div(
                        [
                            tip_percentage_barplot()
                        ],
                        style=divStyle
                    ),
                ],
                style={"width":"30%", "display":"flex", "flexDirection": "column", "justifyContent": "flex-end"}
            )


    @callback(
        Output(component_id='avg_tip_per_city_graph', component_property='figure'),
        Input(component_id='checkbox', component_property='value')
    )
    def remove_EWR(selected: []):
        if selected != None:
            return avg_tip_barplot_fig("remove" in selected)
        else:
            return avg_tip_barplot_fig(False)

    app.layout = html.Div(
        style={'backgroundColor': colors['background'], 'color': colors['text'], 'display':'flex'},
        children=[
            leftVertical,
            rightVertical
        ]
    )

    app.run_server(host='0.0.0.0', debug=False)