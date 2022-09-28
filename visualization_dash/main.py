from dash import Dash, html, Input, Output, dcc
import json
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from datetime import timedelta
from read_cassandra import get_cassandra_data

app = Dash(__name__)

sql = "SELECT * FROM status_code"

data = get_cassandra_data(sql)

data["date"] = pd.to_datetime(data["date"])

d_max = max(data["date"])
d_min = min(data["date"])

last_30days = d_max - timedelta(days=30)
last_year = d_max - timedelta(weeks=52)

app.layout = \
    html.Div([
        html.Div([
            html.H1("Log file Analyzer", style={"position": "absolute", "height": "70px",
                                                "width": "100%", "top": "20px",
                                                "textAlign": "center", "color": "#383838"})
        ], style={"backgroundColor": "#FFA63E", "position": "absolute",
                  "height": "113px", "width": "100%",
                  "top": "0px"}),
        html.Div([
            html.Div([dcc.Dropdown(["Googlebot Desktop", "Googlebot Smartphone", "Bingbot Desktop", "All Bots"],
                                   id="select_bot",
                                   value="All Bots", multi=False, clearable=False, style={"color": "#383838"}),
                      ],
                     style={"backgroundColor": "#23208A", "position": "absolute",
                            "height": "33px", "width": "8%",
                            "top": "150px", "left": "210px"}),
            html.Div([dcc.Dropdown(options=([
                {"label": f"Today ({pd.to_datetime(d_max).date()})",
                 "value": "Today"},
                {"label": f"Past 30days ({pd.to_datetime(last_30days).date()} - {pd.to_datetime(d_max).date()})",
                 "value": "30days"},
                {"label": f"Past year ({pd.to_datetime(last_year).date()} - {pd.to_datetime(d_max).date()})",
                 "value": "year"}
            ]),
                id="select_date",
                value="Today", multi=False, clearable=False, style={"color": "#383838"}), ],
                style={"backgroundColor": "#D9D9D9", "position": "absolute",
                       "height": "33px", "width": "12%",
                       "top": "150px", "left": "550px"}),

            html.Div(style={"backgroundColor": "#C4C4C4", "position": "absolute",
                            "height": "33px", "width": "12%",
                            "top": "250px", "left": "210px"})
        ]),
        html.Div([
            html.Div([dcc.Graph(id="status_code_bar")], style={"backgroundColor": "#C4C4C4", "position": "absolute",
                                                               "height": "522px", "width": "55%",
                                                               "top": "299px", "left": "210px",
                                                               "boxShadow": "0px 1px 4px rgba(0, 0, 0, 0.25)"}),
            html.Div([dcc.Graph(id="status_pie_chart"), ],
                     style={"backgroundColor": "#C4C4C4", "position": "absolute",
                            "height": "220px", "width": "22.9%",
                            "top": "299px", "left": "1575px",
                            "boxShadow": "0px 1px 4px rgba(0, 0, 0, 0.25)"}),
            html.Div([dcc.Graph(id="filetype_chart"), ],
                     style={"backgroundColor": "#C4C4C4", "position": "absolute",
                            "height": "220px", "width": "22.9%",
                            "top": "600px", "left": "1575px", "boxShadow": "0px 1px 4px rgba(0, 0, 0, 0.25)"}),

        ], ),

        dcc.Store(id="store_data")
    ],

        # style={"backgroundColor": "#7FDBFF"}
    )


@app.callback(
    # Output('status_pie_chart', 'figure'),
    Output('select_date', 'options'),
    Input('select_date', 'value')
)
def update_date(date_input):
    stm_sql = "SELECT * FROM status_code"

    cs_data = get_cassandra_data(sql)

    cs_data["date"] = pd.to_datetime(cs_data["date"])

    update_max = max(cs_data["date"])
    update_min = min(cs_data["date"])

    update_last_30days = update_max - timedelta(days=30)
    update_last_year = d_max - timedelta(weeks=52)

    return [
        {"label": f"Today ({pd.to_datetime(update_max).date()})",
         "value": "Today"},
        {"label": f"Past 30days ({pd.to_datetime(update_last_30days).date()} - {pd.to_datetime(update_max).date()})",
         "value": "30days"},
        {"label": f"Past year ({pd.to_datetime(update_last_year).date()} - {pd.to_datetime(update_max).date()})",
         "value": "year"}
    ]


@app.callback(
    Output('store_data', 'data'),
    Input('select_bot', 'value'),
    Input('select_date', 'value')
)
def get_data(bot, date):
    sql_stmt = "SELECT * FROM status_code"
    c_data = get_cassandra_data(sql_stmt)

    status_df = c_data[c_data["crawler"] == bot]

    if bot == "All Bots":
        status_df = c_data

    status_df["date"] = pd.to_datetime(status_df["date"])

    update_max = max(status_df["date"])
    update_min = min(status_df["date"])

    update_last_30days = update_max - timedelta(days=30)
    update_last_year = d_max - timedelta(weeks=52)
    # comp_df = ""

    status_df_pie = ""
    if date == "Today":
        status_df_pie = status_df[status_df["date"] == pd.to_datetime(d_max)]

    elif date == "30days":
        status_df_pie = status_df[
            (status_df["date"] >= pd.to_datetime(last_30days)) & (status_df["date"] <= pd.to_datetime(d_max))]

    if date == "year":
        status_df_pie = status_df[
            (status_df["date"] >= pd.to_datetime(update_last_year)) & (status_df["date"] <= pd.to_datetime(d_max))]
    print("new_gran")
    print(status_df_pie)

    data_sets = {
        "status_df": status_df_pie.to_json(orient="split", date_format="iso")
    }

    return json.dumps(data_sets)


@app.callback(
    Output('status_pie_chart', 'figure'),
    Output('filetype_chart', 'figure'),
    Input('store_data', 'data')
)
def update_status_code_pie(needed_data):
    colors = ["#23208A", "#BC0D68", "#6494FF", "#6494FF"]

    raw_status_data = json.loads(needed_data)
    ready_status_data = pd.read_json(raw_status_data["status_df"], orient="split")
    comp_hourly_df = ready_status_data.groupby(["status"]).agg({"count": "sum"}).reset_index()

    # fig = px.pie(data_frame=comp_hourly_df, names="status", hole=.5, hover_data="count", col)
    # fig.udate_traces()

    fig = go.Figure(data=[go.Pie(labels=comp_hourly_df["status"], values=comp_hourly_df["count"], hole=0.4, )])
    fig.update_layout(margin=dict(t=50, b=15, l=0, r=0, pad=4), title_text="Status code", height=220)
    fig.update_traces(hoverinfo="label+percent", textinfo="value", textfont_size=10, marker=dict(colors=colors))

    return fig, fig


@app.callback(
    Output('status_code_bar', 'figure'),
    Input('store_data', 'data')
)
def update_status_code_line(needed_data):
    colors = ["#23208A", "#BC0D68", "#6494FF", "#6494FF"]
    raw_status_data = json.loads(needed_data)
    ready_status_data = pd.read_json(raw_status_data["status_df"], orient="split")

    comp_hourly_df = ready_status_data.groupby(["status", "hour"]).agg({"count": "sum"}).reset_index()

    fig = px.scatter(comp_hourly_df, x="hour", y="count", color="status")
    fig.update_layout(
        xaxis=dict(
            showline=True,
            showgrid=False,
            showticklabels=True,
            linecolor="rgb(204, 204, 204)",
            linewidth=2,
            ticks='outside',
            fixedrange=True,

        ),
        yaxis=dict(
            # showgrid=False,
            # zeroline=True,
            linecolor="rgb(204, 204, 204)",
            linewidth=2,
            fixedrange=True,
            showline=False,
            showticklabels=True,
            showgrid=True,
            gridcolor="#D9D9D9"),
        plot_bgcolor='white',
        margin=dict(t=50, b=15, l=0, r=0, pad=4),
        title_text="Status code",
        height=522,
        legend=dict(
            # Adjust click behavior
            itemclick="toggleothers",
            itemdoubleclick="toggle",
        )
    )

    return fig


if __name__ == "__main__":
    app.run(port=3032, debug=True)
