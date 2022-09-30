import datetime

from dash import Dash, html, Input, Output, dcc, dash_table
import json
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from datetime import timedelta
from read_cassandra import get_cassandra_data

app = Dash(__name__)

sql = "SELECT * FROM status_code"

crawler_columns = ["Top directory", "Total Bot Hits"]

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
            html.Div([], style={"backgroundColor": "#C4C4C4", "position": "absolute",
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

        dcc.Store(id="store_data"),
        html.Div([
            html.Div(
                [dcc.Graph(id="status_code_bar")], style={"backgroundColor": "#C4C4C4", "position": "absolute",
                           "height": "522px", "width": "55%",
                           "top": "859px", "left": "210px",
                           "boxShadow": "0px 1px 4px rgba(0, 0, 0, 0.25)"}
            ),

            html.Div(
                [], style={"backgroundColor": "#C4C4C4", "position": "absolute",
                           "height": "522px", "width": "55%",
                           "top": "1420px", "left": "210px",
                           "boxShadow": "0px 1px 4px rgba(0, 0, 0, 0.25)"}
            ),

            html.Div(
                [html.Table(dash_table.DataTable(
                    id="crawler_table",
                    columns=[{"name": name, "id": name} for name in crawler_columns],
                    style_data=dict(backgroundColor="#FFFFF"),
                    style_table=dict(top="0px", width="248%", line_color="#FFA63E", textAlign="center",
                                     boxShadow="0px 0px 0px rgba(0, 0, 0, 0.25)"),
                    style_header=dict(color="white", backgroundColor="#FFA63E", fontWeight="bold", textAlign="center")
                ))], style={"backgroundColor": "#FFFFF", "position": "absolute",
                            "height": "600px", "width": "22.9%",
                            "top": "859px", "left": "1575px",
                            "boxShadow": "0px 1px 4px rgba(0, 0, 0, 0.25)"}
            )
        ])
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
    sql_stmt = "SELECT * FROM status_code;"
    crawler_freq_stmt = "SELECT * FROM crawler_frequency;"
    file_type_stmt = "SELECT * FROM file_type;"

    crawler_freq_data = get_cassandra_data(crawler_freq_stmt)
    file_type_data = get_cassandra_data(file_type_stmt)
    # print(file_type_data)
    c_data = get_cassandra_data(sql_stmt)

    c_data["status"] = c_data["status"].astype("str")
    crawler_df = crawler_freq_data[crawler_freq_data["crawler"] == bot]
    status_df = c_data[c_data["crawler"] == bot]
    file_type_df = file_type_data[file_type_data["crawler"] == bot]

    if bot == "All Bots":
        status_df = c_data
        crawler_df = crawler_freq_data
        file_type_df = file_type_data

    status_df["date"] = pd.to_datetime(status_df["date"])
    crawler_df["date"] = pd.to_datetime(crawler_df["date"])
    file_type_df["date"] = pd.to_datetime(file_type_df["date"])
    print(file_type_df)
    update_max = max(status_df["date"])
    update_min = min(status_df["date"])

    update_last_30days = update_max - timedelta(days=30)
    update_last_year = d_max - timedelta(weeks=52)
    # comp_df = ""

    status_df_pie = ""
    crawler_df_gran = ""
    file_type_df_pie = ""

    # specify the period of data needed, and get the total frequency for that period
    if date == "Today":
        status_df_pie = status_df[status_df["date"] == pd.to_datetime(d_max)]
        status_df_pie = status_df_pie.groupby(["status", "hour"]).agg({"frequency": "sum"}).reset_index()

        file_type_df_pie = file_type_df[file_type_df["date"] == pd.to_datetime(d_max)]
        print(file_type_df)
        file_type_df_pie = file_type_df_pie.groupby(["file_type", "hour"]).agg({"frequency": "sum"}).reset_index()

        crawler_df_gran = crawler_df[crawler_df["date"] == pd.to_datetime(d_max)]
        crawler_df_gran = crawler_df_gran.groupby(["top_directory"]).agg({"frequency": "sum"}).reset_index()

    elif date == "30days":
        status_df_pie = status_df[
            (status_df["date"] >= pd.to_datetime(last_30days)) & (status_df["date"] <= pd.to_datetime(d_max))]
        status_df_pie["date"] = pd.to_datetime(status_df_pie["date"]).dt.tz_localize(None).dt.to_period("D").astype(
            "|S")
        status_df_pie = status_df_pie.groupby(["status", "date"]).agg({"frequency": "sum"}).reset_index()

        file_type_df_pie = file_type_df[
            (file_type_df["date"] >= pd.to_datetime(last_30days)) & (file_type_df["date"] <= pd.to_datetime(d_max))]
        file_type_df_pie["date"] = pd.to_datetime(file_type_df_pie["date"]).dt.tz_localize(None).dt.to_period(
            "D").astype("|S")
        file_type_df_pie = file_type_df_pie.groupby(["file_type", "date"]).agg({"frequency": "sum"}).reset_index()

        crawler_df_gran = crawler_df[
            (crawler_df["date"] >= pd.to_datetime(last_30days)) & (crawler_df["date"] <= pd.to_datetime(d_max))]
        crawler_df_gran = crawler_df_gran.groupby(["top_directory"]).agg({"frequency": "sum"}).reset_index()

    elif date == "year":
        status_df_pie = status_df[
            (status_df["date"] >= pd.to_datetime(update_last_year)) & (status_df["date"] <= pd.to_datetime(d_max))]
        status_df_pie["date"] = pd.to_datetime(status_df_pie["date"]).dt.to_period('M').astype("str")
        status_df_pie = status_df_pie.groupby(["status", "date"]).agg({"frequency": "sum"}).reset_index()

        file_type_df_pie = file_type_df[
            (file_type_df["date"] >= pd.to_datetime(update_last_year)) & (
                    file_type_df["date"] <= pd.to_datetime(d_max))]
        file_type_df_pie["date"] = pd.to_datetime(file_type_df_pie["date"]).dt.to_period('M').astype("str")
        file_type_df_pie = file_type_df_pie.groupby(["file_type", "date"]).agg({"frequency": "sum"}).reset_index()

        crawler_df_gran = crawler_df[
            (crawler_df["date"] >= pd.to_datetime(update_last_year)) & (
                    crawler_df["date"] <= pd.to_datetime(d_max))]
        crawler_df_gran = crawler_df_gran.groupby(["top_directory"]).agg({"frequency": "sum"}).reset_index()

    crawler_df_gran.rename(columns={"top_directory": "Top directory", "frequency": "Total Bot Hits"}, inplace=True)

    data_sets = {
        "status_df": status_df_pie.to_json(orient="split", date_format="iso"),
        "crawler_df": crawler_df_gran.to_json(orient="split", date_format="iso"),
        "file_type": file_type_df_pie.to_json(orient="split", date_format="iso")
    }

    return json.dumps(data_sets)


@app.callback(
    Output('status_pie_chart', 'figure'),
    Input('store_data', 'data')
)
def update_status_code_pie(needed_data):
    colors = ["#23208A", "#BC0D68", "#6494FF", "#6494FF"]

    raw_status_data = json.loads(needed_data)
    ready_status_data = pd.read_json(raw_status_data["status_df"], orient="split")
    comp_hourly_df = ready_status_data.groupby(["status"]).agg({"frequency": "sum"}).reset_index()

    # fig = px.pie(data_frame=comp_hourly_df, names="status", hole=.5, hover_data="frequency", col)
    # fig.udate_traces()

    fig = go.Figure(data=[go.Pie(labels=comp_hourly_df["status"], values=comp_hourly_df["frequency"], hole=0.4, )])
    fig.update_layout(margin=dict(t=50, b=15, l=0, r=0, pad=4), title_text="Status code", height=220)
    fig.update_traces(hoverinfo="label+percent", textinfo="value", textfont_size=10, marker=dict(colors=colors))

    return fig


@app.callback(
    Output('filetype_chart', 'figure'),
    Input('store_data', 'data')
)
def update_file_type_pie(needed_data):
    colors = ["#23208A", "#BC0D68", "#6494FF", "#6494FF"]

    raw_file_data = json.loads(needed_data)
    ready_file_data = pd.read_json(raw_file_data["file_type"], orient="split")
    comp_file_df = ready_file_data.groupby(["file_type"]).agg({"frequency": "sum"}).reset_index()
    print(comp_file_df)

    # fig = px.pie(data_frame=comp_hourly_df, names="status", hole=.5, hover_data="frequency", col)
    # fig.udate_traces()

    fig = go.Figure(data=[go.Pie(labels=comp_file_df["file_type"], values=comp_file_df["frequency"], hole=0.4, )])
    fig.update_layout(margin=dict(t=50, b=15, l=0, r=0, pad=4), title_text="File type", height=220)
    fig.update_traces(hoverinfo="label+percent", textinfo="value", textfont_size=10, marker=dict(colors=colors))

    return fig


@app.callback(
    Output('status_code_bar', 'figure'),
    Input('store_data', 'data'),
)
def update_status_code_line(needed_data):
    colors = ["#23208A", "#BC0D68", "#6494FF", "#6494FF"]
    raw_status_data = json.loads(needed_data)
    ready_status_data = pd.read_json(raw_status_data["status_df"], orient="split")

    print(ready_status_data)
    fig = px.bar(ready_status_data, x=ready_status_data.columns[1], y="frequency",
                 color=ready_status_data["status"].astype("str"), barmode="group", )

    fig.update_layout(
        xaxis=dict(
            showline=True,
            showgrid=False,
            showticklabels=True,
            linecolor="rgb(204, 204, 204)",
            linewidth=2,
            ticks='outside',
            fixedrange=True,
            type="category",
            tickformat="%b %d\n%Y",

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
        ),
        # showlegend=True
    )

    return fig


def filetype_bar_chart(needed_data):
    colors = ["#23208A", "#BC0D68", "#6494FF", "#6494FF"]
    raw_status_data = json.loads(needed_data)
    ready_status_data = pd.read_json(raw_status_data["status_df"], orient="split")

    fig = px.bar(ready_status_data, x=ready_status_data.columns[1], y="frequency",
                 color=ready_status_data["file_type"].astype("str"), barmode="group", )

    fig.update_layout(
        xaxis=dict(
            showline=True,
            showgrid=False,
            showticklabels=True,
            linecolor="rgb(204, 204, 204)",
            linewidth=2,
            ticks='outside',
            fixedrange=True,
            type="category",
            tickformat="%b %d\n%Y",

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
        title_text="File type",
        height=522,
        legend=dict(
            # Adjust click behavior
            itemclick="toggleothers",
            itemdoubleclick="toggle",
        ), )


@app.callback(
    # Output('status_pie_chart', 'figure'),
    Output('crawler_table', 'data'),
    Input('store_data', 'data')
)
def update_table_crawler(crawler_data):
    raw_crawler_data = json.loads(crawler_data)
    ready_crawler_data = pd.read_json(raw_crawler_data["crawler_df"], orient="split")

    return ready_crawler_data.to_dict("records")


if __name__ == "__main__":
    app.run(port=3032, debug=True)
