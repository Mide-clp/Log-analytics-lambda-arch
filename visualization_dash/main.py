import datetime
from dash import Dash, html, Input, Output, dcc, dash_table
import json
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from datetime import timedelta, date
from read_cassandra import get_cassandra_data, join_real_batch_data

app = Dash(__name__)

sql = "SELECT * FROM status_code"

data = get_cassandra_data(sql)
pd.options.display.float_format = '{:,.2f}'.format
data["date"] = pd.to_datetime(data["date"])

try:
    d_max = max(data["date"])
    d_min = min(data["date"])
except ValueError as e:
    d_max = datetime.datetime.now()
    d_min = datetime.datetime.now()

last_30days = d_max - timedelta(days=30)
last_year = d_max - timedelta(weeks=52)

crawler_columns = ["Top directory", "Total Bot Hits"]
colors = ["#A424F4", "#143CFC", "#04E474", "#D0EAFC", "#FCBC04", "#FC6CB4", "#FC7C1C", "#944C04", "#CDCDCD", "#1C2C44"]

# dash chart and dashboard design
app.layout = \
    html.Div([
        # The header

        html.Div([
            html.H1("Log file Analyzer", style={"position": "absolute", "height": "70px",
                                                "width": "100%", "top": "20px",
                                                "textAlign": "center", "color": "#383838"})
        ], style={"backgroundColor": "#FFA63E", "position": "absolute",
                  "height": "113px", "width": "100%",
                  "top": "0px"}),
        html.Div([

            # Crawler selector
            html.Div([dcc.Dropdown(["Googlebot Desktop", "Googlebot Smartphone", "Bingbot Desktop", "All Bots"],
                                   id="select_bot",
                                   value="All Bots", multi=False, clearable=False, style={"color": "#383838"}),
                      ],
                     style={"backgroundColor": "#23208A", "position": "absolute",
                            "height": "33px", "width": "8%",
                            "top": "150px", "left": "170px"}),

            # Date option selector

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
                       "top": "150px", "left": "510px"}),

            # html.Div(style={"backgroundColor": "#C4C4C4", "position": "absolute",
            #                 "height": "33px", "width": "12%",
            #                 "top": "250px", "left": "210px"})
        ]),
        html.Div([
            # bot hit line chart
            html.Div([dcc.Graph(id="bot_hit_line")], style={"backgroundColor": "#C4C4C4", "position": "absolute",
                                                            "height": "522px", "width": "55%",
                                                            "top": "299px", "left": "170px",
                                                            "boxShadow": "0px 1px 4px rgba(0, 0, 0, 0.25)"}),
            # Status code pie chart
            html.Div([dcc.Graph(id="status_pie_chart"), ],
                     style={"backgroundColor": "#C4C4C4", "position": "absolute",
                            "height": "220px", "width": "22.9%",
                            "top": "299px", "left": "1535px",
                            "boxShadow": "0px 1px 4px rgba(0, 0, 0, 0.25)"}),
            # file type pie chart
            html.Div([dcc.Graph(id="filetype_chart"), ],
                     style={"backgroundColor": "#C4C4C4", "position": "absolute",
                            "height": "220px", "width": "22.9%",
                            "top": "600px", "left": "1535px", "boxShadow": "0px 1px 4px rgba(0, 0, 0, 0.25)"}),

        ], ),

        # Intermediary for storing data to distribute to other charts
        dcc.Store(id="store_data"),
        html.Div([
            html.Div(
                # Status code bar chart
                [dcc.Graph(id="status_code_bar")], style={"backgroundColor": "#C4C4C4", "position": "absolute",
                                                          "height": "522px", "width": "55%",
                                                          "top": "859px", "left": "170px",
                                                          "boxShadow": "0px 1px 4px rgba(0, 0, 0, 0.25)"}
            ),

            html.Div(
                # File type bar chart
                [dcc.Graph(id="file_type_bar")], style={"backgroundColor": "#C4C4C4", "position": "absolute",
                                                        "height": "522px", "width": "55%",
                                                        "top": "1420px", "left": "170px",
                                                        "boxShadow": "0px 1px 4px rgba(0, 0, 0, 0.25)",
                                                        "bottom": "500px"}
            ),

            html.Div(
                # The table containing domain level directory, and frequency of crawler
                [html.Table(dash_table.DataTable(
                    id="crawler_table",
                    columns=[{"name": name, "id": name} for name in crawler_columns],
                    style_data=dict(backgroundColor="#FFFFF", textAlign="center", left=500,  size=40,),
                    style_table=dict(top="0px", width="546px", line_color="#FFA63E",
                                     boxShadow="0px 0px 0px rgba(0, 0, 0, 0.25)", height="700px", overflowY="auto",
                                     ),
                    style_header=dict(color="white", backgroundColor="#FFA63E", fontWeight="bold", textAlign="center",),
                    fixed_rows={'headers': True},
                    page_action='none',
                    # fill_width=False
                ))], style={"backgroundColor": "#FFFFF", "position": "absolute",
                            "height": "705px", "width": "22.9%",
                            "top": "859px", "left": "1535px",
                            "boxShadow": "0px 1px 4px rgba(0, 0, 0, 0.25)"}
            )
        ])
    ],

        # style={"backgroundColor": "#7FDBFF"}
    )


# This function updates the date drop down, anytime a new date is selected
@app.callback(
    # Output('status_pie_chart', 'figure'),
    Output('select_date', 'options'),
    Input('select_date', 'value')
)
def update_date(date_input):
    """
    :param date_input:
    :return: a list of dictionary containing the label and value for date
    """

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


# This function get the necessary data from the database and store them in the data intermediary store
@app.callback(
    Output('store_data', 'data'),
    Input('select_bot', 'value'),
    Input('select_date', 'value')
)
def get_data(bot, date):
    """

    :param bot:
    :param date:
    :return: A json containing the needed data for charts
    """

    Status_code_data = join_real_batch_data("status_code", "status_code_realtime")
    crawler_freq_data = join_real_batch_data("crawler_frequency", "crawler_frequency_realtime")
    file_type_data = join_real_batch_data("file_type", "file_type_realtime")
    bot_hit_data = join_real_batch_data("bot_hits", "bot_hits_realtime")

    Status_code_data["status"] = Status_code_data["status"].astype("str")

    # filter for where data is equal to the current bot
    crawler_df = crawler_freq_data[crawler_freq_data["crawler"] == bot]
    status_df = Status_code_data[Status_code_data["crawler"] == bot]
    file_type_df = file_type_data[file_type_data["crawler"] == bot]
    bot_hit_df = bot_hit_data[bot_hit_data["crawler"] == bot]

    # If all bot is selected, it should return all the data
    if bot == "All Bots":
        status_df = Status_code_data
        crawler_df = crawler_freq_data
        file_type_df = file_type_data
        bot_hit_df = bot_hit_data

    # format date
    status_df["date"] = pd.to_datetime(status_df["date"])
    crawler_df["date"] = pd.to_datetime(crawler_df["date"])
    file_type_df["date"] = pd.to_datetime(file_type_df["date"])
    bot_hit_df["date"] = pd.to_datetime(bot_hit_df["date"])

    update_max = max(status_df["date"])
    update_min = min(status_df["date"])

    update_last_30days = update_max - timedelta(days=30)
    update_last_year = d_max - timedelta(weeks=52)
    # comp_df = ""

    status_df_pie = ""
    crawler_df_gran = ""
    file_type_df_pie = ""
    bot_hit_df_line = ""

    # specify the period of data needed, and get the total frequency for that period
    if date == "Today":

        # data for status_type
        status_df_pie = status_df[status_df["date"] == pd.to_datetime(d_max)]
        status_df_pie = status_df_pie.groupby(["status", "hour"]).agg({"frequency": "sum"}).reset_index()

        # data for bot hit overtime
        bot_hit_df_line = bot_hit_df[bot_hit_df["date"] == pd.to_datetime(d_max)]
        bot_hit_df_line = bot_hit_df_line.groupby(["crawler", "hour"]).agg({"frequency": "sum"}).reset_index()

        # data for file type
        file_type_df_pie = file_type_df[file_type_df["date"] == pd.to_datetime(d_max)]
        file_type_df_pie = file_type_df_pie.groupby(["file_type", "hour"]).agg({"frequency": "sum"}).reset_index()

        # data for top domain with the most request
        crawler_df_gran = crawler_df[crawler_df["date"] == pd.to_datetime(d_max)]
        crawler_df_gran = crawler_df_gran.groupby(["top_directory"]).agg({"frequency": "sum"}).reset_index()

    elif date == "30days":
        # data for status_type
        status_df_pie = status_df[
            (status_df["date"].dt.tz_localize(None) >= pd.to_datetime(last_30days).tz_localize(None)) & (
                    status_df["date"].dt.tz_localize(None) <= pd.to_datetime(d_max).tz_localize(None))]
        status_df_pie["date"] = pd.to_datetime(status_df_pie["date"]).dt.tz_localize(None).dt.to_period("D").astype(
            "|S")
        status_df_pie = status_df_pie.groupby(["status", "date"]).agg({"frequency": "sum"}).reset_index()

        # data for bot hit overtime
        bot_hit_df_line = bot_hit_df[
            (bot_hit_df["date"].dt.tz_localize(None) >= pd.to_datetime(last_30days).tz_localize(None)) & (
                    bot_hit_df["date"].dt.tz_localize(None) <= pd.to_datetime(d_max).tz_localize(None))]
        bot_hit_df_line["date"] = pd.to_datetime(bot_hit_df_line["date"]).dt.tz_localize(None).dt.to_period("D").astype(
            "|S")
        bot_hit_df_line = bot_hit_df_line.groupby(["crawler", "date"]).agg({"frequency": "sum"}).reset_index()

        # data for file type
        file_type_df_pie = file_type_df[
            (file_type_df["date"].dt.tz_localize(None) >= pd.to_datetime(last_30days).tz_localize(None)) & (
                    file_type_df["date"].dt.tz_localize(None) <= pd.to_datetime(d_max).tz_localize(None))]

        file_type_df_pie["date"] = pd.to_datetime(file_type_df_pie["date"]).dt.tz_localize(None).dt.to_period(
            "D").astype("|S")
        file_type_df_pie = file_type_df_pie.groupby(["file_type", "date"]).agg({"frequency": "sum"}).reset_index()

        # data for top domain with the most request
        crawler_df_gran = crawler_df[
            (crawler_df["date"].dt.tz_localize(None) >= pd.to_datetime(last_30days).tz_localize(None)) & (
                    crawler_df["date"].dt.tz_localize(None) <= pd.to_datetime(d_max).tz_localize(None))]
        crawler_df_gran = crawler_df_gran.groupby(["top_directory"]).agg({"frequency": "sum"}).reset_index()

    elif date == "year":
        # data for status_type
        status_df_pie = status_df[
            (status_df["date"].dt.tz_localize(None) >= pd.to_datetime(update_last_year).tz_localize(None)) & (
                    status_df["date"].dt.tz_localize(None) <= pd.to_datetime(d_max).tz_localize(None))]
        status_df_pie["date"] = pd.to_datetime(status_df_pie["date"]).dt.to_period('M').astype("str")

        status_df_pie = status_df_pie.groupby(["status", "date"]).agg({"frequency": "sum"}).reset_index()

        # data for bot hit overtime
        bot_hit_df_line = bot_hit_df[
            (bot_hit_df["date"].dt.tz_localize(None) >= pd.to_datetime(update_last_year).tz_localize(None)) & (
                    bot_hit_df["date"].dt.tz_localize(None) <= pd.to_datetime(d_max).tz_localize(None))]

        bot_hit_df_line["date"] = pd.to_datetime(bot_hit_df_line["date"]).dt.to_period('M').astype("str")
        bot_hit_df_line = bot_hit_df_line.groupby(["crawler", "date"]).agg({"frequency": "sum"}).reset_index()

        # data for file type
        file_type_df_pie = file_type_df[
            (file_type_df["date"].dt.tz_localize(None) >= pd.to_datetime(update_last_year).tz_localize(None)) & (
                    file_type_df["date"].dt.tz_localize(None) <= pd.to_datetime(d_max).tz_localize(None))]
        file_type_df_pie["date"] = pd.to_datetime(file_type_df_pie["date"]).dt.to_period('M').astype("str")
        file_type_df_pie = file_type_df_pie.groupby(["file_type", "date"]).agg({"frequency": "sum"}).reset_index()

        # data for top domain with the most request
        crawler_df_gran = crawler_df[
            (crawler_df["date"].dt.tz_localize(None) >= pd.to_datetime(update_last_year).tz_localize(None)) & (
                    crawler_df["date"].dt.tz_localize(None) <= pd.to_datetime(d_max).tz_localize(None))]
        crawler_df_gran = crawler_df_gran.groupby(["top_directory"]).agg({"frequency": "sum"}).reset_index()

    crawler_df_gran.rename(columns={"top_directory": "Top directory", "frequency": "Total Bot Hits"}, inplace=True)

    data_sets = {
        "status_df": status_df_pie.to_json(orient="split", date_format="iso"),
        "crawler_df": crawler_df_gran.to_json(orient="split", date_format="iso"),
        "file_type": file_type_df_pie.to_json(orient="split", date_format="iso"),
        "bot_hits": bot_hit_df_line.to_json(orient="split", date_format="iso")

    }

    return json.dumps(data_sets)


# This function updates the pie chart status code in the dashboard
@app.callback(
    Output('status_pie_chart', 'figure'),
    Input('store_data', 'data')
)
def update_status_code_pie(needed_data):
    raw_status_data = json.loads(needed_data)
    ready_status_data = pd.read_json(raw_status_data["status_df"], orient="split")
    comp_hourly_df = ready_status_data.groupby(["status"]).agg({"frequency": "sum"}).reset_index()

    fig = go.Figure(data=[go.Pie(labels=comp_hourly_df["status"], values=comp_hourly_df["frequency"], hole=0.4, )])
    fig.update_layout(margin=dict(t=50, b=15, l=0, r=0, pad=4), title_text="Status code", height=220)
    fig.update_traces(hoverinfo="label+percent", textinfo="value", textfont_size=10, marker=dict(colors=colors))

    return fig


# This function updates the pie chart file type in the dashboard
@app.callback(
    Output('filetype_chart', 'figure'),
    Input('store_data', 'data')
)
def update_file_type_pie(needed_data):
    raw_file_data = json.loads(needed_data)
    ready_file_data = pd.read_json(raw_file_data["file_type"], orient="split")
    comp_file_df = ready_file_data.groupby(["file_type"]).agg({"frequency": "sum"}).reset_index()

    fig = go.Figure(data=[go.Pie(labels=comp_file_df["file_type"], values=comp_file_df["frequency"], hole=0.4, )])
    fig.update_layout(margin=dict(t=50, b=15, l=0, r=0, pad=4), title_text="File type", height=220)
    fig.update_traces(hoverinfo="label+percent", textinfo="value", textfont_size=10, marker=dict(colors=colors))

    return fig


# The function updates the status code bar chart
@app.callback(
    Output('status_code_bar', 'figure'),
    Input('store_data', 'data'),
)
def update_status_code_line(needed_data):
    raw_status_data = json.loads(needed_data)
    ready_status_data = pd.read_json(raw_status_data["status_df"], orient="split")

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


# Thus function updates the file type bar chart
@app.callback(
    Output('file_type_bar', 'figure'),
    Input('store_data', 'data'),
)
def filetype_bar_chart(needed_data):
    raw_status_data = json.loads(needed_data)
    ready_status_data = pd.read_json(raw_status_data["file_type"], orient="split")

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

    return fig


# This function updates the bot hit overtime
@app.callback(
    Output('bot_hit_line', 'figure'),
    Input('store_data', 'data'),
)
def bot_hit_chart(needed_data):
    raw_status_data = json.loads(needed_data)
    ready_status_data = pd.read_json(raw_status_data["bot_hits"], orient="split")

    fig = px.scatter(ready_status_data, x=ready_status_data.columns[1], y="frequency",
                     color=ready_status_data["crawler"])

    fig2 = px.line(ready_status_data, x=ready_status_data.columns[1], y="frequency", color=ready_status_data["crawler"])

    # fig2.update_traces(line=dict(color="rgba(50,50,50,0.2)"))

    fig3 = go.Figure(data=fig.data + fig2.data)
    fig3.update_layout(
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
            title=ready_status_data.columns[1]

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
            gridcolor="#D9D9D9",
            title="frequency",),
        plot_bgcolor='white',
        margin=dict(t=50, b=15, l=0, r=0, pad=4),
        title_text="Bot Hit",
        height=522,
        legend=dict(
            # Adjust click behavior
            itemclick="toggleothers",
            itemdoubleclick="toggle",
        ), )

    return fig3


# This function update the domain top directory and the number of hits
@app.callback(
    # Output('status_pie_chart', 'figure'),
    Output('crawler_table', 'data'),
    Input('store_data', 'data')
)
def update_table_crawler(crawler_data):
    raw_crawler_data = json.loads(crawler_data)
    # .map('{:,.0f}'.format)
    print(raw_crawler_data)
    ready_crawler_data = pd.read_json(raw_crawler_data["crawler_df"], orient="split")
    ready_crawler_data["Total Bot Hits"] = ready_crawler_data.apply(lambda x: "{:,}".format(x['Total Bot Hits']), axis=1)
    print(ready_crawler_data)

    return ready_crawler_data.to_dict("records")


if __name__ == "__main__":
    app.run(port=3032, debug=True)
