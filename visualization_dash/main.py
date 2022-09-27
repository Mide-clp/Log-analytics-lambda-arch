from dash import Dash, html, Input, Output, dcc
import pandas as pd

app = Dash(__name__)
df = pd.DataFrame({
    'student_id': range(1, 11),
    'score': [1, 5, 2, 5, 2, 3, 1, 5, 1, 5]
})
h = 1
h +=1
print(h)
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
            html.Div(style={"backgroundColor": "#C4C4C4", "position": "absolute",
                            "height": "500px", "width": "55%",
                            "top": "299px", "left": "210px"}),
            html.Div(style={"backgroundColor": "#C4C4C4", "position": "absolute",
                            "height": "230px", "width": "22.9%",
                            "top": "299px", "left": "1575px"}),
            html.Div(style={"backgroundColor": "#C4C4C4", "position": "absolute",
                            "height": "230px", "width": "22.9%",
                            "top": "569px", "left": "1575px"}),

        ]),

    ],
        # style={"backgroundColor": "#7FDBFF"}
    )


@app.callback(
    Output('boolean-switch-result', 'children'),
    Input('our-boolean-switch', 'on')
)
def update_output(on):
    return f'The switch is {on}.'


if __name__ == "__main__":
    app.run(port=3032, debug=True)
