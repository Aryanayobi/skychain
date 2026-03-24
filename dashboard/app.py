"""
dashboard/app.py
----------------
Live flight enrichment dashboard.
Auto-refreshes every 60 seconds to show latest data from PostgreSQL.

Run:
    python dashboard/app.py
Open: http://localhost:8050
"""

import os
import psycopg2
import pandas as pd
import dash
from dash import dcc, html, Input, Output
import plotly.express as px
import plotly.graph_objects as go
from dotenv import load_dotenv

load_dotenv()

DB_HOST     = os.getenv('DB_HOST', 'localhost')
DB_PORT     = os.getenv('DB_PORT', '5432')
DB_NAME     = os.getenv('DB_NAME', 'flightdb')
DB_USER     = os.getenv('DB_USER', 'user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password')


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_latest(hours: int = 1) -> pd.DataFrame:
    """Load enriched flight records from the last N hours."""
    sql = f"""
        SELECT *
        FROM enriched_flights
        WHERE fetched_at >= NOW() - INTERVAL '{hours} hours'
        ORDER BY fetched_at DESC
    """
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT,
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
        df = pd.read_sql(sql, conn)
        conn.close()
        return df
    except Exception as e:
        print(f"DB load error: {e}")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# App layout
# ---------------------------------------------------------------------------

app = dash.Dash(__name__, title="Flight Enrichment Pipeline")

CARD = lambda label, value, color: html.Div(style={
    'backgroundColor': 'white', 'borderRadius': '8px',
    'padding': '16px 20px', 'borderLeft': f'4px solid {color}',
    'boxShadow': '0 1px 4px rgba(0,0,0,0.1)', 'flex': '1', 'minWidth': '150px'
}, children=[
    html.P(label, style={'margin': 0, 'color': '#7f8c8d', 'fontSize': '13px'}),
    html.H3(value, style={'margin': '4px 0 0', 'color': '#2c3e50', 'fontSize': '20px'}),
])

app.layout = html.Div(
    style={'fontFamily': 'Arial, sans-serif', 'padding': '24px', 'backgroundColor': '#f4f6f9'},
    children=[
        html.H1("🛫 Live Flight Enrichment Dashboard",
                style={'textAlign': 'center', 'color': '#2c3e50', 'marginBottom': '4px'}),
        html.P("North America · OpenSky + OpenWeatherMap + Lufthansa API",
               style={'textAlign': 'center', 'color': '#7f8c8d', 'marginBottom': '24px'}),

        # Time range filter
        html.Div(style={'display': 'flex', 'gap': '16px', 'marginBottom': '20px', 'alignItems': 'flex-end'}, children=[
            html.Div([
                html.Label("Time window", style={'fontWeight': 'bold', 'color': '#2c3e50'}),
                dcc.Dropdown(id='time-window', value=1, clearable=False, options=[
                    {'label': 'Last 30 minutes', 'value': 0.5},
                    {'label': 'Last 1 hour',     'value': 1},
                    {'label': 'Last 3 hours',    'value': 3},
                    {'label': 'Last 6 hours',    'value': 6},
                ], style={'minWidth': '200px'})
            ]),
        ]),

        # KPI cards
        html.Div(id='kpis', style={'display': 'flex', 'gap': '16px', 'marginBottom': '24px', 'flexWrap': 'wrap'}),

        # Flight map (full width)
        html.Div(
            dcc.Graph(id='flight-map', style={'height': '500px'}),
            style={'backgroundColor': 'white', 'borderRadius': '8px',
                   'padding': '8px', 'boxShadow': '0 1px 4px rgba(0,0,0,0.1)', 'marginBottom': '16px'}
        ),

        # Row 2 — charts
        html.Div(style={'display': 'grid', 'gridTemplateColumns': '1fr 1fr', 'gap': '16px', 'marginBottom': '16px'}, children=[
            html.Div(dcc.Graph(id='airline-chart'),  style={'backgroundColor': 'white', 'borderRadius': '8px', 'padding': '8px', 'boxShadow': '0 1px 4px rgba(0,0,0,0.1)'}),
            html.Div(dcc.Graph(id='weather-chart'),  style={'backgroundColor': 'white', 'borderRadius': '8px', 'padding': '8px', 'boxShadow': '0 1px 4px rgba(0,0,0,0.1)'}),
        ]),

        # Row 3
        html.Div(style={'display': 'grid', 'gridTemplateColumns': '1fr 1fr', 'gap': '16px'}, children=[
            html.Div(dcc.Graph(id='altitude-chart'), style={'backgroundColor': 'white', 'borderRadius': '8px', 'padding': '8px', 'boxShadow': '0 1px 4px rgba(0,0,0,0.1)'}),
            html.Div(dcc.Graph(id='country-chart'),  style={'backgroundColor': 'white', 'borderRadius': '8px', 'padding': '8px', 'boxShadow': '0 1px 4px rgba(0,0,0,0.1)'}),
        ]),

        # Auto-refresh every 60 seconds
        dcc.Interval(id='refresh', interval=60_000, n_intervals=0),
    ]
)


# ---------------------------------------------------------------------------
# Callbacks
# ---------------------------------------------------------------------------

@app.callback(
    Output('kpis', 'children'),
    Output('flight-map', 'figure'),
    Output('airline-chart', 'figure'),
    Output('weather-chart', 'figure'),
    Output('altitude-chart', 'figure'),
    Output('country-chart', 'figure'),
    Input('refresh', 'n_intervals'),
    Input('time-window', 'value'),
)
def update(_, hours):
    df = load_latest(hours=hours)
    empty = go.Figure().update_layout(
        title='No data yet — run the pipeline first!',
        plot_bgcolor='white', paper_bgcolor='white'
    )

    if df.empty:
        return [], empty, empty, empty, empty, empty

    # Deduplicate — keep most recent reading per aircraft
    latest = df.sort_values('fetched_at').groupby('icao24').last().reset_index()

    # KPIs
    kpis = [
        CARD("✈️ Active Flights",    str(len(latest)),                           '#e74c3c'),
        CARD("🌍 Countries",         str(latest['origin_country'].nunique()),     '#3498db'),
        CARD("🏢 Airlines",          str(latest['airline_name'].dropna().nunique()), '#2ecc71'),
        CARD("📏 Avg Altitude",      f"{latest['altitude_ft'].mean():,.0f} ft",  '#9b59b6'),
        CARD("💨 Avg Speed",         f"{latest['velocity_kmh'].mean():.0f} km/h",'#f39c12'),
        CARD("🌡️ Avg Temp Below",   f"{latest['origin_weather_temp'].mean():.1f}°C", '#1abc9c'),
    ]

    # Flight map — scatter on a geographic map
    map_df = latest.dropna(subset=['latitude', 'longitude'])
    map_df = map_df.copy()
    map_df['hover'] = (
        map_df['callsign'].fillna('Unknown') + '<br>' +
        map_df['airline_name'].fillna('Unknown airline') + '<br>' +
        map_df['altitude_ft'].apply(lambda x: f'{x:,.0f} ft' if pd.notna(x) else '') + '<br>' +
        map_df['origin_weather_cond'].fillna('')
    )

    flight_map = go.Figure(go.Scattergeo(
        lat=map_df['latitude'],
        lon=map_df['longitude'],
        text=map_df['hover'],
        mode='markers',
        marker=dict(
            size=6,
            color=map_df['altitude_ft'],
            colorscale='Viridis',
            showscale=True,
            colorbar_title='Altitude (ft)',
            opacity=0.8,
        ),
        hovertemplate='%{text}<extra></extra>',
    ))
    flight_map.update_layout(
        title='🗺️ Live Flight Positions (coloured by altitude)',
        geo=dict(
            scope='world',
            showland=True, landcolor='#f0f0f0',
            showocean=True, oceancolor='#d0e8f0',
            showlakes=True, lakecolor='#d0e8f0',
            showcountries=True, countrycolor='#cccccc',
            showcoastlines=True,
        ),
        paper_bgcolor='white',
        margin=dict(l=0, r=0, t=40, b=0),
    )

    # Top airlines bar chart
    airline_counts = (
        latest['airline_name'].dropna()
        .value_counts().head(10).reset_index()
    )
    airline_counts.columns = ['airline', 'flights']
    airline_fig = px.bar(
        airline_counts, x='flights', y='airline', orientation='h',
        title='🏢 Top Airlines in the Air',
        labels={'flights': 'Active Flights', 'airline': ''},
        color='flights', color_continuous_scale='Blues'
    )
    airline_fig.update_layout(plot_bgcolor='white', paper_bgcolor='white', yaxis={'categoryorder': 'total ascending'})

    # Weather conditions pie chart
    weather_counts = latest['origin_weather_cond'].dropna().value_counts().reset_index()
    weather_counts.columns = ['condition', 'count']
    weather_fig = px.pie(
        weather_counts, names='condition', values='count',
        title='⛅ Weather Conditions Under Flights',
        color_discrete_sequence=px.colors.qualitative.Pastel
    )
    weather_fig.update_layout(paper_bgcolor='white')

    # Altitude distribution histogram
    alt_fig = px.histogram(
        latest.dropna(subset=['altitude_ft']),
        x='altitude_ft', nbins=30,
        title='📏 Altitude Distribution (ft)',
        labels={'altitude_ft': 'Altitude (ft)', 'count': 'Flights'},
        color_discrete_sequence=['#9b59b6']
    )
    alt_fig.update_layout(plot_bgcolor='white', paper_bgcolor='white')

    # Origin country breakdown
    country_counts = latest['origin_country'].value_counts().head(10).reset_index()
    country_counts.columns = ['country', 'flights']
    country_fig = px.bar(
        country_counts, x='country', y='flights',
        title='🌍 Flights by Origin Country',
        labels={'country': 'Country', 'flights': 'Flights'},
        color='flights', color_continuous_scale='Oranges'
    )
    country_fig.update_layout(plot_bgcolor='white', paper_bgcolor='white')

    return kpis, flight_map, airline_fig, weather_fig, alt_fig, country_fig


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8050)
