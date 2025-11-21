# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import plotly.express as px
from sklearn.ensemble import RandomForestRegressor
import shap
import numpy as np

# ===========================
# PAGE CONFIG
# ===========================
st.set_page_config(
    page_title="Healthcare Insights Dashboard",
    page_icon="🏥",
    layout="wide"
)

# Mapping des noms d'États aux codes US pour la carte choroplèthe
STATE_TO_CODE = {
    'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR', 'California': 'CA',
    'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE', 'District of Columbia': 'DC',
    'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI', 'Idaho': 'ID', 'Illinois': 'IL',
    'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS', 'Kentucky': 'KY', 'Louisiana': 'LA',
    'Maine': 'ME', 'Maryland': 'MD', 'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN',
    'Mississippi': 'MS', 'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV',
    'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM', 'New York': 'NY',
    'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH', 'Oklahoma': 'OK', 'Oregon': 'OR',
    'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC', 'South Dakota': 'SD',
    'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT', 'Vermont': 'VT', 'Virginia': 'VA',
    'Washington': 'WA', 'West Virginia': 'WV', 'Wisconsin': 'WI', 'Wyoming': 'WY'
}

# ===========================
# LOAD DATA
# ===========================
try:
    census_df = pd.read_parquet("data/census_demographics")
    trends_df = pd.read_parquet("data/covid_search_trends")
    st.sidebar.success("✅ Data loaded successfully!")
except Exception as e:
    st.error(f"Error loading data: {e}")
    st.stop()

# Ensure required columns exist in census_df
required_census_cols = ['state_name', 'total_population', 'elderly_percentage', 'poverty_rate', 'median_household_income']
for col in required_census_cols:
    if col not in census_df.columns:
        st.error(f"Missing column in census data: {col}")
        st.stop()

# Ensure trends_df has datetime
if 'search_date' in trends_df.columns:
    trends_df['search_date'] = pd.to_datetime(trends_df['search_date'], errors='coerce')
else:
    st.warning("Trends data missing 'search_date', adding placeholder dates")
    trends_df['search_date'] = pd.to_datetime('2024-01-01')

# Add dummy 'state_name' if missing
if 'state_name' not in trends_df.columns:
    trends_df['state_name'] = 'USA'

# ===========================
# DASHBOARD UI
# ===========================
st.title("🏥 COVID Tracker Analytics")
st.markdown("---")
st.sidebar.header("")

# Tabs
tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Overview",
    "👥 Demographics",
    "🔍 Search Trends",
    "🤖 ML Forecasts"
])

# =========================================
# TAB 1: OVERVIEW
# =========================================
with tab1:
    st.header("📊 Executive Summary")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total US Population", f"{census_df['total_population'].sum():,.0f}")
    with col2:
        st.metric("Avg Elderly %", f"{census_df['elderly_percentage'].mean():.1f}%")
    with col3:
        st.metric("Avg Poverty Rate", f"{census_df['poverty_rate'].mean():.1f}%")
    with col4:
        st.metric("Total Search Volume", f"{trends_df['search_volume'].sum():,.0f}")

    st.markdown("---")
    st.subheader("Top 5 Elderly States")
    top_elderly = census_df.nlargest(5, 'elderly_percentage')[['state_name', 'elderly_percentage', 'total_population']]
    st.dataframe(top_elderly, hide_index=True)

    # --- Choropleth Map ---
    st.subheader("🗺️ Elderly Population % by State")
    census_df['state_code'] = census_df['state_name'].map(STATE_TO_CODE)
    fig_map = px.choropleth(
        census_df,
        locations='state_code',
        locationmode="USA-states",
        color='elderly_percentage',
        hover_name='state_name',
        hover_data=['total_population', 'poverty_rate'],
        color_continuous_scale="Reds",
        scope="usa",
        title="Elderly Population % by State"
    )
    fig_map.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    st.plotly_chart(fig_map, use_container_width=True)

# =========================================
# TAB 2: DEMOGRAPHICS
# =========================================
with tab2:
    st.header("👥 Demographic Analysis")
    selected_states = st.multiselect(
        "Select States",
        options=census_df['state_name'].unique(),
        default=census_df.nlargest(5, 'total_population')['state_name'].tolist()
    )
    if selected_states:
        filtered_census = census_df[census_df['state_name'].isin(selected_states)]
        fig1 = px.bar(
            filtered_census,
            x='state_name',
            y='elderly_percentage',
            color='elderly_percentage',
            color_continuous_scale='Reds',
            title='Elderly Population %'
        )
        st.plotly_chart(fig1, use_container_width=True)
        fig2 = px.scatter(
            filtered_census,
            x='median_household_income',
            y='poverty_rate',
            size='total_population',
            hover_data=['state_name'],
            color='poverty_rate',
            title='Income vs Poverty Rate'
        )
        st.plotly_chart(fig2, use_container_width=True)

# =========================================
# TAB 3: SEARCH TRENDS
# =========================================
with tab3:
    st.header("🔍 COVID Search Trends")

    min_date = trends_df['search_date'].min()
    max_date = trends_df['search_date'].max()
    date_range = st.date_input("Select Date Range", value=(min_date, max_date))

    all_terms = trends_df['search_term'].unique()
    selected_terms = st.multiselect("Select Terms", all_terms, default=all_terms[:5])
    if selected_terms:
        filtered = trends_df[
            (trends_df['search_term'].isin(selected_terms)) &
            (trends_df['search_date'] >= pd.to_datetime(date_range[0])) &
            (trends_df['search_date'] <= pd.to_datetime(date_range[1]))
        ]
        fig = px.line(
            filtered,
            x='search_date',
            y='search_volume',
            color='search_term',
            title="Search Trends Over Time"
        )
        st.plotly_chart(fig, use_container_width=True)

# =========================================
# TAB 4: AI INSIGHTS (Interactive & concise)
# =========================================
import requests
import time
with tab4:
    st.header("🔮 AI Healthcare Insights")
    # -----------------------------
    # User selects states
    # -----------------------------
    states_options = census_df['state_name'].tolist()
    selected_states = st.multiselect(
        "Select States for AI Analysis",
        options=states_options,
        default=states_options[:1]  # preselect one state to reduce API load
    )
    if selected_states:
        # Prepare summary: only necessary data
        filtered_census = census_df[census_df['state_name'].isin(selected_states)]
        filtered_trends = trends_df[trends_df['state_name'].isin(selected_states)]
        # Aggregate search volume by state
        agg = filtered_trends.groupby('state_name')['search_volume'].sum().reset_index()
        summary_df = filtered_census.merge(agg, on='state_name', how='left').fillna(0)
        # Only send key numbers
        summary_df = summary_df[['state_name', 'elderly_percentage', 'poverty_rate', 'search_volume']]
        # Build concise prompt
        prompt = (
            "You are a healthcare analytics expert.\n"
            "Provide **short and actionable insights** about healthcare pressure.\n"
            f"State data: {summary_df.to_dict(orient='records')}\n"
            "What are the possible future healthcare pressures and recommended early interventions?"
        )
        # -----------------------------
        # OpenAI GPT API function
        # -----------------------------
        def ask_openai(prompt: str, retries=2, delay=5):
            API_KEY = "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"
            url = "https://api.openai.com/v1/chat/completions"
            headers = {
                "Authorization": f"Bearer {API_KEY}",
                "Content-Type": "application/json"
            }
            body = {
                "model": "gpt-3.5-turbo",
                "messages": [
                    {"role": "system", "content": "You are an expert in healthcare analytics."},
                    {"role": "user", "content": prompt}
                ]
            }
            for attempt in range(retries):
                try:
                    resp = requests.post(url, headers=headers, json=body)
                    resp.raise_for_status()
                    data = resp.json()
                    return data["choices"][0]["message"]["content"]
                except requests.exceptions.HTTPError as http_err:
                    if resp.status_code == 429:
                        st.warning(f"Rate limit hit. Retrying in {delay} seconds...")
                        time.sleep(delay)
                    else:
                        st.error(f"HTTP error occurred: {http_err}")
                        return None
                except Exception as err:
                    st.error(f"Error calling OpenAI API: {err}")
                    return None
            st.warning("Max retries exceeded. Try again later.")
            return None
        # -----------------------------
        # Button to trigger AI insights
        # -----------------------------
        if st.button("Generate AI Insights for Selected States"):
            with st.spinner("Generating AI insights..."):
                insight = ask_openai(prompt)
            if insight:
                st.markdown("**AI Analysis:**")
                st.write(insight)
            else:
                st.warning("No insight returned. Try again later or select fewer states.")