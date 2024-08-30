import streamlit as st
from core.duckdb import DuckDB
from datetime import datetime
from pydantic import BaseModel, Field
import plotly.express as px


st.set_page_config(layout="wide")

class FilterOptions(BaseModel):
    month: str = Field(..., description="Month for filtering")
    user_name: str = Field(..., description="User name for filtering")

class DataPlotter:
    @staticmethod
    def plot_bar_chart(df, x: str, y: list, color_map: dict, labels: dict):
        fig = px.bar(
            df,
            x=x,
            y=y,
            color_discrete_map=color_map,
            labels=labels
        )
        fig.update_layout(
            barmode='group',
            yaxis_title=labels['value'],
            xaxis_title=labels['category'],
            legend_title_text='',
            hovermode='x',
            bargap=0.2
        )
        fig.update_traces(
            texttemplate='%{y:.2f}',
            textposition='outside',
            insidetextanchor='start',
            textfont=dict(size=14)
        )
        return fig

class DataStyler:
    @staticmethod
    def highlight_categories(s, color_map: dict):
        color = color_map.get(s, '#FFFFFF')
        return f'background-color: {color}; color: black;'

    @staticmethod
    def gradient_cost(val, min_val, max_val):
        if val > 400:
            normalized = (val - min_val) / (max_val - min_val)
            red_intensity = int((220 - 100) * (1 - normalized) + 100)
            return f'background-color: rgb(255, 100, 100); color: black;'
        else:
            normalized = (val - min_val) / (400 - min_val)
            red_intensity = int((220 - 100) * (1 - normalized) + 100)
            return f'background-color: rgb(255, {red_intensity}, {red_intensity}); color: black;'

def get_filtered_dataframe(df, options: FilterOptions):
    return df[(df['month'] == options.month) & (df['user_name'] == options.user_name)]

def get_queries():
    queries = {
        "summary": """
            WITH results AS (
                SELECT
                    month,
                    category,
                    user_name,
                    SUM(user_cost) AS user_cost
                FROM
                    splitwise.master
                GROUP BY
                    1, 2, 3
            )
            SELECT 
                r.month,
                r.category,
                r.user_name,
                SUM(r.user_cost) AS result_cost,
                SUM(mlap.user_cost) AS limit_cost
            FROM 
                results r
            LEFT JOIN 
                splitwise.master_limits_and_percentages mlap 
                ON mlap.month = r.month
                AND mlap.category = r.category
                AND mlap.user_name = r.user_name
            WHERE r.category NOT LIKE '%ganhos%'
            GROUP BY
                1, 2, 3
            ORDER BY 
                1, 2, 3
        """,
        "purchases": """
            SELECT
                month,
                group_name,
                cast(expense_id as string) as expense_id,
                category,
                description,
                cost,
                user_cost,
                CONCAT(
                    ROUND(user_percentage * 100, 1),
                    '%'
                ) AS '%',
                user_name,
                is_payer
            FROM splitwise.master
            WHERE category NOT LIKE '%ganhos%' and category = 'compras'
            ORDER BY
                CASE category
                    WHEN 'contas' THEN 1
                    WHEN 'mercado' THEN 2
                    WHEN 'conveniência' THEN 3
                    WHEN 'alimentação' THEN 4
                    WHEN 'bem-estar' THEN 5
                    WHEN 'outros' THEN 6
                    WHEN 'transporte' THEN 7
                    WHEN 'assinaturas' THEN 8
                    WHEN 'apenas joão' THEN 9
                    WHEN 'apenas lana' THEN 10
                    ELSE 7
                END,
                user_cost DESC;
        """,
        "expenses": """
            SELECT
                month,
                group_name,
                expense_id,
                category,
                description,
                cost,
                user_cost,
                CONCAT(
                    ROUND(user_percentage * 100, 1),
                    '%'
                ) AS '%',
                user_name,
                is_payer
            FROM splitwise.master
            WHERE category NOT LIKE '%ganhos%' and category <> 'compras'
            ORDER BY
                CASE category
                    WHEN 'contas' THEN 1
                    WHEN 'mercado' THEN 2
                    WHEN 'conveniência' THEN 3
                    WHEN 'alimentação' THEN 4
                    WHEN 'bem-estar' THEN 5
                    WHEN 'outros' THEN 6
                    WHEN 'transporte' THEN 7
                    WHEN 'assinaturas' THEN 8
                    WHEN 'apenas joão' THEN 9
                    WHEN 'apenas lana' THEN 10
                    ELSE 7
                END,
                user_cost DESC;
        """
    }
    return queries

duckdb_client = DuckDB()

st.markdown('## Visão Detalhada')

queries = get_queries()
summary_df = duckdb_client.query_duckdb(queries['summary'])

months_sorted = sorted(summary_df['month'].unique())
users_sorted = sorted(summary_df['user_name'].unique())

selected_month = st.selectbox("Mês", months_sorted, index=months_sorted.index(datetime.now().strftime("%Y-%m")))
selected_user = st.selectbox("Usuário", users_sorted)

filter_options = FilterOptions(month=selected_month, user_name=selected_user)
filtered_df = get_filtered_dataframe(summary_df, filter_options)

fig = DataPlotter.plot_bar_chart(
    df=filtered_df.sort_values(by='result_cost', ascending=False),
    x='category',
    y=['result_cost', 'limit_cost'],
    color_map={
        'result_cost': '#cf1204',
        'limit_cost': 'lightgray'
    },
    labels={'value': 'Cost', 'category': 'Category'}
)

col1, col2 = st.columns((5, 3))

with col1:
    st.plotly_chart(fig, use_container_width=True)

purchases_df = duckdb_client.query_duckdb(queries['purchases'])
filtered_purchases_df = get_filtered_dataframe(purchases_df, filter_options)

with col2:
    st.markdown('### Compras')
    st.dataframe(filtered_purchases_df.drop(columns=['month', 'group_name', 'user_name']), use_container_width=True, height=340)

expenses_df = duckdb_client.query_duckdb(queries['expenses'])
filtered_expenses_df = get_filtered_dataframe(expenses_df, filter_options)

min_cost = filtered_expenses_df['cost'].min()
max_cost = filtered_expenses_df['cost'].max()

styled_df = filtered_expenses_df.style.applymap(
    lambda s: DataStyler.highlight_categories(s, color_map={
        "contas": "#A9A9A9",
        "mercado": "#F0F0F0",
        "conveniência": "#A9A9A9",
        "alimentação": "#F0F0F0",
        "bem-estar": "#A9A9A9",
        "outros": "#F0F0F0",
        "transporte": "#A9A9A9",
        "assinaturas": "#F0F0F0",
        "apenas lana": "#A9A9A9",
        "apenas joão": "#F0F0F0"     
    }), subset=['category']
).applymap(
    lambda val: DataStyler.gradient_cost(val, min_cost, max_cost),
    subset=['user_cost']
)

st.markdown('### Despesas Mensais')
st.dataframe(styled_df, use_container_width=True, height=750)
