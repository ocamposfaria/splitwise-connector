import streamlit as st
import pandas as pd
from datetime import datetime
from core.duckdb_client import get_groups, get_expenses, get_suggested_value, get_suggested_category
from main import create_expense  # <-- importa a função que você me passou

# --- DEFINE O MÊS ATUAL ---
mes_atual = datetime.today().strftime("%Y-%m")

# --- FUNÇÃO CACHE PARA GRUPOS E GASTOS ---
@st.cache_data
def cached_get_groups():
    return get_groups()

@st.cache_data
def cached_get_expenses():
    return get_expenses()

# --- INICIALIZAÇÃO DO DATAFRAME ---
if 'df_gastos' not in st.session_state:
    st.session_state.df_gastos = pd.DataFrame(columns=['grupo', 'categoria', 'gasto', 'valor', 'mes'])

# --- CRIAÇÃO DE COLUNAS PARA A INTERFACE ---
col1, col2, col3, col4, col5 = st.columns(5)

# --- SELEÇÃO DE GRUPO E GASTO ---
grupo = st.selectbox('*grupo*', options=cached_get_groups())

gastos = st.selectbox(
    '*gasto*',
    options=cached_get_expenses(),
    accept_new_options=True,
    placeholder="",
    index=None
)

# --- CAMPO DE SUGESTÃO DE CATEGORIA ---
suggested_category = None
if gastos:
    suggested_category = get_suggested_category(gastos)

if suggested_category is not None:
    st.write(f'categoria sugerida para "{gastos}": {suggested_category}')
else:
    st.write(f'nenhuma categoria encontrada para "{gastos}".')

# --- CAMPO PARA CATEGORIA ---
categoria = st.text_input(
    '*categoria*',
    value=suggested_category if suggested_category is not None else ""
)

# --- CAMPO DE SUGESTÃO DE VALOR ---
suggested_value = None
if gastos:
    suggested_value = get_suggested_value(gastos)

suggested_value_float = None
if suggested_value is not None:
    try:
        suggested_value_float = float(suggested_value)
    except (ValueError, TypeError):
        suggested_value_float = None

if suggested_value_float is not None:
    st.write(f'valor sugerido para "{gastos}": R$ {suggested_value_float:.2f}')
else:
    st.write(f'nenhum valor encontrado para "{gastos}".')

# --- CAMPO PARA INSERIR O VALOR ---
valor = st.number_input(
    '*valor*',
    min_value=0.0,
    value=suggested_value_float if suggested_value_float is not None else 0.0,
    step=0.01,
    format="%.2f"
)

st.write("---")  # Separador visual

# ----------------------------- BOTÕES DE ADICIONAR E REMOVER (LADO A LADO) -----------------------------
col_adicionar, col_remover = st.columns(2)

with col_adicionar:
    adicionar = st.button('➕ adicionar gasto', use_container_width=True)

with col_remover:
    remover = st.button('🗑️ remover selecionados', use_container_width=True)

# --- ADICIONA O GASTO ---
if adicionar and grupo and gastos:
    novo_gasto = {'grupo': grupo, 'categoria': categoria, 'gasto': gastos, 'valor': valor, 'mes': mes_atual}
    novo_gasto_clean = {key: novo_gasto.get(key, "") for key in st.session_state.df_gastos.columns}
    novo_df = pd.DataFrame([novo_gasto_clean]).astype(st.session_state.df_gastos.dtypes.to_dict(), errors='ignore')
    st.session_state.df_gastos = pd.concat([st.session_state.df_gastos, novo_df], ignore_index=True)

# ----------------------------- EXIBE O DATAFRAME -----------------------------
st.write(" ")  # Espaço visual

if not st.session_state.df_gastos.empty:
    df_with_selection = st.session_state.df_gastos.copy()
    df_with_selection['remover'] = False

    edited_df = st.data_editor(
        df_with_selection,
        use_container_width=True,
        hide_index=True,
        column_config={
            "remover": st.column_config.CheckboxColumn(
                label="remover?",
                help="marque para remover este gasto"
            )
        }
    )

    # --- REMOVE GASTOS SE CLICADO ---
    if remover:
        linhas_para_remover = edited_df[edited_df['remover'] == True].index
        st.session_state.df_gastos = st.session_state.df_gastos.drop(index=linhas_para_remover).reset_index(drop=True)
        st.rerun()
else:
    st.info("nenhum gasto adicionado ainda.")

# ----------------------------- BOTÃO GRANDE "ENVIAR PARA O SPLITWISE" -----------------------------
st.write(" ")  # Espaço visual
enviar = st.button('🚀 Enviar para o Splitwise', use_container_width=True)

if enviar:
    if st.session_state.df_gastos.empty:
        st.warning("adicione pelo menos um gasto antes de enviar.")
    else:
        resultados = []
        for idx, row in st.session_state.df_gastos.iterrows():
            try:
                # Monta a descrição como [categoria] gasto
                description = f"[{row['categoria']}] {row['gasto']}"
                
                # Chama a função create_expense
                response = create_expense(
                    cost=row['valor'],
                    description=description,
                    group_id="35336773",
                    details=None,
                    date=None,
                    repeat_interval=None,
                    currency_code=None,
                    category_id=None,
                    main_user_id=27512092,
                    main_user_paid_share=None,
                    main_user_owed_share=None,
                    other_users=None
                )
                resultados.append(f"gasto '{description}' enviado com sucesso")
            except Exception as e:
                resultados.append(f"erro ao enviar '{description}': {str(e)}")

        # Exibe o resultado de cada envio
        for r in resultados:
            st.success(r)

        # Limpa a tabela após enviar
        st.session_state.df_gastos = pd.DataFrame(columns=['grupo', 'categoria', 'gasto', 'valor', 'mes'])
