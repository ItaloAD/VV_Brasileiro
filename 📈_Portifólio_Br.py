import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import json
import numpy as np
import datetime as dt
from scipy.stats import norm
from math import sqrt, log, pow, erf, e

def converter_valor(valor):
    if valor is np.nan:
        return 0
    valor = valor.replace('$', '')
    valor = valor.replace('.', '')
    valor = valor.replace(',', '.')
    return float(valor)
    
def sheet_parser(csv_path, portifolio_list:list):
    df = pd.read_csv(csv_path, decimal=',')
    df = df[df['Portfólio'].isin(portifolio_list)]
    df['Data'] = pd.to_datetime(df['Data'],format='%d/%m/%Y')
    df['Qtde'] = df['Qtde'].apply(lambda x: float(x.replace('.','')))
    df = df[['Portfólio','Operação','Ativo','Tipo Oper', 'Data', 'Opção', 'Tipo Opt', 'Direção', 'Qtde', 'Prêmio', 'Total', 'Strike']]
    df['Prêmio'] = df['Prêmio'].apply(lambda x: round(x,2))
    df['Total'] = df['Total'].apply(converter_valor)
    df['Strike'] = df['Strike'].apply(converter_valor)
    return df

def formatar(texto):
    try: formatado = f'{texto:.2f}'.replace('.',',')
    except: formatado = texto
    return formatado

def formatar_valor(valor, tipo, branco=False):
    if tipo == 'moeda':
        if branco:
            return f'font-grande-branca>{valor:,.0f}'.replace(',','.')
        elif valor > 0:
            return f'font-grande-verde>{valor:,.0f}'.replace(',','.')
        else:
            return f'font-grande-vermelha>{valor:,.0f}'.replace(',','.')
    elif tipo == 'pct':
        if branco:
            return f'font-grande-branca>{valor:,.2f}%'.replace('.',',')
        if valor > 0:
            return f'font-grande-verde>{valor:,.2f}%'.replace('.',',')
        else:
            return f'font-grande-vermelha>{valor:,.2f}%'.replace('.',',')

def conectar_db_spec (user,password,host,db):
    return create_engine(f"mysql+pymysql://{user}:{password}@{host}/{db}")

def conectar_db(user,password,host):
    return create_engine(f"mysql+pymysql://{user}:{password}@{host}")

def fetch_data(engine, table_name):
    query = f"SELECT * FROM {table_name}"
    with engine.connect() as conn:
        data = pd.read_sql(query, conn)
    return data

st.set_page_config(
    layout="wide",
    page_title="Portifólio"
)

ip_publico = st.text_input("IP Público", "177.70.175.53")
porta = st.text_input("Porta", "3715")

user = "aws"
password = "123"
host = f"{ip_publico}:{porta}"
db_name = "portifolios"

engine = conectar_db_spec(user,password,host,db_name)

with engine.connect() as conn:
    data = pd.read_sql("SELECT * FROM ativos", conn)
    ativos = data.ativo.to_list()

df_planilha = fetch_data(engine, "controle")

df = fetch_data(engine, "portifolio_br")
df['ativo'] = df['codigo'].apply(lambda x: [ativo for ativo in ativos if x[:4] == ativo[:4]][0])
df.rename(columns={'preco_fechamento':'ultimo','preco_ativo':'spot'},inplace=True)

todos_contratos = df.groupby('codigo').agg(
    qtde=('qtde', 'sum'),
    strike=('strike', 'first'),
    tipo_opt=('categoria', 'first')
).reset_index()
todos_contratos['notional'] = todos_contratos['qtde']*todos_contratos['strike']*-1
notional = todos_contratos[(todos_contratos['tipo_opt'] == 'PUT') & (todos_contratos['qtde'] != 0)]['notional'].sum()

ciclos = list(df['expiracao'].drop_duplicates())

ciclo_selecionado = st.selectbox("Ciclo", ["Geral"]+ciclos)

sep_ativos = list(df['ativo'].drop_duplicates())
if ciclo_selecionado == "Geral":
    dfs = [df[df['ativo'] == ativo] for ativo in sep_ativos]
else:
    dfs = [df[(df['ativo'] == ativo) & (df['expiracao'] == ciclo_selecionado)] for ativo in sep_ativos if len(df[(df['ativo'] == ativo) & (df['expiracao'] == ciclo_selecionado)]) > 0]

contratos = list(todos_contratos[todos_contratos['qtde'] != 0].codigo)
dfs = [df_planilha[(df_planilha['Operação'] == op) & (df_planilha['Opção'].isin(contratos))] for op in df_planilha.Operação.unique()]
dfs = [df_un for df_un in dfs if len(df_un) > 0 and df_un['Qtde'].sum() != 0]
for i, df_un in enumerate(dfs):
    df_un = df_un.drop_duplicates(subset='Opção')[['Portfólio','Operação','Ativo','Opção','Tipo Opt']]
    df1 = df_un.sort_values('Opção')
    df2 = df[df['codigo'].isin(df_un.Opção)].sort_values('codigo')
    df_merged = df1.merge(df2, left_on='Opção', right_on='codigo')
    df_merged.pop('Tipo Opt')
    df_merged.pop('Opção')
    df_merged.rename(columns={'Portfólio':'portifolio', 'Operação':'operacao', 'Ativo':'ativo'}, inplace=True)
    dfs[i] = df_merged

show = st.sidebar.toggle("Visualizar Operações")
pela_vi = st.sidebar.toggle("Calculo com VI do Último Preço", value=True)

colunas_importantes = ['codigo','categoria','expiracao','strike','dte','spot','qtde','pm','ultimo','bid','ask', 'pl%', 'pl']
colunas_gregas = ['vi','vh','price','delta','theta','intrinseco','extrinseco']

tabela_geral = {
    'Portifólio': [],
    'Operação':[],
    'Ativo': [],
    'P/L %': [],
    'Resultado': [],
    'B.E. Put': [],
    'Strike Put': [],
    'Spot': [],
    'Strike Call': [],
    'B.E. Call': [],
    'Delta': [],
    'Theta': [],
    'Delta Beta': []
}

com_po = st.toggle("Resultados com pó", value=True)
premios_i = fetch_data(engine, "premios_iniciais")
capturas_theta = fetch_data(engine, "capturas_theta")
for dfa in dfs:
    ciclo = list(dfa.sort_values('qtde')['expiracao'])[0]
    ativo = dfa['ativo'].iloc[0][0]
    operacao = dfa['operacao'].iloc[0]
    pi = premios_i[premios_i['operacao'] == operacao]['pi_total'].iloc[0]
    ct_group = capturas_theta.groupby('operacao').agg({'captura':'sum','total':'sum','vencimento':'first'})
    if operacao in ct_group.index: 
        ct_cent = ct_group.loc[operacao, 'captura']
        ct = ct_group.loc[operacao, 'total']
    else: 
        ct_cent = 0
        ct = 0

    if pela_vi:
        dfa['delta'] = dfa['delta_vi']
        dfa['theta'] = dfa['theta_vi']
        dfa['price'] = dfa['price_vi']

    put = dfa[(dfa['categoria'] == 'PUT') & (dfa['qtde'] < 0)].sort_values('strike')
    call = dfa[(dfa['categoria'] == 'CALL') & (dfa['qtde'] < 0)].sort_values('strike', ascending=False)
    inv = False
    if len(put) > 0 and len(call) > 0:
        if put.strike.iloc[0] > call.strike.iloc[0]:
            inv = True
    if len(put) > 0: 
        if inv:
            be_put = round(call.strike.iloc[0] - ((dfa['qtde'] * dfa['pm']) / abs(dfa['qtde'])*-1).sum() - ct_cent,2)
        else: be_put = round(put.strike.iloc[0] - ((dfa['qtde'] * dfa['pm']) / abs(dfa['qtde'])*-1).sum() - ct_cent,2)
    else: be_put = "Infinito"
    if len(call) > 0: 
        if inv:
            be_call = round(put.strike.iloc[0] + ((dfa['qtde'] * dfa['pm']) / abs(dfa['qtde'])*-1).sum() + ct_cent,2)
        else: be_call = round(call.strike.iloc[0] + ((dfa['qtde'] * dfa['pm']) / abs(dfa['qtde'])*-1).sum() + ct_cent,2)
    else: be_call = "Infinito"

    delta = round((dfa['delta']*(dfa['qtde']/abs(dfa['qtde']))).sum()*100,2)
    delta_p = dfa['delta']*dfa['qtde']*(dfa['spot'].iloc[0])/100
    delta_beta = (delta_p*dfa['beta'].iloc[0]).sum()
    theta = round((dfa['theta']*dfa['qtde']).sum(),2)    
    
    maximo = dfa['pm']*dfa['qtde']*-1
    resultado_monetario = maximo-(dfa['ultimo']*dfa['qtde']*-1)
    dfa['pl'] = resultado_monetario
    dfa['pl%'] = round(resultado_monetario/maximo*100,2)
    dfa['pl%'] = dfa.apply(lambda row: str(row['pl%'] * -1)+"%" if row['qtde'] > 0 else str(row['pl%'])+"%", axis=1)
    
    if com_po:
        resultado = dfa['pl'].sum() + ct 
        pl_pct = resultado/pi*100
    else:
        sem_po = dfa[~((dfa['categoria'] == 'CALL') & (dfa['qtde'] > 0))]
        resultado = sem_po['pl'].sum() + ct
        pl_pct = resultado/pi*100

    tabela_geral['Portifólio'].append(ciclo)
    tabela_geral['Operação'].append(operacao)
    tabela_geral['Ativo'].append(ativo)
    tabela_geral['P/L %'].append(round(pl_pct,2))
    tabela_geral['Resultado'].append(resultado)
    tabela_geral['B.E. Put'].append(be_put)
    if be_put != "Infinito": tabela_geral['Strike Put'].append(put.strike.iloc[0])
    else: tabela_geral['Strike Put'].append("Infinito")
    tabela_geral['Spot'].append(dfa['spot'].iloc[0])
    if be_call != "Infinito": tabela_geral['Strike Call'].append(call.strike.iloc[0])
    else: tabela_geral['Strike Call'].append("Infinito")
    tabela_geral['B.E. Call'].append(be_call)
    tabela_geral['Delta'].append(delta)
    tabela_geral['Theta'].append(theta)
    tabela_geral['Delta Beta'].append(delta_beta)

    if show:
        col1, col2 = st.columns(2)
        col1.subheader(f"{ativo} / {ciclo}")
        col1.dataframe(dfa[colunas_importantes])

        col1.write(f"**P/L (R$):** {resultado:,.2f}")
        col1.write(f"**P/L (%):** {round(pl_pct,2)}%")
        col1.write(f"**B.E. =** {be_put}  /  {dfa['spot'].iloc[0]}  /  {be_call}")

        col2.subheader("GREGAS")
        col2.dataframe(dfa[colunas_gregas])

        col2.write(f"**Delta:** {delta}%")
        col2.write(f"**Delta/Beta:** {delta_beta:,.2f}")
        col2.write(f"**Theta:** {theta:,.2f}")
        st.divider()

st.markdown(
    """
    <style>
    .font-media-verde {
        font-size: 20px !important;
        text-align: right;
        color: #66FF00;
    }
    .font-media-vermelha {
        font-size: 20px !important;
        text-align: right;
        color: red;
    }
    .font-media-branca {
        font-size: 20px !important;
        text-align: left;
        color: white;
    }
    .font-grande-verde {
        font-size: 35px !important;
        text-align: right;
        color: #66FF00;
    }
    .font-grande-vermelha {
        font-size: 35px !important;
        text-align: right;
        color: red;
    }
    .font-grande-branca {
        font-size: 35px !important;
        text-align: right;
        color: white;
    }
    .font-media-branca-direita {
        font-size: 20px !important;
        text-align: right;
        color: white;
    }
    .font-negrito-branca {
        font-size: 20px !important;
        font-weight: bold;
        text-align: center;
        color: white;
    }
    .titulo-alinhado {
        font-size: 50px !important;
        text-align: center;
        font-weight: bold;
    }
    .texto-alinhado {
        font-size: 30px !important;
        text-align: center;
        font-weight: bold;
    }
    </style>
    """,
    unsafe_allow_html=True
)

c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12 = st.columns(12)
c1.markdown(f'<p class="font-negrito-branca">PORTIFÓLIO</p>', unsafe_allow_html=True)
c2.markdown(f'<p class="font-negrito-branca">ATIVO</p>', unsafe_allow_html=True)
c3.markdown(f'<p class="font-negrito-branca">P/L (%)</p>', unsafe_allow_html=True)
c4.markdown(f'<p class="font-negrito-branca">P/L (R$)</p>', unsafe_allow_html=True)
c5.markdown(f'<p class="font-negrito-branca">B.E. PUT</p>', unsafe_allow_html=True)
c6.markdown(f'<p class="font-negrito-branca">STK. PUT</p>', unsafe_allow_html=True)
c7.markdown(f'<p class="font-negrito-branca">SPOT</p>', unsafe_allow_html=True)
c8.markdown(f'<p class="font-negrito-branca">STK. CALL</p>', unsafe_allow_html=True)
c9.markdown(f'<p class="font-negrito-branca">B.E. CALL</p>', unsafe_allow_html=True)
c10.markdown(f'<p class="font-negrito-branca">DELTA</p>', unsafe_allow_html=True)
c11.markdown(f'<p class="font-negrito-branca">THETA</p>', unsafe_allow_html=True)
c12.markdown(f'<p class="font-negrito-branca">DELTA BETA</p>', unsafe_allow_html=True)

for i in range(len(tabela_geral['Ativo'])):
    itm = False
    if tabela_geral['B.E. Put'][i] != "Infinito":
        if tabela_geral['Spot'][i] < tabela_geral['B.E. Put'][i]: itm = True
    if tabela_geral['B.E. Call'][i] != "Infinito":
        if tabela_geral['Spot'][i] > tabela_geral['B.E. Call'][i]: itm = True
    sp = f'{tabela_geral["Spot"][i]:.2f}'.replace('.',',')
    if itm: spot_color = f'font-media-vermelha>{sp}'
    else: spot_color = f'font-media-branca-direita>{sp}'

    lucro = False
    if tabela_geral['Resultado'][i] >= 0:
        lucro = True
    
    if lucro:
        if tabela_geral["P/L %"][i] < 0:
            valor = tabela_geral["P/L %"][i] * -1
        else: valor = tabela_geral["P/L %"][i]
        pl = f'{valor:.2f}'.replace('.',',')
        pl_color = f'font-media-verde>{pl}%'
        res = f'{tabela_geral["Resultado"][i]:,.0f}'.replace(',','.')
        res_color = f'font-media-verde>{res}'
    else:
        pl = f'{tabela_geral["P/L %"][i]:.2f}'.replace('.',',')
        pl_color = f'font-media-vermelha>{pl}%'
        res = f'{tabela_geral["Resultado"][i]:,.0f}'.replace(',','.')
        res_color = f'font-media-vermelha>{res}'

    dir = False
    if tabela_geral['Delta'][i] >= 0:
        dir = True
    if dir: 
        d = f'{tabela_geral["Delta"][i]:.2f}'.replace('.',',')
        delta_color = f'font-media-verde>{d}%'
    else: 
        d = f'{tabela_geral["Delta"][i]:.2f}'.replace('.',',')
        delta_color = f'font-media-vermelha>{d}%'

    dir_db = False
    if tabela_geral['Delta Beta'][i] >= 0:
        dir_db = True
    
    if dir_db: db_color = f'font-media-verde>{tabela_geral["Delta Beta"][i]:,.0f}'.replace(',','.')
    else: db_color = f'font-media-vermelha>{tabela_geral["Delta Beta"][i]:,.0f}'.replace(',','.')

    c1.markdown(f'<p class="font-media-branca">{tabela_geral["Portifólio"][i]}</p>', unsafe_allow_html=True)
    c2.markdown(f'<p class="font-media-branca">{tabela_geral["Ativo"][i]}</p>', unsafe_allow_html=True)
    c3.markdown(f'<p class={pl_color}</p>', unsafe_allow_html=True)
    c4.markdown(f'<p class={res_color}</p>', unsafe_allow_html=True)
    c5.markdown(f'<p class="font-media-branca-direita">{formatar(tabela_geral["B.E. Put"][i])}</p>', unsafe_allow_html=True)
    c6.markdown(f'<p class="font-media-branca-direita">{formatar(tabela_geral["Strike Put"][i])}</p>', unsafe_allow_html=True)
    c7.markdown(f'<p class={formatar(spot_color)}</p>', unsafe_allow_html=True)
    c8.markdown(f'<p class="font-media-branca-direita">{formatar(tabela_geral["Strike Call"][i])}</p>', unsafe_allow_html=True)
    c9.markdown(f'<p class="font-media-branca-direita">{formatar(tabela_geral["B.E. Call"][i])}</p>', unsafe_allow_html=True)
    c10.markdown(f'<p class={delta_color}</p>', unsafe_allow_html=True)
    t = f'{tabela_geral["Theta"][i]:,.0f}'.replace(',','.')
    c11.markdown(f'<p class="font-media-branca-direita">{t}</p>', unsafe_allow_html=True)
    c12.markdown(f'<p class={db_color}</p>', unsafe_allow_html=True)

ct_group = capturas_theta.groupby('operacao').agg({'captura':'sum','total':'sum','vencimento':'first'})

planilha = pd.DataFrame.from_dict(tabela_geral)

#planilha.to_excel('C:/Users/Bruno/Desktop/OpLab API/Carteira.xlsx')

if ciclo_selecionado != 'Geral':
    ciclo_selecionado = ciclo_selecionado[8:]+"/"+ciclo_selecionado[5:7]+"/"+ciclo_selecionado[:4]

encerradas = {}
st.divider()

c1, c2, c3, c4, c5, c6 = st.columns(6)

c1.subheader("")
c2.subheader("ATIVO")
c3.subheader("P/L (%)")
c4.subheader("P/L (R$)")
c5.subheader("ENCERRAMENTO")
c6.subheader("CICLO")

for inicio in premios_i['operacao']:
    if inicio in tabela_geral['Operação']:
        continue
    else: 
        if inicio in list(ct_group.index):
            vencimento = ct_group.loc[inicio].vencimento
        else:
            continue
        if ciclo_selecionado != 'Geral':
            if ciclo_selecionado == vencimento:
                pass
            else:                
                continue

        pi = premios_i[premios_i['operacao'] == inicio].pi.iloc[0]
        qt = ct_group.loc[inicio].total / ct_group.loc[inicio].captura
        premio_total = pi * qt
        resultado_pct = ct_group.loc[inicio].total/premio_total*100
        encerradas[inicio] = ct_group.loc[inicio].total

        res = False
        if resultado_pct > 0:
            res = True
        
        if res: 
            res_color = f'font-media-verde>{ct_group.loc[inicio].total:,.2f}'
            res_pct_color = f'font-media-verde>{resultado_pct:,.2f}%'
        else: 
            res_color = f'font-media-vermelha>-{ct_group.loc[inicio].total*-1:,.2f}'
            res_pct_color = f'font-media-vermelha>{resultado_pct:,.2f}%'
        
        c1.markdown(f'<p class="font-media-branca">ENCERRADA -></p>', unsafe_allow_html=True)
        c2.markdown(f'<p class="font-media-branca">{premios_i[premios_i["operacao"] == inicio].ativo.iloc[0]}</p>', unsafe_allow_html=True)
        c3.markdown(f'<p class={res_pct_color}</p>', unsafe_allow_html=True)
        c4.markdown(f'<p class={res_color}</p>', unsafe_allow_html=True)
        c5.markdown(f'<p class="font-media-branca">{premios_i[premios_i["operacao"] == inicio].encerramento.iloc[0]}</p>', unsafe_allow_html=True)
        c6.markdown(f'<p class="font-media-branca">{vencimento}</p>', unsafe_allow_html=True)

if len(encerradas) == 0:
    encerradas = {'nada': 0}
    st.markdown(f"<p class='texto-alinhado'>NENHUMA OPERAÇÃO ENCERRADA NO CICLO</p>",unsafe_allow_html=True)
    
st.divider()

c1, c2, c3, c4, c5, c6, c7 = st.columns(7)

idp = round(sum(tabela_geral['Delta Beta'])/sum(tabela_geral['Theta']),2)
tp = round(sum(tabela_geral['Theta'])/25000000*100,2)
estoque_theta = (0.3-tp)/100*25000000
resultado_portifolio = sum(tabela_geral['Resultado'])+sum(list(encerradas.values()))
#notional = f"{notional:,.0f}"
somatorio_db = sum(tabela_geral['Delta Beta'])

#c1.metric(label="P/L (R$)", value=f"{resultado_portifolio:,.0f}".replace(',','.'))
#c2.metric(label="P/L (%)", value=f"{resultado_portifolio/25000000*100:.2f}%")
#c3.metric(label="IDP", value=f"{idp}%")
#c4.metric(label="TP", value=f"{tp}%")
#c5.metric(label="Som. de Theta", value=f"{sum(tabela_geral['Theta']):,.2f}".replace(',','.'))
#c6.metric(label="Notional", value=f"{notional.replace(',','.')}")
#c7.metric(label="Delta Beta", value=f"{somatorio_db:,.2f}".replace(',','.'))

c1.markdown(f'<p class="font-negrito-branca">P/L (R$)</p>', unsafe_allow_html=True)
c1.markdown(f'<p class={formatar_valor(resultado_portifolio, tipo="moeda")}</p>', unsafe_allow_html=True)
c2.markdown(f'<p class="font-negrito-branca">P/L (%)</p>', unsafe_allow_html=True)
c2.markdown(f'<p class={formatar_valor(resultado_portifolio/25000000*100, tipo="pct")}</p>', unsafe_allow_html=True)
c3.markdown(f'<p class="font-negrito-branca">IDP</p>', unsafe_allow_html=True)
c3.markdown(f'<p class={formatar_valor(idp, tipo="pct", branco=True)}</p>', unsafe_allow_html=True)
c4.markdown(f'<p class="font-negrito-branca">TP</p>', unsafe_allow_html=True)
c4.markdown(f'<p class={formatar_valor(tp, tipo="pct", branco=True)}</p>', unsafe_allow_html=True)
c5.markdown(f'<p class="font-negrito-branca">Somatório de Theta</p>', unsafe_allow_html=True)
c5.markdown(f'<p class={formatar_valor(sum(tabela_geral["Theta"]), tipo="moeda", branco=True)}</p>', unsafe_allow_html=True)
c6.markdown(f'<p class="font-negrito-branca">Notional (R$)</p>', unsafe_allow_html=True)
c6.markdown(f'<p class={formatar_valor(notional, tipo="moeda", branco=True)}</p>', unsafe_allow_html=True)
c7.markdown(f'<p class="font-negrito-branca">Somatório Delta Beta</p>', unsafe_allow_html=True)
c7.markdown(f'<p class={formatar_valor(somatorio_db, tipo="moeda", branco=True)}</p>', unsafe_allow_html=True)

st.divider()

capturas_ativos = list(capturas_theta.ativo.drop_duplicates())

st.markdown(f"<p class='titulo-alinhado'>Contratos Encerrados</p>",unsafe_allow_html=True)

col1, col2 = st.columns(2)

for ativo, i in zip(capturas_ativos, range(1, len(capturas_ativos)+1)):
    if i % 2 == 0: 
        coluna = col2
    else: coluna = col1

    aux = capturas_theta[capturas_theta['ativo'] == ativo]
    aux.set_index('codigo',inplace=True)
    
    c1,c2,c3 = coluna.columns(3)
    c1.subheader("**CÓDIGO**")
    c2.subheader("**CT**")
    c3.subheader("**P/L**")

    for codigo in list(aux.index):
        c1.subheader(codigo)
        positivo = False
        if aux.loc[codigo, 'captura'].sum() > 0:
            captura = f":green[{aux.loc[codigo,'captura'].sum():,.2f}]"
            total = f":green[{aux.loc[codigo,'total'].sum():,.2f}]"
        else: 
            captura = f":red[{aux.loc[codigo,'captura'].sum():,.2f}]"
            total = f":red[-{aux.loc[codigo,'total'].sum()*-1:,.2f}]"
        c2.subheader(captura)
        c3.subheader(total)
    
    positivo = False
    if aux.captura.sum() > 0:
        captura = f":green[{aux.captura.sum():,.2f}]"
        total = f":green[{aux.total.sum():,.2f}]"
    else: 
        captura = f":red[{aux.captura.sum():,.2f}]"
        total = f":red[-{aux.total.sum()*-1:,.2f}]"
    c1.subheader("---------------")
    c2.subheader("---------------")
    c3.subheader("---------------")
    c1.subheader("**TOTAL**")
    c2.subheader(captura)
    c3.subheader(total)

    coluna.divider()
