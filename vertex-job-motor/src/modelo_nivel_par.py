from modelo_nivel_par_lib import *
import warnings
import pandas as pd
from prophet import Prophet
from datetime import datetime, timedelta, date
import pandas as pd
import logging
#logging.getLogger("cmdstanpy").disabled = True #  turn 'cmdstanpy' logs off
import math
#from motor_nivel_par_queries import *
import multiprocessing
import time
from google.cloud import bigquery
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context
import os
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/support_files/projeto-smart-break.json"


def execucao_motor():

    start_time = time.time()

    pd.options.mode.chained_assignment = None  # default='warn'
    warnings.simplefilter(action='ignore', category=FutureWarning)





    start_time = time.time()

    pd.options.mode.chained_assignment = None  # default='warn'
    warnings.simplefilter(action='ignore', category=FutureWarning)





    #Contador de tempo
    start_time = time.time()


    #apply_convolution(['fewfrereer'])

    # Definições de ambiente
    Hoje=date.today()
    Percentual_Shelf_Life=0.4 #Atua na formula percentual_shelf * Shelf *Venda Media
    Loja='Full' #O parametro pode ser Full ou Important(Partial), Full roda para todas as lojas, ja important apenas lojas pre selecionadas
    Periodo_Analise_Historico=90 # Quantos dias corridos consideramos para as analises
    #Locais=(136110,136173)
    Locais=(137066, 137523, 136745, 136337)  # Lojas a serem moeladas no partial
    # Locais=(136110,136173,136565,137066,137523,137524,136997,137051,136951,137529,137531,136970,136602,136516,137019,137008,137539,136384,137003,144780,137020,136621,137045,137549,136721,137095,136600,137067,141012,136731,136386,136528,142078,139167,139035,137062,136651,138744,140101,136876,136757,136391,137080,136723,136738,137361)
    # colocar a variavel locais por meio de arg parse ou pelo sys, passando um vetor de bibliotecas.
    # tabela com todas as lojas com status ativo/inativo
    # LOGICA: garantir que consigo fazer uma execução do códico no Vertex passando o número mínimo de lojas
    # script consultando lojas ativas e o resultado da query com lojas ativas a cada 2 lojas cria um novo Job, pode dar problema rodar com uma só.
    # No futuro, fazer um filtro de produtos.
    
    Ultimos_90_Dias=str(Hoje - timedelta(Periodo_Analise_Historico))

    if Loja=="Full":
        print(f'Baixando full {Loja}')
        query_historico_transacoes="select * from curated_ds.vw_base_transacional_agg_sem_ruptura  where dat_order>='"+Ultimos_90_Dias+"'  order by dat_order"
        Query_StockOut="SELECT id_location, id_sku, dat_faturamento, qtd_stock_out_items, horas_rupturas_30_dias,round(vlr_numero_vendas_hora, 8)*24  as media FROM curated_ds.tb_metricas_modelo WHERE date(dat_faturamento) >='"+ str(Ultimos_90_Dias) + "'"
    else:
        print(f'Baixando lojas parciais {Locais}')
        query_historico_transacoes="select * from curated_ds.vw_base_transacional_agg_sem_ruptura  where dat_order>='"+Ultimos_90_Dias+"' and id_location in " +str(Locais)+ " order by dat_order"
        Query_StockOut="SELECT id_location, id_sku, dat_faturamento, qtd_stock_out_items, horas_rupturas_30_dias,round(vlr_numero_vendas_hora, 8)*24  as media FROM curated_ds.tb_metricas_modelo WHERE date(dat_faturamento) >='"+ str(Ultimos_90_Dias) +"' and id_location in " + str(Locais)

    #query_lojas_inativas= 'SELECT id_location, flg_inactive FROM processed.tb_kpi_lojas'
    #query_Filtro_SKU_Ativo='SELECT id_sku,des_status FROM processed.dim_products_cd'


    #Apagar bloco
    '''
    #Faremos de outra forma
    if Loja=='Full':
        #Query_StockOut="SELECT id_location, id_sku, dat_faturamento AS dat_order, qtd_stock_out_items, horas_rupturas_30_dias FROM CURATED_DS.tb_metricas_modelo WHERE dat_order >='"+ str(Ultimos_90_Dias) +"'"
    else:
        Query_StockOut="SELECT id_location, id_sku, dat_faturamento, qtd_stock_out_items, horas_rupturas_30_dias,round(vlr_numero_vendas_hora, 8)*24  as media FROM CURATED_DS.tb_metricas_modelo WHERE date(dat_faturamento) >='"+ str(Ultimos_90_Dias) +"' and id_location in " + str(Locais)
    #print(Query_StockOut)
    '''



    #Baixando os dados necessarios para alimentar o motor do banco
    print('Importando dados do BigQuery...')

    def read_sql(query, conn=None):
        client = bigquery.Client()
        print("Projeto autenticado:", client.project)
        return client.query(query).result().to_dataframe()

    
    # Baixando os dados necessários
    print('Importando dados do BigQuery...')

    Reabastecimento = read_sql(query_Reabastecimento)
    print(f'Baixando dados de reabastecimento {Reabastecimento.shape}')

    Stockout = read_sql(Query_StockOut)
    print(f'Baixando dados de stockout {Stockout.shape}')

    Prod = read_sql("SELECT id_sku, des_department FROM processed.dim_products_cd")
    print(f'Baixando dados de produtos {Prod.shape}')

    historico_vendas = read_sql(query_historico_transacoes).rename(columns={'max': 'periodo_tokenizado'})
    print(f'Baixando dados de vendas {historico_vendas.shape}')
    historico_vendas['dat_order'] = pd.to_datetime(historico_vendas['dat_order'])

    rupturas = read_sql(Query_Dados_Ruptura).rename(columns={'max': 'periodo_tokenizado'})
    print(f'Baixando dados de detalhes de stockout {rupturas.shape}')
    rupturas['dat_order'] = pd.to_datetime(rupturas['dat_order'])

    relacao_id_produto = read_sql(Query_Relacao_id_Produto)
    print(f'Baixando dados de detalhes de relacao id produtos {relacao_id_produto.shape}')

    relacao_id_loja = read_sql(Query_Relacao_id_Loja)
    print(f'Baixando dados de detalhes das lojas {relacao_id_loja.shape}')

    Valor_Par = read_sql(Query_Valor_Par)
    print(f'Baixando dados de nivel par {Valor_Par.shape}')

    planograma = read_sql(Query_Planograma)
    print(f'Baixando dados de planogramas {planograma.shape}')

    Tipos = read_sql("SELECT id_sku, des_perecividade FROM processed.dim_products_cd WHERE des_perecividade != 'Pereciveis'")
    print(f'Baixando dados de tipos {Tipos.shape}')

    print('Dados importados com sucesso')


    Outros=set(Prod['des_department'].unique())- set(['CONGELADOS'])
    Prod['des_department'].replace(list(Outros),'Outros',inplace=True)

    print("historico v")
    print(historico_vendas.dtypes)
    print("rupturas")
    print(rupturas.dtypes)


    historico_vendas.groupby(['id_location','id_sku'])['ind_curva'].nunique().reset_index().query('ind_curva>1')
    historico_vendas=historico_vendas.query('des_item_status=="active"')

    historico_vendas.groupby(['id_sku','id_location'])['total_sales'].sum().sort_values()
    Teste=historico_vendas.query('id_location==136110 and id_sku==3542608').rename(columns={'dat_order':'ds','total_sales':'y'})[['ds','y']]

    rupturas['dat_ended'] = pd.to_datetime(rupturas['dat_ended']).dt.date

    delta=rupturas['dat_ended']-rupturas['dat_back']
    rupturas['time_rupture']=[int(round(i.days*24 + i.seconds/3600,0)) for i in delta]
    rupturas['time_rupture']=rupturas[['time_rupture','time_rupture2']].min(axis=1)

     
    rupturas=rupturas.groupby(['dat_order'	,'id_sku','id_location','total_sales','periodo_tokenizado'],as_index=False)['time_rupture'].sum()
    historico_vendas=historico_vendas.merge(rupturas,how='left').fillna(0)
    print(historico_vendas.dtypes)



    #removendo produtos pereciveis
    historico_vendas=historico_vendas[historico_vendas['id_sku'].isin(Tipos['id_sku'])]

    Produtos_Candidatos=historico_vendas[['id_location','id_sku']].drop_duplicates()
    Produtos_Vendidos=historico_vendas.query('dat_order>=@Ultimos_30_Dias')[['id_location','id_sku']].drop_duplicates()

    Produtos_Vendidos['Vendeu']=1
    Produtos_Vendidos=Produtos_Vendidos.merge(Produtos_Candidatos,how='right')


    '''
    Produtos_Vendidos[Produtos_Vendidos['Vendeu'].isna()].merge(relacao_id_produto).\
        merge(relacao_id_loja).\
        merge(produto_categoria).to_csv('Produtos_Nao_Vendidos_Ultimos_30_Dias.csv')

    #Consistencia=pd.read_csv('Produtos_Nao_Vendidos_Ultimos_30_Dias.csv').shape
    '''


    Ranking=historico_vendas.groupby(['id_location','id_sku'])['periodo_tokenizado'].mean().sort_values()

    Ranking=Ranking.reset_index()
    Ranking['periodo_tokenizado']=round(Ranking['periodo_tokenizado'],0)


    #Visitas considerando congelados e nao congelados
    visitas=Reabastecimento.merge(Prod)
    visitas['dat_visit']=[visitas.loc[i,'dat_visit'].date() for i in visitas.index]
    visitas=visitas.drop_duplicates(subset=['id_location','dat_visit','des_department'])

    visitas['proxima']=visitas.groupby(['id_location','des_department'])['dat_visit'].shift(1)
    visitas=visitas.dropna()
    visitas['Dias_Para_Abastecer']=(visitas['proxima']-visitas['dat_visit']).astype('str').str.split(' ',expand=True)[0].astype('int')

    # print("Visitas ----> " + visitas['Dias_Para_Abastecer'])

    Dias_Proxima_Visita=visitas.groupby(['id_location','des_department'])['Dias_Para_Abastecer'].mean().apply(lambda x: math.ceil(x)).reset_index()

    # Lista de lojas com valores fixos
    Lista_Lojas = [
        137066, 137523, 136745, 136337, 136460, 145863, 136850, 136875, 140706, 136661,
        136779, 141107, 140544, 137095, 139869, 137067, 136714, 141103, 136528, 136744,
        139167, 140922, 136490, 137064, 137062, 138415, 136654, 136845, 136520, 136757,
        136228, 185065, 137588, 136669, 136828
    ]

    Secos = pd.DataFrame({
        'id_location': Lista_Lojas,
        'des_department': 'Outros',
        'Dias_Para_Abastecer': 7
    })

    Congelados = pd.DataFrame({
        'id_location': Lista_Lojas,
        'des_department': 'Congelados',
        'Dias_Para_Abastecer': 30
    })

    sobrescrita_dias = pd.concat([Secos, Congelados])

    #modelagem
    Dias_Proxima_Visita=historico_vendas.groupby(['id_location','id_sku'])['total_sales'].sum().sort_values(ascending=False).reset_index().merge(Prod).merge(Dias_Proxima_Visita)
    
    #AJUSTE GUILHERME 
    Dias_Proxima_Visita['Dias_Para_Abastecer'] = Dias_Proxima_Visita['Dias_Para_Abastecer'].abs()
    
    #AJUSTE GUILHERME 
    Dias_Proxima_Visita.loc[
        Dias_Proxima_Visita['Dias_Para_Abastecer'] == 0, 'Dias_Para_Abastecer'
    ] = 1

    Dias_Proxima_Visita.groupby(['id_location','des_department'])['Dias_Para_Abastecer'].agg(['mean','std'])


    # Substitui os valores fixos nas lojas desejadas
    Dias_Proxima_Visita = Dias_Proxima_Visita.merge(sobrescrita_dias, on=['id_location', 'des_department'], how='left') \
        .assign(Dias_Para_Abastecer=lambda df: df['Dias_Para_Abastecer_y'].combine_first(df['Dias_Para_Abastecer_x'])) \
        .drop(columns=['Dias_Para_Abastecer_x', 'Dias_Para_Abastecer_y'])

    print(Dias_Proxima_Visita.query('id_location == 137066')[['id_location', 'des_department', 'Dias_Para_Abastecer']])

    Dias_Proxima_Visita=Dias_Proxima_Visita.drop(columns=['des_department'])

 


    # print("Calculado Visitas ----> " + Dias_Proxima_Visita)

    #O tempo de analise de cada produto varia de acordo com sua curva:
    #   curva A = 60 dias, convolução pico
    #   curva B = 30 dias, convolução pico
    #   curva A = 30 dias, convolução media
    #


    historico_vendas['dat_order']=pd.to_datetime(historico_vendas['dat_order'])
    historico_vendas['dat_order'].max()

    Stockout.groupby(['id_location','id_sku']).size().min()
    Stockout.rename(columns={'dat_faturamento':'dat_order'},inplace=True)

    if pd.api.types.is_datetime64tz_dtype(Stockout['dat_order']):
        Stockout['dat_order'] = Stockout['dat_order'].dt.tz_localize(None)

    Analise_30_Dias=historico_vendas.merge(Stockout,how='right')
    Analise_30_Dias['total_sales'].fillna(0,inplace=True)

    Analise_30_Dias=Analise_30_Dias[['id_location',
                                    'id_sku',
                                    'total_sales',
                                    'dat_order',
                                    'qtd_stock_out_items']]

    Analise_30_Dias=Analise_30_Dias.merge(historico_vendas[['id_sku','id_location','ind_curva']].drop_duplicates())


    # Curva Produtos
    Curva_A=Analise_30_Dias[Analise_30_Dias['ind_curva']=='A']
    Curva_B=Analise_30_Dias[Analise_30_Dias['ind_curva']=='B']
    Curva_C=Analise_30_Dias[Analise_30_Dias['ind_curva']=='C']

    #Filtro Temporal
    Curva_A=Curva_A.query('dat_order>=@Ultimos_60_Dias')
    Curva_B=Curva_B.query('dat_order>=@Ultimos_30_Dias')
    Curva_C=Curva_C.query('dat_order>=@Ultimos_30_Dias')



    Curva_A=Curva_A.merge(Dias_Proxima_Visita.drop('total_sales',axis=1))
    Curva_B=Curva_B.merge(Dias_Proxima_Visita.drop('total_sales',axis=1))
    Curva_C=Curva_C.merge(Dias_Proxima_Visita.drop('total_sales',axis=1))



    Curva_A=Curva_A[['id_location','id_sku','total_sales','dat_order','Dias_Para_Abastecer','qtd_stock_out_items']]
    Curva_B=Curva_B[['id_location','id_sku','total_sales','dat_order','Dias_Para_Abastecer','qtd_stock_out_items']]
    Curva_C=Curva_C[['id_location','id_sku','total_sales','dat_order','Dias_Para_Abastecer','qtd_stock_out_items']]

    Curva_A=Curva_A.drop_duplicates()
    Curva_B=Curva_B.drop_duplicates()
    Curva_C=Curva_C.drop_duplicates()

    ##inclusão de tratamento GUILHERME
    Curva_A = Curva_A[Curva_A['total_sales'] > 0]
    Curva_B = Curva_B[Curva_B['total_sales'] > 0]
    Curva_C = Curva_C[Curva_C['total_sales'] > 0]

    ##inclusão de tratamento GUILHERME
    Curva_A = Curva_A.reset_index(drop=True)
    Curva_B = Curva_B.reset_index(drop=True)
    Curva_C = Curva_C.reset_index(drop=True)

    ##inclusão de tratamento GUILHERME
    Curva_A['Dias_Para_Abastecer'] = Curva_A['Dias_Para_Abastecer'].astype(int)
    Curva_B['Dias_Para_Abastecer'] = Curva_B['Dias_Para_Abastecer'].astype(int)
    Curva_C['Dias_Para_Abastecer'] = Curva_C['Dias_Para_Abastecer'].astype(int)


    #Fazendo processamento paralelo
    Pico_Curva_A=Curva_A.groupby(['id_sku','id_location']).apply(apply_convolution)
    Pico_Curva_B=Curva_B.groupby(['id_sku','id_location']).apply(apply_convolution)
    Pico_Curva_C=Curva_C.groupby(['id_sku','id_location']).apply(apply_convolution)


    #Pico_Curva_A.to_csv('picoA_teste_7.csv')
    #print(Pico_Curva_A)
    print('-----')
    Pico_Curva_A=Pico_Curva_A.reset_index().drop('level_2',axis=1)
    Pico_Curva_B=Pico_Curva_B.reset_index().drop('level_2',axis=1)
    Pico_Curva_C=Pico_Curva_C.reset_index().drop('level_2',axis=1)

    Pico=pd.concat([Pico_Curva_A,Pico_Curva_B,Pico_Curva_C])
    Pico.rename(columns={'Total':'max','Vendas':'total_sales','Stockout':'qtd_stock_out_items'},inplace=True)

    #Ordenando os valores
    Stockout = Stockout.sort_values(by='dat_order')
    Modelo_Media=Stockout.groupby(['id_location','id_sku']).tail(1)[['id_location','id_sku','media']]

    pd.options.mode.chained_assignment = None


    Maximo=Pico
    Analise_30_Dias=Maximo.merge(Modelo_Media)



    Analise_30_Dias.rename(columns={'media':'Media'},inplace=True)
    Analise_30_Dias=historico_vendas[['id_sku','id_location','vlr_prazo_shelf_life_dias']].drop_duplicates().merge(Analise_30_Dias,how='right')
    Analise_30_Dias.loc[Analise_30_Dias['vlr_prazo_shelf_life_dias']==0,'vlr_prazo_shelf_life_dias'] =1e6
    Analise_30_Dias=Dias_Proxima_Visita.drop(columns='total_sales').merge(Analise_30_Dias)  

    #AJUSTE GUILHERME
    Analise_30_Dias['Dias_Para_Abastecer'] = Analise_30_Dias['Dias_Para_Abastecer'].astype(int)


    #Modelo heuristico - aplicação da equação
    Analise_30_Dias['Modelo_Media_Visita']=Analise_30_Dias['Media'] * Analise_30_Dias['Dias_Para_Abastecer']
    Analise_30_Dias['Modelo_pico_Visita']=Analise_30_Dias['max'] #* Analise_30_Dias['Dias_Para_Abastecer']

    Analise_30_Dias['Modelo_Media_Shelf']=Analise_30_Dias['Media'].astype(float) * Analise_30_Dias['vlr_prazo_shelf_life_dias'].astype(float)
    Analise_30_Dias=Analise_30_Dias.dropna()

    Analise_30_Dias['Modelo_Media_Shelf']=Analise_30_Dias['Modelo_Media_Shelf']*Percentual_Shelf_Life

    Analise_30_Dias['Modelo_Media_Visita'] = Analise_30_Dias['Modelo_Media_Visita'].apply(lambda x: int(math.ceil(x)))
    Analise_30_Dias['Modelo_Media_Shelf'] = Analise_30_Dias['Modelo_Media_Shelf'].apply(lambda x: int(math.ceil(x)))
    Analise_30_Dias['Modelo_pico_Visita'] = Analise_30_Dias['Modelo_pico_Visita'].apply(lambda x: int(math.ceil(x)))

    Analise_30_Dias=Analise_30_Dias.rename(columns={'max':'pico_30_dias',
                                                    'vlr_prazo_shelf_life_dias':'shelf_life',
                                                    'qtd_stock_out_items':'Stockout',
                                                    'total_sales':'Pico_Vendas_30_Dias',
                                                    'Media':'Media_Vendas_30_dias'})\
                                                                                    [['id_sku',
                                                                                    'id_location',
                                                                                    'Media_Vendas_30_dias',
                                                                                    'shelf_life',
                                                                                    'pico_30_dias',
                                                                                    'Stockout',
                                                                                    'Pico_Vendas_30_Dias',
                                                                                    'Dias_Para_Abastecer',
                                                                                    'Modelo_Media_Visita',
                                                                                    'Modelo_pico_Visita',
                                                                                    'Modelo_Media_Shelf']]


    #Modelo estatistico
    dados=historico_vendas

    dados=Dias_Proxima_Visita[['id_location','id_sku','Dias_Para_Abastecer']].merge(dados)
    dados=dados.rename(columns={'total_sales':'y','dat_order':'ds'})
    Teste=historico_vendas[['id_location','id_sku']].drop_duplicates()

    Frequencia=(dados.groupby(['id_sku','id_location']).size()>2).reset_index()
    Frequencia=Frequencia[Frequencia[0]][['id_sku','id_location']]
    Frequencia=Frequencia.merge(Analise_30_Dias[['id_sku','id_location']])
    dados=dados.merge(Frequencia.drop_duplicates())

    Teste=dados[['id_sku','id_location']].drop_duplicates()

    

    dados_paralelos=[]
    for i in Teste.index:
        location=Teste.loc[i,'id_location']
        sku=Teste.loc[i,'id_sku']
        Amostra=dados.query('id_location==@location and id_sku==@sku')[['ds','y','time_rupture','id_sku','Dias_Para_Abastecer','id_location']].drop_duplicates(subset=['ds','y'])
        if Amostra.shape[0]>2:
            dados_paralelos.append(Amostra)

    print("passou 1")

    teste=dados_paralelos[0]

    Modelo = Prophet(interval_width=0,uncertainty_samples=0) #Captura mudancas de tendencia ate o ultimo momento
    Modelo.add_country_holidays(country_name='BR') #Feriados Brasileiros
    Modelo.add_regressor('time_rupture') # Tempo em ruptura nos ultimos 15 dias
    Modelo.add_seasonality(name='monthly', period=30.5, fourier_order=5)


    teste=pd.DataFrame({'ds':[pd.to_datetime(Ultimos_90_Dias) + timedelta(i) for i in range(91)]}).merge(teste,how='left') 
    teste['y'].fillna(0,inplace=True) #Zero Padding
    teste["time_rupture"] = (teste["time_rupture"].astype("float").interpolate(method="nearest")) #round().astype("Int64"))

    #teste['time_rupture']=teste['time_rupture'].interpolate(method='nearest')
    teste['time_rupture']=teste['time_rupture'].fillna(0)
            #print('foi')
    Modelo.fit(teste)
    #forecast=Modelo.predict(teste)

    pool = multiprocessing.Pool(processes=6)
    Output = pool.map(Gerar_Previsao_valor_par_suporte_paralelo, dados_paralelos)

    # Close the pool of worker processes
    pool.close()
    pool.join()

    '''
    import pickle
    with open('output.pkl', 'wb') as f:
    pickle.dump(Output, f)
    '''

    print("passou 2")

    pool = multiprocessing.Pool(processes=6)
    Output_15Dias = pool.map(Gerar_Previsao_valor_par_suporte_paralelo_15Dias, dados_paralelos)

    # Close the pool of worker processes
    pool.close()
    pool.join()




    for i in range(len(dados_paralelos)):
        location=dados_paralelos[i]['id_location'].values[0]
        sku=dados_paralelos[i]['id_sku'].values[0]
        Output[i]['id_location']=location
        Output[i]['id_sku']=sku

    for i in range(len(dados_paralelos)):
        try:
            location=dados_paralelos[i]['id_location'].values[0]
            sku=dados_paralelos[i]['id_sku'].values[0]
            Output_15Dias[i]['id_location']=location
            Output_15Dias[i]['id_sku']=sku
        except :
            print(i)
            pass
            

    Previsoes=pd.concat(Output)
    Previsoes15dias=pd.concat(Output_15Dias)
    
    Previsoes=Previsoes[['yhat','yhat_lower','yhat_upper','id_sku','id_location']]
    Previsoes15dias=Previsoes15dias[['yhat','yhat_lower','yhat_upper','id_sku','id_location']]

    print("passou 3")
    Previsoes['Dia']=1
    Previsoes15dias['Dia']=1
    Previsoes[['yhat_lower','yhat','yhat_upper']]=Previsoes[['yhat_lower','yhat','yhat_upper']] *(Previsoes[['yhat_lower','yhat','yhat_upper']]>0)
    Previsoes15dias[['yhat_lower','yhat','yhat_upper']]=Previsoes15dias[['yhat_lower','yhat','yhat_upper']] *(Previsoes15dias[['yhat_lower','yhat','yhat_upper']]>0)

    #Previsoes=Previsoes[['id_sku','id_location']]
    Previsoes=Previsoes.groupby(['id_sku','id_location'],as_index=False).sum().round(0).astype('int')

    Previsoes15dias=Previsoes15dias.groupby(['id_sku','id_location'],as_index=False).sum().round(0).astype('int')

    Valor_Par=Valor_Par.drop_duplicates(subset=['id_sku','id_location'],keep='last')

    Tabela_Saida=Previsoes.merge(Valor_Par[['id_location','id_sku','valor_par']],how='left').\
    rename(columns={'yhat_lower':'Valor_Par_Minimo','yhat':'Valor_Par_Estimado','yhat_upper':'Valor_Par_Maximo','valor_par':'Valor_Par_Utilizado'})

    Tab=Tabela_Saida

    Tabela_Saida['Dist'] = abs(Tabela_Saida['Valor_Par_Utilizado'] - Tabela_Saida['Valor_Par_Estimado'])

    print("passou 4")

    Tab = Tabela_Saida

    Tab['Dist']=abs(Tab['Valor_Par_Utilizado']-Tab['Valor_Par_Estimado'])
    Saida_Banco=Tab.merge(relacao_id_produto).merge(relacao_id_loja).merge(Ranking).sort_values('Dist').dropna()


    Saida_Banco = Saida_Banco.merge(planograma,how='left')

    Saida_Banco = Saida_Banco[['Valor_Par_Estimado'	,
        'Dia','Valor_Par_Utilizado','des_sku_name',
        'des_name','periodo_tokenizado','vlr_capacity_item','vlr_item_alert_level','id_sku','id_location']]


    Saida_Banco = Saida_Banco.merge(Previsoes15dias[['yhat','id_sku','id_location']],how='left').rename(columns={'yhat':'Valor_Par_15_Dias'}).fillna(0)
    Colunas_Float = Saida_Banco.dtypes[Saida_Banco.dtypes==float].index.to_list()

    Saida_Banco[Colunas_Float] = Saida_Banco[Colunas_Float].astype('int')

    #AJUSTE DIEGO
    Analise_30_Dias_final = (
        Analise_30_Dias
        .sort_values('Pico_Vendas_30_Dias', ascending=False)
        .drop_duplicates(subset=['id_sku', 'id_location'], keep='first'))

    Saida_Banco = Saida_Banco.merge(Analise_30_Dias,on=['id_sku','id_location'],how='left').fillna(0)
    aux = Saida_Banco.merge(historico_vendas[['id_sku','vlr_prazo_shelf_life']].drop_duplicates())
    Saida_Banco['shelf_life']=aux[['shelf_life','vlr_prazo_shelf_life']].max(axis=1)

    Saida_Banco = Saida_Banco.drop_duplicates().reset_index(drop=True)

    print("passou 5")
    logico = Saida_Banco['shelf_life']==0
    Saida_Banco.loc[logico,'Modelo_Media_Shelf']=1e6
    Saida_Banco.loc[logico,'Modelo_Media_Shelf']

    Saida_Banco=Saida_Banco.merge(dados[['ind_curva','id_location','id_sku']].drop_duplicates())

    logico=Saida_Banco['ind_curva']=='C'
    Saida_Banco.loc[logico,'Modelo_pico_Visita']=Saida_Banco.loc[logico,'Modelo_Media_Visita'] #+Saida_Banco.loc[logico,'Stockout']

    Saida_Banco['media_venda_30_dias'] = Saida_Banco[['Media_Vendas_30_dias']]#(Saida_Banco['Modelo_Media_Visita']/Saida_Banco['Dias_Para_Abastecer']).fillna(0)

    print("ind curva resultado final")
    print(Saida_Banco['ind_curva'].value_counts())


    Saida_Banco['Valor_Par_Utilizado'] = Saida_Banco['Valor_Par_Utilizado'].astype(int)
    Saida_Banco['vlr_capacity_item'] = Saida_Banco['vlr_capacity_item'].astype(int)
    Saida_Banco['vlr_item_alert_level'] = Saida_Banco['vlr_item_alert_level'].astype(int)
    Saida_Banco['shelf_life'] = Saida_Banco['shelf_life'].astype(int) 

    logico = Saida_Banco['Valor_Par_Utilizado'] < Saida_Banco['vlr_item_alert_level']
    if logico.sum()>0:
        Saida_Banco.loc[logico,'vlr_item_alert_level']= 0.6*Saida_Banco.loc[logico,'Valor_Par_Utilizado']


    Saida_Banco['Pico_Previsao']=Saida_Banco[['Modelo_pico_Visita','Valor_Par_Estimado','Valor_Par_15_Dias']].max(axis=1)
    Saida_Banco['limitador']=Saida_Banco[['Pico_Previsao','Modelo_Media_Shelf','vlr_capacity_item']].min(axis=1)
    Saida_Banco['sugestao_modelo']=Saida_Banco[['limitador','vlr_item_alert_level']].max(axis=1)
    Saida_Banco['sugestao_modelo']=Saida_Banco['sugestao_modelo'].astype('int')

    print(f"Exportando o resultado {Saida_Banco.shape}")
    Saida_Banco.rename(columns={'Unnamed: 0' : 'index'}, inplace=True)
    '''
    Saida_Banco = Saida_Banco[['des_sku_name','des_name',
                                'Valor_Par_Utilizado','Modelo_Media_Shelf',
                                'media_venda_30_dias','shelf_life',
                                'Modelo_pico_Visita','pico_30_dias',
                                'Stockout','Pico_Vendas_30_Dias',
                                'Dia','Valor_Par_Estimado','Valor_Par_15_Dias',
                                'Modelo_Media_Visita','sugestao_modelo',
                                'vlr_capacity_item','vlr_item_alert_level','id_sku','id_location','ind_curva']]
    '''

    Saida_Banco = Saida_Banco[['des_sku_name','des_name',
                                'Valor_Par_Utilizado','Modelo_Media_Shelf',
                                'media_venda_30_dias','shelf_life',
                                'Modelo_pico_Visita','pico_30_dias',
                                'Stockout','Pico_Vendas_30_Dias',
                                'Dia','Valor_Par_Estimado','Valor_Par_15_Dias',
                                'Modelo_Media_Visita','sugestao_modelo',
                                'vlr_capacity_item','vlr_item_alert_level','id_sku','id_location','ind_curva', 'Pico_Previsao']]
    

    Saida_Banco['Modelo_Media_Shelf'] = Saida_Banco['Modelo_Media_Shelf'].astype(int)
    Saida_Banco['media_venda_30_dias'] = Saida_Banco['media_venda_30_dias'].astype(float).round(2)
    Saida_Banco['shelf_life'] = Saida_Banco['shelf_life'].astype(int)
    Saida_Banco['Modelo_pico_Visita'] = Saida_Banco['Modelo_pico_Visita'].astype(int)
    Saida_Banco['Stockout'] = Saida_Banco['Stockout'].apply(np.ceil).astype(int)
    Saida_Banco['Pico_Vendas_30_Dias'] = Saida_Banco['Pico_Vendas_30_Dias'].astype(int)
    Saida_Banco['Modelo_Media_Visita'] = Saida_Banco['Modelo_Media_Visita'].astype(int)
    Saida_Banco['vlr_item_alert_level'] = Saida_Banco['vlr_item_alert_level'].astype(int)
    Saida_Banco['pico_30_dias'] = Saida_Banco['pico_30_dias'].apply(np.ceil).astype(int)
    #Saida_Banco['DAT_PROCESSED'] = [datetime.now() for item in Saida_Banco.iloc[:,0]]
    agora = datetime.now()
    Saida_Banco['DAT_PROCESSED'] = agora

    print("nulos onde sku e location aparece:")
    problemas = historico_vendas[historico_vendas['ind_curva'].isna()]
    print(problemas[['id_sku', 'id_location', 'ind_curva']])



    print("--- tempo de execucao em segundos: %s ---" % (time.time() - start_time))
    return Saida_Bancon