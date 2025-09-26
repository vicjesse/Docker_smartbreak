import warnings
import pandas as pd
import numpy as np
from prophet import Prophet
#import datetime as dt
from datetime import datetime, date, timedelta
import redshift_connector
import pandas as pd
import logging
logging.getLogger("cmdstanpy").disabled = True #  turn 'cmdstanpy' logs off
import time
from joblib import Parallel, delayed
import contextlib
import joblib
from tqdm import tqdm
from scipy.signal import convolve
#from motor_nivel_par_queries import *
#from google.cloud import bigquery
#from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
#import os
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/support_files/projeto-smart-break.json"

Hoje=date.today()

Locais=(136110,136173)
# Locais=(136110,136173,136565,137066,137523,137524,136997,137051,136951,137529,137531,136970,136602,136516,137019,137008,137539,136384,137003,144780,137020,136621,137045,137549,136721,137095,136600,137067,141012,136731,136386,136528,142078,139167,139035,137062,136651,138744,140101,136876,136757,136391,137080,136723,136738,137361)


Periodo_Analise_Historico=90 

Ultimos_15_Dias=date.today() - timedelta(days=15)
Ultimos_30_Dias=date.today() - timedelta(days=30) #Aqui tenho que somar 30 dias a mais para fim do calculo de stockout
Ultimos_60_Dias=date.today() - timedelta(days=60)
Ultimos_90_Dias=str(Hoje - timedelta(Periodo_Analise_Historico))


Filtro_Recencia=str(date.today() - timedelta(30))

Query_Valor_Par='''select
                par.vlr_item_par_level as valor_par,
                par.id_good_item as id_sku,
                insta.id_location as id_location,par.dat_update as data
            from
                trusted_vmpay.tb_planograms as par
                inner join trusted_vmpay.tb_installations as insta
                on par.id_installation=insta.id_installation
            order by data
            '''



Query_Planograma='''WITH RankedRows AS (
                SELECT
                    par.vlr_capacity_item,
                    par.vlr_item_alert_level,
                    par.id_good_item AS id_sku,
                    par.dat_creation,
                    insta.id_location,
                    ROW_NUMBER() OVER (PARTITION BY id_good_item, insta.id_location ORDER BY par.dat_creation DESC) AS RowRank
                FROM
                    trusted_vmpay.tb_planograms AS par
                    INNER JOIN trusted_vmpay.tb_installations AS insta on par.id_installation=insta.id_installation -- Specify the join condition
                )
                SELECT
                vlr_capacity_item,
                vlr_item_alert_level,
                id_sku,
                -- dat_creation,
                id_location
                FROM
                RankedRows
                WHERE
                RowRank = 1;
                '''


Query_Visitas_Loja = "SELECT * FROM curated_ds.visitas where flg_restock = True and dat_visit>='"+Filtro_Recencia+"' order by dat_visit desc"


Query_Relacao_id_Produto= 'SELECT * FROM curated_ds.relacao_id_produto'
Query_Relacao_id_Loja= 'SELECT * FROM curated_ds.relacao_id_loja'

query_Reabastecimento="select id_location,dat_visit,id_good as id_sku,flg_restock from curated_ds.vw_tb_visits_restock where dat_visit >='"+str(Ultimos_15_Dias)+"'  order by dat_visit desc "


Query_Dados_Ruptura="""select sum(TIMESTAMP_DIFF(R.dat_ended,DATE_SUB(TIMESTAMP(T.dat_order), INTERVAL 15 DAY),HOUR )) as time_rupture,
                        sum(R.tempo_em_horas_de_ruptura) as time_rupture2,
                        R.dat_started,R.dat_ended,DATE_SUB(T.dat_order, INTERVAL 15 DAY) as dat_back,
                        T.dat_order,T.id_sku,R.id_location,max(T.Total_Sales) as total_sales,max(T.periodo_dia) as max
                        from curated_ds.vw_base_transacional_agg_sem_ruptura as T
                        inner join curated_ds.vw_rupturas_90_dias_refinada as R
                        on R.id_location=T.id_location 
                        and R.id_good = T.id_sku
                        where T.dat_order>= date_sub(current_date, INTERVAL 90 DAY) 
                        and DATE(R.dat_ended) <=T.dat_order 
                        and DATE(R.dat_ended) >=DATE_SUB(dat_order, INTERVAL 15 DAY)
                        group by 
                        T.dat_order,T.id_sku,R.id_location,R.dat_ended,R.dat_started

                        """


Query_StockOut="SELECT id_location, id_sku, dat_faturamento, qtd_stock_out_items, horas_rupturas_30_dias,round(vlr_numero_vendas_hora, 8)*24  as Media FROM CURATED_DS.TB_METRICAS_MODELO WHERE date(dat_faturamento) >='"+ str(Ultimos_90_Dias) +"' and id_location in " + str(Locais)

Query_View_Resultados='select * from curated_ds.vw_modelo_visualizacao limit 10'





def Moving_Window_Prophet_Scorer_Pair_Value(Model,df,min_training_size,days_to_predict):
    #Nova versão para modelar o comportamento de produção do valor par
    Step=1
    size=df.shape[0]-min_training_size
    mape=[]
    Final_Results=[]
    Regressores_Adicionais=list(Model.extra_regressors.keys())
    Cont=0
    for i in range(0,size,Step):
        Model=Prophet(changepoint_range=1.0)
        for k in Regressores_Adicionais:
            Model.add_regressor(k)
        Margem=(min_training_size+i)
        data=df.iloc[0:Margem]
        #print(data.dtypes)
        Model.fit(data)
        output=df.iloc[Margem:(Margem +days_to_predict - Cont%days_to_predict  ),:] #Assim vamos fazer o forecast de 7 dias, depois 6 depois 5,....

        #print(output.columns)
        Prediction=round(Model.predict(output)['yhat'],0)
        Contained=np.logical_and(output['y'].values>=round(Model.predict(output)['yhat_lower'],0),output['y'].values<=round(Model.predict(output)['yhat_upper'],0) )


        result=pd.DataFrame({'data_previsao':output['ds'].values,'previsao_diaria':Prediction.values,'valor_real':output['y'].values, 'mape':(np.abs(output['y'].values-Prediction.values)/output['y'].values),'Confidence_Interval_True':Contained,'Days_Forecast':(days_to_predict - Cont%days_to_predict) })
        Final_Results.append(result)
        Cont=Cont+1
        #mape.append( np.abs(output['y'].values-Prediction.values)/output['y'].values   )
        #print(i)
    return pd.concat(Final_Results)#mape


def Moving_Window_Prophet_Scorer_Pair_Value_Alternative(Model,df,min_training_size,days_to_predict):
    #Nova versão para modelar o comportamento de produção do valor par
    Step=days_to_predict
    import numpy as np
    size=df.shape[0]-min_training_size
    mape=[]
    Final_Results=[]
    Regressores_Adicionais=list(Model.extra_regressors.keys())
    #Cont=0
    for i in range(0,size,Step):
        Model=Prophet(changepoint_range=1.0)
        for k in Regressores_Adicionais:
            Model.add_regressor(k)
        Margem=(min_training_size+i)
        data=df.iloc[0:Margem]
        #print(data.dtypes)
        Model.fit(data)
        output=df.iloc[Margem:(Margem +days_to_predict ),:]


        Prediction=round(Model.predict(output)[['yhat','yhat_lower','yhat_upper']],0)
        #Contained=np.logical_and(output['y'].values>=round(Model.predict(output)['yhat_lower'],0),output['y'].values<=round(Model.predict(output)['yhat_upper'],0) )

        result=pd.DataFrame({'previsao_diaria':Prediction['yhat'].values,'valor_real':output['y'].values,'Limite_Inferior':Prediction['yhat_lower'].values,'Limite_Superior':Prediction['yhat_upper'].values,'Contagem':1})
        result=result.cumsum()
        result['data_previsao']=output['ds'].values
        result['Confidence_Interval_True']=np.logical_and(result['valor_real'].values>=result['Limite_Inferior'],result['valor_real'].values<=result['Limite_Superior'] )
        result['mae']=(np.abs(result['valor_real'].values-result['previsao_diaria'].values))
        result['mape']=result['mae']/result['valor_real']
        #result=pd.DataFrame({'data_previsao':output['ds'].values,'previsao_diaria':Prediction.values,'valor_real':output['y'].values, 'mape':(np.abs(output['y'].values-Prediction.values)/output['y'].values),'Confidence_Interval_True':Contained,'Days_Forecast':1})
        Final_Results.append(result)
        #Cont=Cont+1
        #mape.append( np.abs(output['y'].values-Prediction.values)/output['y'].values   )
        #print(i)
    return pd.concat(Final_Results)#mape


def Moving_Window_Prophet_Scorer(Model,df,min_training_size,days_to_predict,Step):

    size=df.shape[0]-min_training_size
    mape=[]
    Final_Results=[]
    Regressores_Adicionais=list(Model.extra_regressors.keys())
    for i in range(0,size,Step):
        Model=Prophet(changepoint_range=1.0)
    for k in Regressores_Adicionais:
        Model.add_regressor(k)
    Margem=(min_training_size+i)
    data=df.iloc[0:Margem]
    #print(data.dtypes)
    Model.fit(data)
    output=df.iloc[[Margem]]
    #print(output.columns)
    Prediction=round(Model.predict(output)['yhat'],0)
    Contained=np.logical_and(output['y'].values>=round(Model.predict(output)['yhat_lower'],0),output['y'].values<=round(Model.predict(output)['yhat_upper'],0) )


    result=pd.DataFrame({'data_previsao':output['ds'].values,'previsao_diaria':Prediction.values,'valor_real':output['y'].values, 'mape':(np.abs(output['y'].values-Prediction.values)/output['y'].values),'Confidence_Interval_True':Contained })
    Final_Results.append(result)
    #mape.append( np.abs(output['y'].values-Prediction.values)/output['y'].values   )
    #print(i)
    return pd.concat(Final_Results)#mape



def ajustar_modelo(df):
    # Ajuste do modelo
    Modelo = Prophet(changepoint_range=1.0)
    Modelo.fit(df)

    # Previsão para o dia atual
    X = pd.DataFrame({'ds': [date.today()]}, index=[0])
    previsao_dia_atual = round(Modelo.predict(X)[['yhat_lower', 'yhat', 'yhat_upper']], 0)
    previsao_dia_atual = (previsao_dia_atual > 0) * 1 * previsao_dia_atual
    return previsao_dia_atual


def ajustar_modelo_valor_par(df,days_to_predict):
    # Ajuste do modelo
    Modelo = Prophet(changepoint_range=1.0)
    Modelo.add_country_holidays(country_name='BR')
    Modelo.add_regressor('time_rupture')

    Modelo.fit(df)

    # Previsão para o dia atual
    Days=[date.today() + timedelta(days=x) for x in range(days_to_predict)]
    X = pd.DataFrame({'ds':Days })
    #previsao_dia_atual = round(Modelo.predict(X)[['yhat_lower', 'yhat', 'yhat_upper']], 0)
    #previsao_dia_atual = (previsao_dia_atual > 0) * 1 * previsao_dia_atual
    #previsao_dia_atual['ds']=X['ds'].values
    previsao_dia_atual=Modelo.predict(df)
    return previsao_dia_atual

def Gerar_Previsao_valor_par(df,days_to_predict):
    # Ajuste do modelo
    Modelo = Prophet(changepoint_range=1.0)
    Modelo.add_country_holidays(country_name='BR')
    Modelo.add_regressor('time_rupture')

    Modelo.fit(df)

    # Previsão para o dia atual
    Days=[date.today() + timedelta(days=x) for x in range(days_to_predict)]
    X = pd.DataFrame({'ds':Days })
    df['ds']=pd.to_datetime(df['ds'])

    Dias_Passados=X['ds'].values-df.tail(1)['ds'].values[0]
    Dias_Passados=[i.days*24 for i in Dias_Passados]
    X['time_rupture']=df.tail(1)['time_rupture'].values[0] - Dias_Passados
    X['time_rupture']=X['time_rupture'] * (1*(X['time_rupture']>0))
    #previsao_dia_atual = round(Modelo.predict(X)[['yhat_lower', 'yhat', 'yhat_upper']], 0)
    #previsao_dia_atual = (previsao_dia_atual > 0) * 1 * previsao_dia_atual
    #previsao_dia_atual['ds']=X['ds'].values
    previsao_dia_atual=Modelo.predict(X)
    return previsao_dia_atual

def ajustar_modelo_valor_par_med_mov(df,days_to_predict):
    # Ajuste do modelo
    Modelo = Prophet(changepoint_range=1.0)
    Modelo.add_country_holidays(country_name='BR')
    Modelo.fit(df)
    days_to_predict=int(days_to_predict)


    # Previsão para o dia atual
    Days=[date.today() + timedelta(days=x) for x in range(7)]
    X = pd.DataFrame({'ds':Days })
    previsao_dia_atual = round(Modelo.predict(X)[['yhat_lower', 'yhat', 'yhat_upper']], 0)
    previsao_dia_atual = (previsao_dia_atual > 0) * 1 * previsao_dia_atual
    previsao_dia_atual=previsao_dia_atual.rolling(window=days_to_predict,min_periods=days_to_predict).mean().dropna()
    #previsao_dia_atual['ds']=X['ds'].values
    previsao_dia_atual=previsao_dia_atual.mean().round(0)
    previsao_dia_atual['Dia']=days_to_predict
    return previsao_dia_atual#previsao_dia_atual.mean().round(0)



def ajuste_previsao(df):

    resultados_previsao = []

    # Processar cada combinação de id_location(loja)/id_sku
    for (id_location, id_sku), df_group in df.groupby(['id_location', 'id_sku']):
        # Verificar se o grupo tem mais de 3 observações
        if df_group.shape[0] > 2:
            # Ajustar o modelo e calcular a previsão
            previsao = ajustar_modelo(df_group)

            previsao['id_location'] = id_location
            previsao['id_sku'] = id_sku

            # Adicionar a data da previsão como uma nova coluna
            previsao['data_previsao'] = date.today()

            # Adicionar os resultados a lista
            resultados_previsao.append(previsao)

    df_resultados_previsao = pd.concat(resultados_previsao)

    return df_resultados_previsao



def Estimar_Data_De_Forecast(visitas,visitas_agendadas):
    visitas['dat_visit']=visitas['dat_visit'].date

    visitas=visitas.drop_duplicates()

    tempo_medio_entre_visitas=visitas[['id_location','dat_visit']].set_index('id_location').groupby('id_location').diff().abs().dropna().groupby('id_location').median()

    tempo_medio_entre_visitas['dat_visit']=tempo_medio_entre_visitas['dat_visit'].astype('str').str.split(' ',expand=True)[0].astype(int)

    tempo_medio_entre_visitas=tempo_medio_entre_visitas.reset_index()

    Ultimas_Visitas_Realizadas=visitas[['id_location','dat_visit']].groupby('id_location').max().reset_index()

    Ultimas_Visitas_Agendadas=visitas_agendadas[['id_location','dat_visit']].groupby('id_location').max().reset_index()

    Ultimas_Visitas_Realizadas=tempo_medio_entre_visitas.rename(columns={'dat_visit':'dias_entre_visitas'}).merge(Ultimas_Visitas_Realizadas)

    Ultimas_Visitas_Realizadas['Esperado']=Ultimas_Visitas_Realizadas['dat_visit'] + pd.DataFrame({'Soma':[timedelta(i) for i in Ultimas_Visitas_Realizadas['dias_entre_visitas'] ]})['Soma']

    Visitas_Quase_La=Ultimas_Visitas_Realizadas.merge(Ultimas_Visitas_Agendadas.rename(columns={'dat_visit':'visita_planejada'}))

    Vetor_Logico=Visitas_Quase_La['visita_planejada']>=pd.to_datetime(date.today())
    Visitas_Quase_La['Logico']=Vetor_Logico

    Visitas_Quase_La['Final']=[ Visitas_Quase_La.loc[i,'visita_planejada'] if Visitas_Quase_La.loc[i,'Logico']  else Visitas_Quase_La.loc[i,'Esperado'] for i in Visitas_Quase_La.index   ]

    Visitas_Quase_La['Dias_Para_Abastecer']=(Visitas_Quase_La['Final']- pd.to_datetime(datetime.today() ) ).dt.days

    Visitas_Quase_La['Dias_Para_Abastecer']=1*((Visitas_Quase_La['Dias_Para_Abastecer']>0)*Visitas_Quase_La['Dias_Para_Abastecer'] )

    Visitas_A_Realizar=Visitas_Quase_La[['id_location','Dias_Para_Abastecer']]

    return Visitas_A_Realizar


def calcular_previsao(visitas, visitas_agendadas):
    hoje = pd.Timestamp.now().normalize()
    previsoes = []

    for id_location in visitas['id_location'].unique():
        df_visita_loja = visitas[(visitas['id_location'] == id_location) & (visitas['flg_restock'] == True)]
        df_visita_agendada_loja = visitas_agendadas[(visitas_agendadas['id_location'] == id_location) & (visitas_agendadas['flg_restock'] == True)]

        if not df_visita_loja.empty and not df_visita_agendada_loja.empty:
            # Calcular a media de tempo entre visitas da loja
            if len(df_visita_loja) > 1:
                diferenca_visitas = df_visita_loja['dat_visit'].diff().abs().mean()
                tempo_medio_visitas = timedelta(days=diferenca_visitas.days)
                #tempo_medio_visitas = timedelta(days=diferenca_visitas.days)

            # Aqui vou encontrar as visitas agendadas futuras
            visitas_futuras = df_visita_agendada_loja[df_visita_agendada_loja['dat_visit'] > hoje]

            if not visitas_futuras.empty:
                # Se eu tiver uma visita agendada futura, calcular a diferença de dias ate a visita
                proxima_visita = visitas_futuras['dat_visit'].min()
                diferenca_dias = (proxima_visita - hoje).days
                previsao = hoje + timedelta(days=diferenca_dias)
            elif tempo_medio_visitas:
                # Calcular a previsão
                previsao = hoje + tempo_medio_visitas

            # Para eu obter a data da ultima visita de df_visita_loja
            ultima_visita = df_visita_loja['dat_visit'].max()
            previsoes.append((id_location, previsao, ultima_visita, tempo_medio_visitas))

    # Crio um DataFrame a partir das previsões
    df_previsoes = pd.DataFrame(previsoes, columns=['id_location', 'previsao', 'ultima_visita', 'tempo_medio_entre_visitas'])

    # Insero a coluna 'visita_agendada' com as datas agendadas
    df_previsoes['visita_agendada'] = visitas_agendadas['dat_visit']

    # Remover "days" da coluna tempo_medio_entre_visitas
    df_previsoes['tempo_medio_entre_visitas'] = df_previsoes['tempo_medio_entre_visitas'].astype(str).str.replace(' days', '')


    return df_previsoes



#FUnções de processamento paralelo
def parallel_groupby(df, groupby_columns, func, *args, **kwargs):
    """
    Perform a groupby operation on a DataFrame in parallel.

    Parameters:
    df (DataFrame): The DataFrame to group.
    groupby_columns (str or list of str): The column(s) to group by.
    func (function): The function to apply to each group.
    *args: Additional positional arguments to pass to func.
    **kwargs: Additional keyword arguments to pass to func.

    Returns:
    DataFrame: A DataFrame containing the result of the groupby operation.
    """
    # Get unique groups
    groups = df.groupby(groupby_columns).groups

    # Define a function to apply to each group in parallel
    def apply_func(group):
        return func(df.loc[group], *args, **kwargs)
    with tqdm_joblib(tqdm(desc="My calculation", total=len(groups.values()))) as progress_bar:

        # Use joblib to parallelize the groupby operation
        results = Parallel(n_jobs=joblib.cpu_count())(delayed(apply_func)(group) for group in groups.values())

    # Concatenate the results
    result_df = pd.concat(results)

    return result_df


def apply_convolution(group):
    kernel = np.array([1, 1])
    # Get the size of the sliding window for this group
    window_size = group['Dias_Para_Abastecer'].iloc[0]
    group['Total']=group['total_sales']+group['qtd_stock_out_items']
    kernel=[1]*window_size
    
    # Apply convolution
    conv_total = convolve(group['Total'].values, kernel, mode='valid')
    conv_vendas = convolve(group['total_sales'].values, kernel, mode='valid')
    conv_stockout= convolve(group['qtd_stock_out_items'].values, kernel, mode='valid')
    Convolution=pd.DataFrame({'Total':conv_total,'Vendas':conv_vendas,'Stockout':conv_stockout})
    #df=pd.DataFrame({'Total':np.max(conv_total)},index=[0])
    df=pd.DataFrame({'Total':np.max(conv_vendas)},index=[0])
    return Convolution.merge(df).tail(1)
    #return Convolution





#def apply_convolution(group):
#    try:
#        window_size = int(group['Dias_Para_Abastecer'].iloc[0])
#
#        # Proteger contra grupos pequenos
#        if len(group) < window_size:
#            print(f"[SKIP] Grupo pequeno: {len(group)} linhas, janela = {window_size}, id_sku={group['id_sku'].iloc[0]}, id_location={group['id_location'].iloc[0]}")
#            return pd.DataFrame()
#
#        # Prepara os dados
#        group = group.copy()
#        group['Total'] = group['total_sales'] + group['qtd_stock_out_items']
#
#        kernel = [1] * window_size
#
#        # Aplica convolução
#        conv_total = convolve(group['Total'].values, kernel, mode='valid')
#        conv_vendas = convolve(group['total_sales'].values, kernel, mode='valid')
#        conv_stockout = convolve(group['qtd_stock_out_items'].values, kernel, mode='valid')
#
#        # Cria DataFrame com o pico
#        df = pd.DataFrame({
#            'id_sku': [group['id_sku'].iloc[0]],
#            'id_location': [group['id_location'].iloc[0]],
#            'pico_total': [conv_total.max()],
#            'pico_vendas': [conv_vendas.max()],
#            'pico_stockout': [conv_stockout.max()]
#        })
#        return df
#
#    except Exception as e:
#        print(f"[ERRO] id_sku={group['id_sku'].iloc[0]}, id_location={group['id_location'].iloc[0]}: {e}")
#        return pd.DataFrame()





def Gerar_Previsao_valor_par_suporte_paralelo(df):
    # Ajuste do modelo
    try:
        Modelo = Prophet(interval_width=0,uncertainty_samples=0) #Captura mudancas de tendencia ate o ultimo momento
        Modelo.add_country_holidays(country_name='BR') #Feriados Brasileiros
        Modelo.add_regressor('time_rupture') # Tempo em ruptura nos ultimos 15 dias
        days_to_predict=df['Dias_Para_Abastecer'].head(1).values[0] #Quantos dias para frente vamos projetar
        days_to_predict=int(days_to_predict)
        df=pd.DataFrame({'ds':[pd.to_datetime(Ultimos_90_Dias) + datetime.timedelta(i) for i in range(91)]}).merge(df,how='left') 
        df['y'].fillna(0,inplace=True) #Zero Padding
        #df['time_rupture']=df['time_rupture'].interpolate(method='nearest')
        df['time_rupture'] = (df['time_rupture'].astype("float").interpolate(method="nearest"))
        df['time_rupture']=df['time_rupture'].fillna(0)
        #print('foi')
        Modelo.fit(df)
        #print('foi')
                

        #print('foi')
        # Previsão para o dia atual
        Days=[date.today() + timedelta(days=x) for x in range(days_to_predict)]
        X = pd.DataFrame({'ds':pd.to_datetime(Days) })
        df['ds']=pd.to_datetime(df['ds'])
        #print('foi')
        Dias_Passados=X['ds']-df.tail(1)['ds'].values[0]
        Dias_Passados=[i.days*24 for i in Dias_Passados]
        X['time_rupture']=df.tail(1)['time_rupture'].values[0] - Dias_Passados #Para tratar a ruptura, assumimos que  não teremos novas rupturas, e assim reduzimos 24h por dia
        X['time_rupture']=X['time_rupture'] * (1*(X['time_rupture']>0))
        #print('foi')
        previsao_dia_atual=Modelo.predict(X)
        previsao_dia_atual['yhat_upper']=previsao_dia_atual['yhat']
        previsao_dia_atual['yhat_lower']=previsao_dia_atual['yhat']
        
        previsao_dia_atual['id_location']=df['id_location'].unique()[0]
        previsao_dia_atual['id_sku']=df['id_sku'].unique()[0]
        return previsao_dia_atual
    except:
        pass

#Aqui deixo como duas funções separadas devio ao 0 padding delas ser em periodos diferentes.
#Uma otimização seria fazer o zero padding  a extração dos dias antes de levar para a função principal.

def Gerar_Previsao_valor_par_suporte_paralelo_15Dias(df):
    # Ajuste do modelo
    try:
        Ultimos_15_Dias = Hoje - timedelta(15)
        df=df[df['ds'] >= pd.to_datetime(Ultimos_15_Dias) ]
        Modelo = Prophet(interval_width=0,uncertainty_samples=0) #Captura mudancas de tendencia ate o ultimo momento
        Modelo.add_country_holidays(country_name='BR') #Feriados Brasileiros
        Modelo.add_regressor('time_rupture') # Tempo em ruptura nos ultimos 15 dias
        days_to_predict=df['Dias_Para_Abastecer'].head(1).values[0] #Quantos dias para frente vamos projetar
        days_to_predict=int(days_to_predict)
        df=pd.DataFrame({'ds':[pd.to_datetime(Ultimos_15_Dias) + timedelta(i) for i in range(16)]}).merge(df,how='left') 
        df['y'].fillna(0,inplace=True) #Zero Padding
        #df['time_rupture']=df['time_rupture'].interpolate(method='nearest')
        df['time_rupture'] = (df['time_rupture'].astype("float").interpolate(method="nearest"))
        df['time_rupture']=df['time_rupture'].fillna(0)
        #print('foi')
        Modelo.fit(df)
        #print('foi')
                

        #print('foi')
        # Previsão para o dia atual
        Days=[date.today() + timedelta(days=x) for x in range(days_to_predict)]
        X = pd.DataFrame({'ds':pd.to_datetime(Days) })
        df['ds']=pd.to_datetime(df['ds'])
        #print('foi')
        Dias_Passados=X['ds']-df.tail(1)['ds'].values[0]
        Dias_Passados=[i.days*24 for i in Dias_Passados]
        X['time_rupture']=df.tail(1)['time_rupture'].values[0] - Dias_Passados #Para tratar a ruptura, assumimos que  não teremos novas rupturas, e assim reduzimos 24h por dia
        X['time_rupture']=X['time_rupture'] * (1*(X['time_rupture']>0))
        #print('foi')
        previsao_dia_atual=Modelo.predict(X)
        previsao_dia_atual['yhat_upper']=previsao_dia_atual['yhat']
        previsao_dia_atual['yhat_lower']=previsao_dia_atual['yhat']
        
        previsao_dia_atual['id_location']=df['id_location'].unique()[0]
        previsao_dia_atual['id_sku']=df['id_sku'].unique()[0]
        return previsao_dia_atual
    except Exception as e:
        print(f"Erro durante a execução: {e}")
    return None  # Para garantir que o erro é compreendido

def Gerar_Previsao_valor_par_suporte_paralelo(df):
    # Ajuste do modelo
        Modelo = Prophet(interval_width=0,uncertainty_samples=0) #Captura mudancas de tendencia ate o ultimo momento
        Modelo.add_country_holidays(country_name='BR') #Feriados Brasileiros
        Modelo.add_regressor('time_rupture') # Tempo em ruptura nos ultimos 15 dias
        days_to_predict=df['Dias_Para_Abastecer'].head(1).values[0] #Quantos dias para frente vamos projetar
        days_to_predict=int(days_to_predict)
        df=pd.DataFrame({'ds':[pd.to_datetime(Ultimos_90_Dias) + timedelta(i) for i in range(91)]}).merge(df,how='left') 
        df['y'].fillna(0,inplace=True) #Zero Padding
        #df['time_rupture']=df['time_rupture'].interpolate(method='nearest')
        df['time_rupture'] = (df['time_rupture'].astype("float").interpolate(method="nearest"))
        df['time_rupture']=df['time_rupture'].fillna(0)
        #print('foi')
        Modelo.fit(df)
        #print('foi')
                

        #print('foi')
        # Previsão para o dia atual
        Days=[date.today() + timedelta(days=x) for x in range(days_to_predict)]
        X = pd.DataFrame({'ds':pd.to_datetime(Days) })
        df['ds']=pd.to_datetime(df['ds'])
        #print('foi')
        Dias_Passados=X['ds']-df.tail(1)['ds'].values[0]
        Dias_Passados=[i.days*24 for i in Dias_Passados]
        X['time_rupture']=df.tail(1)['time_rupture'].values[0] - Dias_Passados #Para tratar a ruptura, assumimos que  não teremos novas rupturas, e assim reduzimos 24h por dia
        X['time_rupture']=X['time_rupture'] * (1*(X['time_rupture']>0))
        #print('foi')
        previsao_dia_atual=Modelo.predict(X)
        previsao_dia_atual['yhat_upper']=previsao_dia_atual['yhat']
        previsao_dia_atual['yhat_lower']=previsao_dia_atual['yhat']
        
        previsao_dia_atual['id_location']=df['id_location'].unique()[0]
        previsao_dia_atual['id_sku']=df['id_sku'].unique()[0]
        return previsao_dia_atual


@contextlib.contextmanager
def tqdm_joblib(tqdm_object):
    """Context manager to patch joblib to report into tqdm progress bar given as argument"""
    class TqdmBatchCompletionCallback(joblib.parallel.BatchCompletionCallBack):
        def __call__(self, *args, **kwargs):
            tqdm_object.update(n=self.batch_size)
            return super().__call__(*args, **kwargs)

    old_batch_callback = joblib.parallel.BatchCompletionCallBack
    joblib.parallel.BatchCompletionCallBack = TqdmBatchCompletionCallback
    try:
        yield tqdm_object
        
    finally:
        joblib.parallel.BatchCompletionCallBack = old_batch_callback
        tqdm_object.close()
