import requests
import pandas as pd
import json
import matplotlib
import datetime as dt
from datetime import date
import sqlalchemy as alch
from getpass import getpass
from functools import reduce
import src

#agregamos las solicitud de fechas para el analisis
def fecha_historico(x,y):
    list_day =[]
    for i in range(x,y,2): # de 2 en 2
        start_year = date(i,1,1) # fechas iniciales
        list_day.append(start_year) # agregamos a l
        end_year = date(i+1,12,31) # fechas finales
        list_day.append(end_year) # agregamos los finales
    return list_day 
def Consulta_API(date,x,y):
    abc = []
    for i in range(len(date)-1,0,-2): # [7, 5, 3, 1]
        url='https://apidatos.ree.es' #url de la API de REE
        # Parametros principales de la API (lang, category & widget)
        lang = 'es'
        category = x # balance - balance electrico  o demanda - evolucion
        widget = y
        # url a añadir para acceder a los datos segun la category y el widget
        url_ext = f'/{lang}/datos/{category}/{widget}?query'

        # Parametros para definir el rango de tiempo que se va a descargar + nacional o por region precisas
        #start_date = f'start_date=2021-03-12T00:00'
        start_date = f'start_date={date[i-1]}' # solos los start day

        #end_date = 'end_date=2021-03-15T00:00'
        end_date = f'end_date={date[i]}' #  solo los end day

        time_trunc = 'time_trunc=month' # por mes 
        #geo_limit = 'geo_limit='
        #geo_ids = 'geo_ids=8'
        # url que añade los ultimos parametros
        url_params = f'&{start_date}&{end_date}&{time_trunc}'
        #url_params = f'&{start_date}&{end_date}&{time_trunc}&{geo_ids}'
        response = requests.get(url+url_ext+url_params) # unimos la url con sus parametros
        abc.append(json.loads(response.content.decode(response.encoding)))
    return abc # creamos una lista con los datos establecidos

    # esto lo creamos porque para consultar en la API nos dan un limite de 24 meses
    #por lo que con esto lo consultamos las veces que nosotros deseemos



def dataframe_demanda(demanda):
    df0 = pd.DataFrame(demanda[0]['included'][0]['attributes']['values'])
    df1 = pd.DataFrame(demanda[1]['included'][0]['attributes']['values'])
    df2 = pd.DataFrame(demanda[2]['included'][0]['attributes']['values'])
    df3 = pd.DataFrame(demanda[3]['included'][0]['attributes']['values'])
    df_demanda = pd.concat([df0, df1, df2, df3])
    df_demanda['datetime'] = pd.to_datetime(df_demanda['datetime'], format='%Y-%m-%dT%H:%M:%S')
# Se crea una columna para la fecha y otra para el hora
    df_demanda['Date']= df_demanda['datetime'].apply(lambda x:x.date())
    df_demanda = df_demanda.drop(columns=['datetime', 'percentage'], axis=1)
    df_demanda = df_demanda.rename(columns={'value':'Demanda'})
    df_demanda['Demanda'] = df_demanda['Demanda'].astype(int) # es que me bota float y quiero int
    return df_demanda

#GENERACION GENERAL
def dataframe_evolucionrenovable(gen_ge):
    df0 = pd.DataFrame(gen_ge[0]['included'][0]['attributes']['values'])
    df1 = pd.DataFrame(gen_ge[1]['included'][0]['attributes']['values'])
    df2 = pd.DataFrame(gen_ge[2]['included'][0]['attributes']['values'])
    df3 = pd.DataFrame(gen_ge[3]['included'][0]['attributes']['values'])
    df_evolucionrenovable = pd.concat([df0, df1, df2, df3])
    df_evolucionrenovable['datetime'] = pd.to_datetime(df_evolucionrenovable['datetime'], format='%Y-%m-%dT%H:%M:%S')
# Se crea una columna para la fecha y otra para el hora
    df_evolucionrenovable['Date']= df_evolucionrenovable['datetime'].apply(lambda x:x.date())
    df_evolucionrenovable = df_evolucionrenovable.drop(columns=['datetime', 'percentage'], axis=1)
    df_evolucionrenovable = df_evolucionrenovable.rename(columns={'value':'Demanda'})
    df_evolucionrenovable['Demanda'] = df_evolucionrenovable['Demanda'].astype(int)
    return df_evolucionrenovable

def dataframe_evolucionnorenovable(gen_ge):
    df0 = pd.DataFrame(gen_ge[0]['included'][1]['attributes']['values'])
    df1 = pd.DataFrame(gen_ge[1]['included'][1]['attributes']['values'])
    df2 = pd.DataFrame(gen_ge[2]['included'][1]['attributes']['values'])
    df3 = pd.DataFrame(gen_ge[3]['included'][1]['attributes']['values'])
    df_evolucionnorenovable = pd.concat([df0, df1, df2, df3])
    df_evolucionnorenovable['datetime'] = pd.to_datetime(df_evolucionnorenovable['datetime'], format='%Y-%m-%dT%H:%M:%S')
# Se crea una columna para la fecha y otra para el hora
    df_evolucionnorenovable['Date']= df_evolucionnorenovable['datetime'].apply(lambda x:x.date())
    df_evolucionnorenovable = df_evolucionnorenovable.drop(columns=['datetime', 'percentage'], axis=1)
    df_evolucionnorenovable = df_evolucionnorenovable.rename(columns={'value':'Demanda'})
    df_evolucionnorenovable['Demanda'] = df_evolucionnorenovable['Demanda'].astype(int)
    
    return df_evolucionnorenovable
    
    

# GENERACION ESPECIFICA
def dataframe_balance_hidraulica(gen_es):
    df0 = pd.DataFrame(gen_es[0]['included'][0]['attributes']['content'][0]['attributes']['values'])
    df1 = pd.DataFrame(gen_es[1]['included'][0]['attributes']['content'][0]['attributes']['values'])
    df2 = pd.DataFrame(gen_es[2]['included'][0]['attributes']['content'][0]['attributes']['values'])
    df3 = pd.DataFrame(gen_es[3]['included'][0]['attributes']['content'][0]['attributes']['values'])
    df_balance_valor0 = pd.concat([df0, df1, df2, df3])
    df_balance_valor0 = df_balance_valor0.drop(columns=['percentage'], axis=1)
    df_balance_valor0 = df_balance_valor0.rename(columns={'value':'Hidráulica'})
    df_balance_valor0['Hidráulica'] = df_balance_valor0['Hidráulica'].astype(int)
    return df_balance_valor0

def dataframe_balance_eolica(gen_es):
    df0 = pd.DataFrame(gen_es[0]['included'][0]['attributes']['content'][1]['attributes']['values'])
    df1 = pd.DataFrame(gen_es[1]['included'][0]['attributes']['content'][1]['attributes']['values'])
    df2 = pd.DataFrame(gen_es[2]['included'][0]['attributes']['content'][1]['attributes']['values'])
    df3 = pd.DataFrame(gen_es[3]['included'][0]['attributes']['content'][1]['attributes']['values'])
    df_balance_valor0 = pd.concat([df0, df1, df2, df3])
    df_balance_valor0 = df_balance_valor0.drop(columns=['percentage'], axis=1)
    df_balance_valor0 = df_balance_valor0.rename(columns={'value':'Eólica'})
    df_balance_valor0['Eólica'] = df_balance_valor0['Eólica'].astype(int)
    
    return df_balance_valor0

def dataframe_balance_solar(gen_es):
    df0 = pd.DataFrame(gen_es[0]['included'][0]['attributes']['content'][2]['attributes']['values'])
    df1 = pd.DataFrame(gen_es[1]['included'][0]['attributes']['content'][2]['attributes']['values'])
    df2 = pd.DataFrame(gen_es[2]['included'][0]['attributes']['content'][2]['attributes']['values'])
    df3 = pd.DataFrame(gen_es[3]['included'][0]['attributes']['content'][2]['attributes']['values'])
    df_balance_valor0 = pd.concat([df0, df1, df2, df3])
    df_balance_valor0 = df_balance_valor0.drop(columns=['percentage'], axis=1)
    df_balance_valor0 = df_balance_valor0.rename(columns={'value':'Fotovoltaica'})
    df_balance_valor0['Fotovoltaica'] = df_balance_valor0['Fotovoltaica'].astype(int)
    return df_balance_valor0

def dataframe_balance_termica(gen_es):
    df0 = pd.DataFrame(gen_es[0]['included'][0]['attributes']['content'][3]['attributes']['values'])
    df1 = pd.DataFrame(gen_es[1]['included'][0]['attributes']['content'][3]['attributes']['values'])
    df2 = pd.DataFrame(gen_es[2]['included'][0]['attributes']['content'][3]['attributes']['values'])
    df3 = pd.DataFrame(gen_es[3]['included'][0]['attributes']['content'][3]['attributes']['values'])
    df_balance_valor0 = pd.concat([df0, df1, df2, df3])
    df_balance_valor0 = df_balance_valor0.drop(columns=['percentage'], axis=1)
    df_balance_valor0 = df_balance_valor0.rename(columns={'value':'Termica'})
    df_balance_valor0['Termica'] = df_balance_valor0['Termica'].astype(int)
    return df_balance_valor0

def dataframe_balance_hidroeolica(gen_es):
    df0 = pd.DataFrame(gen_es[0]['included'][0]['attributes']['content'][4]['attributes']['values'])
    df1 = pd.DataFrame(gen_es[1]['included'][0]['attributes']['content'][4]['attributes']['values'])
    df2 = pd.DataFrame(gen_es[2]['included'][0]['attributes']['content'][4]['attributes']['values'])
    df3 = pd.DataFrame(gen_es[3]['included'][0]['attributes']['content'][4]['attributes']['values'])
    df_balance_valor0 = pd.concat([df0, df1, df2, df3])
    df_balance_valor0 = df_balance_valor0.drop(columns=['percentage'], axis=1)
    df_balance_valor0 = df_balance_valor0.rename(columns={'value':'Hidroeólica'})
    df_balance_valor0['Hidroeólica'] = df_balance_valor0['Hidroeólica'].astype(int)
    return df_balance_valor0

def dataframe_balance_otras(gen_es): #Otras renovables: incluyen biogás, biomasa, hidráulica marina y geotérmica.
    df0 = pd.DataFrame(gen_es[0]['included'][0]['attributes']['content'][5]['attributes']['values'])
    df1 = pd.DataFrame(gen_es[1]['included'][0]['attributes']['content'][5]['attributes']['values'])
    df2 = pd.DataFrame(gen_es[2]['included'][0]['attributes']['content'][5]['attributes']['values'])
    df3 = pd.DataFrame(gen_es[3]['included'][0]['attributes']['content'][5]['attributes']['values'])
    df_balance_valor0 = pd.concat([df0, df1, df2, df3])
    df_balance_valor0 = df_balance_valor0.drop(columns=['percentage'], axis=1)
    df_balance_valor0 = df_balance_valor0.rename(columns={'value':'Otras_Renovables'})
    df_balance_valor0['Otras_Renovables'] = df_balance_valor0['Otras_Renovables'].astype(int)
    return df_balance_valor0

def dataframe_balance_residuos(gen_es): # Residuos renovables: el 50% de la generación procedente de residuos sólidos urbanos se considera renovable.
    df0 = pd.DataFrame(gen_es[0]['included'][0]['attributes']['content'][6]['attributes']['values'])
    df1 = pd.DataFrame(gen_es[1]['included'][0]['attributes']['content'][6]['attributes']['values'])
    df2 = pd.DataFrame(gen_es[2]['included'][0]['attributes']['content'][6]['attributes']['values'])
    df3 = pd.DataFrame(gen_es[3]['included'][0]['attributes']['content'][6]['attributes']['values'])
    df_balance_valor0 = pd.concat([df0, df1, df2, df3])
    df_balance_valor0 = df_balance_valor0.drop(columns=['percentage'], axis=1)
    df_balance_valor0 = df_balance_valor0.rename(columns={'value':'Residuos_Renovables'})
    df_balance_valor0['Residuos_Renovables'] = df_balance_valor0['Residuos_Renovables'].astype(int)
    return df_balance_valor0


def unirrenovables(gen_es):
    dfs = [dataframe_balance_hidraulica(gen_es), dataframe_balance_eolica(gen_es), dataframe_balance_solar(gen_es), 
           dataframe_balance_termica(gen_es),dataframe_balance_hidroeolica(gen_es),dataframe_balance_otras(gen_es),
           dataframe_balance_residuos(gen_es)]
    df_balance = reduce(lambda  left,right: pd.merge(left,right,on=['datetime'],
                                                how='outer'), dfs)

    df_balance['datetime'] = pd.to_datetime(df_balance['datetime'], format='%Y-%m-%dT%H:%M:%S')
    # Se crea una columna para la fecha y otra para el hora
    df_balance['Date'] = df_balance['datetime'].apply(lambda x:x.date()) 
    df_balance = df_balance.drop(columns=['datetime'], axis=1)

    df_balance_renovable = df_balance 
    return df_balance_renovable  

def dataframe_balance_turbi(gen_es): # Turbinación bombeo 
    df0 = pd.DataFrame(gen_es[0]['included'][1]['attributes']['content'][0]['attributes']['values'])
    df1 = pd.DataFrame(gen_es[1]['included'][1]['attributes']['content'][0]['attributes']['values'])
    df2 = pd.DataFrame(gen_es[2]['included'][1]['attributes']['content'][0]['attributes']['values'])
    df3 = pd.DataFrame(gen_es[3]['included'][1]['attributes']['content'][0]['attributes']['values'])
    df_balance_valor0 = pd.concat([df0, df1, df2, df3])
    df_balance_valor0 = df_balance_valor0.drop(columns=['percentage'], axis=1)
    df_balance_valor0 = df_balance_valor0.rename(columns={'value':'Turbinación_bombeo'})
    df_balance_valor0['Turbinación_bombeo'] = df_balance_valor0['Turbinación_bombeo'].astype(int)
    return df_balance_valor0

def dataframe_balance_nuclear(gen_es): # Nuclear
    df0 = pd.DataFrame(gen_es[0]['included'][1]['attributes']['content'][1]['attributes']['values'])
    df1 = pd.DataFrame(gen_es[1]['included'][1]['attributes']['content'][1]['attributes']['values'])
    df2 = pd.DataFrame(gen_es[2]['included'][1]['attributes']['content'][1]['attributes']['values'])
    df3 = pd.DataFrame(gen_es[3]['included'][1]['attributes']['content'][1]['attributes']['values'])
    df_balance_valor0 = pd.concat([df0, df1, df2, df3])
    df_balance_valor0 = df_balance_valor0.drop(columns=['percentage'], axis=1)
    df_balance_valor0 = df_balance_valor0.rename(columns={'value':'Nuclear'})
    df_balance_valor0['Nuclear'] = df_balance_valor0['Nuclear'].astype(int)
    return df_balance_valor0

def dataframe_balance_ciclo(gen_es): # Ciclo combinado: coexisten dos ciclos termodinámicos en un sistema: uno, cuyo fluido de trabajo es el vapor de agua, y otro, cuyo fluido de trabajo es un gas. En una central eléctrica el ciclo de gas genera energía eléctrica mediante una turbina de gas y el ciclo de vapor de agua lo hace mediante una o varias turbinas de vapor.
    df0 = pd.DataFrame(gen_es[0]['included'][1]['attributes']['content'][2]['attributes']['values'])
    df1 = pd.DataFrame(gen_es[1]['included'][1]['attributes']['content'][2]['attributes']['values'])
    df2 = pd.DataFrame(gen_es[2]['included'][1]['attributes']['content'][2]['attributes']['values'])
    df3 = pd.DataFrame(gen_es[3]['included'][1]['attributes']['content'][2]['attributes']['values'])
    df_balance_valor0 = pd.concat([df0, df1, df2, df3])
    df_balance_valor0 = df_balance_valor0.drop(columns=['percentage'], axis=1)
    df_balance_valor0 = df_balance_valor0.rename(columns={'value':'Ciclo_combinado'})
    df_balance_valor0['Ciclo_combinado'] = df_balance_valor0['Ciclo_combinado'].astype(int)
    return df_balance_valor0

def dataframe_balance_carton(gen_es): 
    df0 = pd.DataFrame(gen_es[0]['included'][1]['attributes']['content'][3]['attributes']['values'])
    df1 = pd.DataFrame(gen_es[1]['included'][1]['attributes']['content'][3]['attributes']['values'])
    df2 = pd.DataFrame(gen_es[2]['included'][1]['attributes']['content'][3]['attributes']['values'])
    df3 = pd.DataFrame(gen_es[3]['included'][1]['attributes']['content'][3]['attributes']['values'])
    df_balance_valor0 = pd.concat([df0, df1, df2, df3])
    df_balance_valor0 = df_balance_valor0.drop(columns=['percentage'], axis=1)
    df_balance_valor0 = df_balance_valor0.rename(columns={'value':'Carton'})
    df_balance_valor0['Carton'] = df_balance_valor0['Carton'].astype(int)
    return df_balance_valor0

def dataframe_balance_diesel(gen_es): # motores de diesel
    df0 = pd.DataFrame(gen_es[0]['included'][1]['attributes']['content'][4]['attributes']['values'])
    df1 = pd.DataFrame(gen_es[1]['included'][1]['attributes']['content'][4]['attributes']['values'])
    df2 = pd.DataFrame(gen_es[2]['included'][1]['attributes']['content'][4]['attributes']['values'])
    df3 = pd.DataFrame(gen_es[3]['included'][1]['attributes']['content'][4]['attributes']['values'])
    df_balance_valor0 = pd.concat([df0, df1, df2, df3])
    df_balance_valor0 = df_balance_valor0.drop(columns=['percentage'], axis=1)
    df_balance_valor0 = df_balance_valor0.rename(columns={'value':'Diesel'})
    df_balance_valor0['Diesel'] = df_balance_valor0['Diesel'].astype(int)
    return df_balance_valor0

def dataframe_balance_gas(gen_es): # Turbina de gas
    df0 = pd.DataFrame(gen_es[0]['included'][1]['attributes']['content'][5]['attributes']['values'])
    df1 = pd.DataFrame(gen_es[1]['included'][1]['attributes']['content'][5]['attributes']['values'])
    df2 = pd.DataFrame(gen_es[2]['included'][1]['attributes']['content'][5]['attributes']['values'])
    df3 = pd.DataFrame(gen_es[3]['included'][1]['attributes']['content'][5]['attributes']['values'])
    df_balance_valor0 = pd.concat([df0, df1, df2, df3])
    df_balance_valor0 = df_balance_valor0.drop(columns=['percentage'], axis=1)
    df_balance_valor0 = df_balance_valor0.rename(columns={'value':'Gas'})
    df_balance_valor0['Gas'] = df_balance_valor0['Gas'].astype(int)
    return df_balance_valor0

def dataframe_balance_vapor(gen_es): # Turbina de vapor
    df0 = pd.DataFrame(gen_es[0]['included'][1]['attributes']['content'][6]['attributes']['values'])
    df1 = pd.DataFrame(gen_es[1]['included'][1]['attributes']['content'][6]['attributes']['values'])
    df2 = pd.DataFrame(gen_es[2]['included'][1]['attributes']['content'][6]['attributes']['values'])
    df3 = pd.DataFrame(gen_es[3]['included'][1]['attributes']['content'][6]['attributes']['values'])
    df_balance_valor0 = pd.concat([df0, df1, df2, df3])
    df_balance_valor0 = df_balance_valor0.drop(columns=['percentage'], axis=1)
    df_balance_valor0 = df_balance_valor0.rename(columns={'value':'Vapor'})
    df_balance_valor0['Vapor'] = df_balance_valor0['Vapor'].astype(int)
    return df_balance_valor0

def dataframe_balance_cogeneracion(gen_es): # Proceso mediante el cual se obtiene simultáneamente energía eléctrica y energía térmica y/o mecánica útil.
    df0 = pd.DataFrame(gen_es[0]['included'][1]['attributes']['content'][8]['attributes']['values'])
    df1 = pd.DataFrame(gen_es[1]['included'][1]['attributes']['content'][8]['attributes']['values'])
    df2 = pd.DataFrame(gen_es[2]['included'][1]['attributes']['content'][8]['attributes']['values'])
    df3 = pd.DataFrame(gen_es[3]['included'][1]['attributes']['content'][8]['attributes']['values'])
    df_balance_valor0 = pd.concat([df0, df1, df2, df3])
    df_balance_valor0 = df_balance_valor0.drop(columns=['percentage'], axis=1)
    df_balance_valor0 = df_balance_valor0.rename(columns={'value':'Cogeneracion'})
    df_balance_valor0['Cogeneracion'] = df_balance_valor0['Cogeneracion'].astype(int)
    return df_balance_valor0

def dataframe_balance_residuosno(gen_es): # Residuos no renovables
    df0 = pd.DataFrame(gen_es[0]['included'][1]['attributes']['content'][9]['attributes']['values'])
    df1 = pd.DataFrame(gen_es[1]['included'][1]['attributes']['content'][9]['attributes']['values'])
    df2 = pd.DataFrame(gen_es[2]['included'][1]['attributes']['content'][9]['attributes']['values'])
    df3 = pd.DataFrame(gen_es[3]['included'][1]['attributes']['content'][9]['attributes']['values'])
    df_balance_valor0 = pd.concat([df0, df1, df2, df3])
    df_balance_valor0 = df_balance_valor0.drop(columns=['percentage'], axis=1)
    df_balance_valor0 = df_balance_valor0.rename(columns={'value':'Residuos_No_Renovables'})
    df_balance_valor0['Residuos_No_Renovables'] = df_balance_valor0['Residuos_No_Renovables'].astype(int)
    return df_balance_valor0

def unirnorenovables(gen_es):
    dfs = [dataframe_balance_turbi(gen_es), dataframe_balance_nuclear(gen_es),dataframe_balance_ciclo(gen_es),
           dataframe_balance_carton(gen_es),dataframe_balance_diesel(gen_es),dataframe_balance_gas(gen_es),
           dataframe_balance_vapor(gen_es),dataframe_balance_cogeneracion(gen_es),dataframe_balance_residuosno(gen_es)]
    df_balance = reduce(lambda  left,right: pd.merge(left,right,on=['datetime'],
                                                how='outer'), dfs)

    df_balance['datetime'] = pd.to_datetime(df_balance['datetime'], format='%Y-%m-%dT%H:%M:%S')
    # Se crea una columna para la fecha y otra para el hora
    df_balance['Date'] = df_balance['datetime'].apply(lambda x:x.date()) 
    df_balance = df_balance.drop(columns=['datetime'], axis=1)

    df_balance_norenovable = df_balance 
    return df_balance_norenovable 


def dataframe_precio(pre):
    df0 = pd.DataFrame(pre[0]['included'][3]['attributes']['content'][0]['attributes']['values'])
    df1 = pd.DataFrame(pre[1]['included'][3]['attributes']['content'][0]['attributes']['values'])
    df2 = pd.DataFrame(pre[2]['included'][3]['attributes']['content'][0]['attributes']['values'])
    df3 = pd.DataFrame(pre[3]['included'][3]['attributes']['content'][0]['attributes']['values'])
    df_precio = pd.concat([df0, df1, df2, df3])
    df_precio = df_precio.drop(columns=['percentage'], axis=1)
    df_precio = df_precio.rename(columns={'value':'Precio'})
    df_precio['Precio'] = df_precio['Precio'].astype(int)
    return df_precio
def dataframe_energy(pre):
    df0 = pd.DataFrame(pre[0]['included'][4]['attributes']['content'][0]['attributes']['values'])
    df1 = pd.DataFrame(pre[1]['included'][4]['attributes']['content'][0]['attributes']['values'])
    df2 = pd.DataFrame(pre[2]['included'][4]['attributes']['content'][0]['attributes']['values'])
    df3 = pd.DataFrame(pre[3]['included'][4]['attributes']['content'][0]['attributes']['values'])
    df_ener = pd.concat([df0, df1, df2, df3])
    df_ener = df_ener.drop(columns=['percentage'], axis=1)
    df_ener = df_ener.rename(columns={'value':'Energia_Vendida'})
    df_ener['Energia_Vendida'] = df_ener['Energia_Vendida'].astype(int)
    return df_ener

def precio(pre):
    df_precioenergy = pd.merge(dataframe_precio(pre),dataframe_energy(pre))
    df_precioenergy['datetime'] = pd.to_datetime(df_precioenergy['datetime'], format='%Y-%m-%dT%H:%M:%S')
    df_precioenergy['Date'], df_precioenergy['Hour']= df_precioenergy['datetime'].apply(lambda x:x.date()), df_precioenergy['datetime'].apply(lambda x:x.time())
    df_precioenergy = df_precioenergy.drop(columns=['datetime', 'Hour'], axis=1)
    return df_precioenergy

