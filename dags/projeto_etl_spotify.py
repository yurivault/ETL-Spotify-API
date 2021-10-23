import sqlalchemy
import pandas as pd 
import requests
import datetime
import sqlite3

database_location = "sqlite:///minhas_musicas.sqlite"
spotify_user_id = '31gvxltw5zsewtjht2dgm4lz4cvm' # Meu usuário no spotify
refresh_token = "AQAv98TLnoEv5-FWOiAGO0G59lLVsjpXirjyf0-QbLDx2bEq6Nvr-SHxoCktIg6huYiLJ9v3-K_Sjk-pEIpyfO1rNaKiqSpGSK-rUny2q4wCoIBp69BvQEPh7K93wS1DAf4"
base_64 = "ZGYxNDM3NjNkZjA5NGM2MTliOTRjZWE4NGE1YjY1Zjc6Njg4NGM5ZWM0YmY2NGUyMDlmNzk4MmVmODNiZGNlYmY=" #users id 
access_token = "BQBvVOdzrRVzbW-3KqEQh5Sxs-HF3FpmjxT0PHGWtBk-Ft0XpSOqF17QatNstfeu-QwLJ_DHajsg6Dj2yA9uDOpJc0NPU7AP44Xm8YDJ4hUZZ5vTrTWH26ms5AtfP8tgxyHknOmdjEVMj_tVOnVh2D2YvoQeTYJ9p97yt8zs8dBM1uc"

#   Criando uma classe de Refresh para dar refresh no token do spotify 
class Refresh:

    def __init__(self):
        self.refresh_token = refresh_token
        self.base_64 = base_64

    def refresh(self):
        query = "https://accounts.spotify.com/api/token"
        response = requests.post(query,
                                 data={"grant_type": "refresh_token",
                                       "refresh_token": refresh_token},
                                 headers={"Authorization": "Basic " + base_64})
        response_json = response.json()
        print(response_json)
        return response_json["access_token"]
        
def data_validacao(df: pd.DataFrame) -> bool:
    # Checando se o dataframe está vazio
    if df.empty:
        print("Nenhuma música foi baixada. Finalizando a execução.")
        return False 

    # Chave primária check
    if pd.Series(df['tocado_em']).is_unique:
        pass
    else:
        raise Exception("A chave primária está violada")

    # Checando valores nulos
    if df.isnull().values.any():
        raise Exception("Foram encontrados valores nulos neste dataframe.")

    # Confirmando a data em que as músicas foram ouvidas
    carimbodata = df["carimbodata"].tolist()
    for timestamp in carimbodata:
        if (datetime.datetime.strptime(timestamp, '%Y-%m-%d')) < ontem:
            raise Exception("Pelo menos uma das músicas que foram retornadas foi ouvida há mais de 1 dia")
    return True

#   Convertendo o timestamp para milisegundos e checando se todos os carimbodata que eu peguei foram de ontem
hoje = datetime.datetime.now()
ontem = (hoje - datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
ontem_unix_timestamp = int(ontem.timestamp()) * 1000

#   Começando o ETL
class comecar_sistema:
    
    def __init__(self):
        self.user_id = spotify_user_id
        self.spotify_token = ""

    def comecar_spotify_etl(self):

        # Fazendo download de todos as músicas que eu escutei "depois de ontem", ou seja, nas últimas 24h      
        query = "https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=ontem_unix_timestamp)
        response = requests.get(query, 
                                headers = {"Content-Type" : "application/json",
                                           "Authorization" : "Bearer {}".format(self.spotify_token)})
        response_json = response.json()
        print(response)
              
        carimbodatas = []
        lista_tocada_em = []
        nome_das_musicas = []
        nome_dos_artistas = []

        # Extraindo apenas os dados relevantes do json object         
        for song in response_json["items"]:
            nome_das_musicas.append(song["track"]["name"])
            nome_dos_artistas.append(song["track"]["album"]["artists"][0]["name"])
            lista_tocada_em.append(song["played_at"])
            carimbodatas.append(song["played_at"][0:10])
            
        # Preparando um dicionário pra depois criar um dataframe usando pandas       
        dicionario_musica = {
            "nome_musica" : nome_das_musicas,
            "nome_artista": nome_dos_artistas,
            "tocado_em" : lista_tocada_em,
            "carimbodata" : carimbodatas
        }

        #Criando dataframe
        musica_df = pd.DataFrame(dicionario_musica, columns = ["nome_musica", "nome_artista", "tocado_em", "carimbodata"])
        
        # Validando
        if data_validacao(musica_df):
            print("Os dados são válidos, prosseguindo para o carregamento")

        # Carregando

        engine = sqlalchemy.create_engine(database_location)
        conn = sqlite3.connect('minhas_musicas.sqlite')
        cursor = conn.cursor()


        sql_query = """CREATE TABLE IF NOT EXISTS minhas_musicas(
                        nome_musica VARCHAR(200),
                        nome_artista VARCHAR(200),
                        tocado_em VARCHAR(200),
                        carimbodata VARCHAR(200),
                        CONSTRAINT primary_key_constraint PRIMARY KEY (tocado_em)
                    )
                    """
        
        cursor.execute(sql_query)
        print("Database criado com sucesso.")

        try:
            musica_df.to_sql("minhas_musicas", engine, index=False, if_exists='append')
        except:
            print("Estes dados já existem na base.")

        
        conn.close()
        print("Database fechado com sucesso.")
    
    def executar_refresh(self):

        print("Recarregando token token")
        refreshCaller = Refresh()
        self.spotify_token = refreshCaller.refresh()
        self.comecar_spotify_etl()


def executar_etl():
    a = comecar_sistema()
    a.executar_refresh()



    


