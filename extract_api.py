#%%


from pathlib import Path
import requests
import os
import time
from dotenv import load_dotenv
from pathlib import Path
from itertools import islice
script_dir = Path(__file__).resolve().parent


env_path = script_dir / ".env"
success = load_dotenv(dotenv_path=env_path)




ACCESS_TOKEN = os.environ.get('TMDB_TOKEN') 
STAGING_DIR = "data/raw_tmdb"

HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "accept": "application/json"
}

def get_all_movies_by_year(start_year, end_year):
   
    discover_url = "https://api.themoviedb.org/3/discover/movie"
    movie_detail_url_base = "https://api.themoviedb.org/3/movie/"
    
    # Loop externo: Itera pelos anos
    for year in range(start_year, end_year + 1):
        print(f"\n--- Iniciando extração para o ano: {year} ---")
        current_page = 1
        total_pages = 1
        
        while current_page <= total_pages:
            params = {
                'include_adult': 'false', 'language': 'en-US',
                'sort_by': 'popularity.desc', 'year': year,
                'page': current_page
            }
            
            try:
                # 1. FAZ A CHAMADA DE "DESCOBERTA" (/discover)
                response = requests.get(discover_url, headers=HEADERS, params=params)
                response.raise_for_status() 
                data = response.json()
                
                if current_page == 1:
                    total_pages = data.get('total_pages', 0)
                    print(f"Ano {year} tem {total_pages} páginas.")

                results = data.get('results', [])
                if not results and current_page == 1:
                    print(f"Nenhum resultado encontrado para o ano {year}.")
                    break
                
                print(f"  Processando página {current_page}/{total_pages}...")
                
                # 2. FAZ A CHAMADA DE "DETALHE" (/movie/{id}) PARA CADA FILME
                for movie_summary in results:
                    movie_id = movie_summary.get('id')
                    if not movie_id:
                        continue
                        
                    try:
                        # Busca o detalhe completo
                        detail_response = requests.get(
                            f"{movie_detail_url_base}{movie_id}", 
                            headers=HEADERS
                        )
                        detail_response.raise_for_status()
                        movie_detail = detail_response.json()
                        
                        # "Entrega" o JSON com os detalhes completos
                        yield movie_detail 
                        
                        time.sleep(0.1) # Pausa curta para não sobrecarregar a API

                    except requests.exceptions.RequestException as e_detail:
                        print(f"    Erro ao buscar detalhe do ID {movie_id}: {e_detail}")
                
                current_page += 1
                time.sleep(0.25) # Pausa entre páginas
                
            except requests.exceptions.RequestException as e_discover:
                print(f"Erro na requisição /discover para {year}, pág {current_page}: {e_discover}")
                time.sleep(5)
                continue

#%%
if __name__ == "__main__":
    

    START_YEAR = 1928
    END_YEAR = 1931 
    
    # 1. Cria o iterador (o generator ainda não rodou)
    movie_generator = get_all_movies_by_year(START_YEAR, END_YEAR)
    

    
    # Define o tamanho do seu lote (batch)
    BATCH_SIZE = 100 
    all_my_movies = [] # O array que você queria

    print(f"\nIniciando processamento em lotes de {BATCH_SIZE}...")
    
    # 'islice' pega BATCH_SIZE itens do generator de cada vez

    while batch := list(islice(movie_generator, BATCH_SIZE)):
        print(f"--- Processado um lote de {len(batch)} filmes ---")
        
      
        all_my_movies.extend(batch)
        if batch:
            print(f"  Primeiro filme do lote: '{batch[0]['title']}' ({batch[0]['release_date']})")
            
    print("\n✅ Extração completa!")
    print(f"Total de filmes extraídos no array: {len(all_my_movies)}")
# %%

def process_film(film): 
  film_id = film.get('id')
  imdb_id = film.get('imdb_id')
  if not imdb_id: 
      print(f"  -> AVISO: Filme ID {film_id} ('{film.get('title')}') está a ser ignorado (sem imdb_id).")
      return None, None # Retorna None para ambos

  result = {}

 
  result['id'] = film_id
  result['imdb_id'] = imdb_id # Agora sabemos que este é válido
  result['title'] = film.get('title')
  result['adult'] = film.get('adult', None)
  result['backdrop_path'] = film.get('backdrop_path', None)
  result['budget'] = film.get('budget', None)
  result['revenue'] = film.get('revenue', None)
  result['original_language'] = film.get('original_language', None)
  result['overview'] = film.get('overview', None)
  result['popularity'] = film.get('popularity', None)
  result['poster_path'] = film.get('poster_path', None)
  release_date_str = film.get('release_date', None)
  result['release_date'] = None if release_date_str == "" else release_date_str
  
  result['video'] = film.get('video', None)
  result['vote_count'] = film.get('vote_count', None)
  result['vote_average'] = film.get('vote_average', None)
  
  # Genres é uma lista de dicts, csperando em outro array para criar outra tabela no banco

  genres = film.get('genres', []) 
  film_id = film.get('id')
  
  processed_genres = [] 
  
  if film_id:
    for genre_dict in genres:
      processed_genre = {
          'film_id': film_id,
          'genre_id': genre_dict.get('id'),
          'genre_name': genre_dict.get('name') 
      }
      processed_genres.append(processed_genre)

  return result, processed_genres


#%%
processed_films = []
processed_genres = []

# (Assumindo que 'all_my_movies' é o seu generator ou lista de filmes brutos)
print("A processar e validar filmes brutos...")

for film in all_my_movies:
  processed_film, genres = process_film(film)
  
  # --- VERIFICAÇÃO ---
  # Só adiciona às listas se o process_film não retornou None
  if processed_film is not None:
      processed_films.append(processed_film)
      processed_genres.extend(genres)
print(f"Processamento concluído. {len(processed_films)} filmes válidos encontrados.")
print(processed_films[:5])
print(processed_genres[:5])

# %%
import psycopg2

script_dir = Path(__file__).resolve().parent
env_path = script_dir / ".env"
load_dotenv(dotenv_path=env_path)


DB_USERNAME = os.environ.get('DB_USERNAME')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT', '5432')
DB_NAME = os.environ.get('DB_NAME')

CREATE_FILMS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS tmdb_films_silver (
    id BIGINT PRIMARY KEY,
    imdb_id TEXT,
    title TEXT,
    adult BOOLEAN,
    backdrop_path TEXT,
    budget BIGINT,
    revenue BIGINT,
    original_language VARCHAR(10),
    overview TEXT,
    popularity DOUBLE PRECISION,
    poster_path TEXT,
    release_date DATE,
    video BOOLEAN,
    vote_count INTEGER,
    vote_average DOUBLE PRECISION,
    
    
    CONSTRAINT idx_imdb_id UNIQUE (imdb_id) 
);
"""

CREATE_GENRES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS tmdb_genres_silver (
    film_id BIGINT,
    genre_id INTEGER,
    genre_name TEXT,
    FOREIGN KEY (film_id) REFERENCES tmdb_films_silver (id) ON DELETE CASCADE,
    PRIMARY KEY (film_id, genre_id)
);
"""

conn = None

try:
    conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USERNAME,
            password=DB_PASSWORD,
            port=DB_PORT
        )
    cur = conn.cursor()

    cur.execute(CREATE_FILMS_TABLE_SQL)
    cur.execute(CREATE_GENRES_TABLE_SQL)
    conn.commit()
    print("Tabelas criadas")
    
    
    if not processed_films or not processed_genres:
        print("Processed_films ou processed_genres estão vazios. ")
    else:
        
       
        cols_films = processed_films[0].keys() 
        cols_str = ", ".join([f'"{c}"' for c in cols_films])
        placeholders = ", ".join(["%s" for _ in cols_films])
        #Filmes
        upsert_query_films = f"""
        INSERT INTO tmdb_films_silver ({cols_str})
        VALUES ({placeholders})
        ON CONFLICT (id) DO UPDATE SET {", ".join(f'"{col}"=excluded."{col}"' for col in cols_films if col != "id")};
        """
        
        data_tuples_films = [tuple(film.get(col) for col in cols_films) for film in processed_films]
        cur.executemany(upsert_query_films, data_tuples_films)

      
        # Generos
        cols_genres = processed_genres[0].keys()
        cols_str_genres = ", ".join([f'"{c}"' for c in cols_genres])
        placeholders_genres = ", ".join(["%s" for _ in cols_genres])

        upsert_query_genres = f"""
        INSERT INTO tmdb_genres_silver ({cols_str_genres})
        VALUES ({placeholders_genres})
        ON CONFLICT (film_id, genre_id) DO NOTHING;"""

        data_tuples_genres = [tuple(genre.get(col) for col in cols_genres) for genre in processed_genres]
        cur.executemany(upsert_query_genres, data_tuples_genres)

        conn.commit()
        print(f"Dados carregados .({len(processed_films)} filmes, {len(processed_genres)} géneros)")

except (Exception, psycopg2.DatabaseError) as error:
    print(f"Erro: {error}")
    if conn:
        conn.rollback() 
finally:
    if conn:
        cur.close()
        conn.close()
        print(f"Conexão com '{DB_NAME}' fechada.")
# %%
from sqlalchemy import create_engine
import pandas as pd

def create_gold_dataset():
    script_dir = Path(__file__).resolve().parent
    env_path = script_dir / ".env"
    load_dotenv(dotenv_path=env_path)

    DB_USERNAME = os.environ.get('DB_USERNAME')
    DB_PASSWORD = os.environ.get('DB_PASSWORD')
    DB_HOST = os.environ.get('DB_HOST')
    DB_PORT = os.environ.get('DB_PORT', '5432')
    DB_NAME = os.environ.get('DB_NAME')

    # Criar a "engine" do SQLAlchemy para ler dados
    db_url = f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    try:
        engine = create_engine(db_url)
        print("Conectado ao PostgreSQL com sucesso.")   
    except Exception as e:
        print(f"Erro ao conectar ao PostgreSQL: {e}")
        return
    
    # Fonte 1: Tabela de Filmes (a nossa tabela de dimensão principal)
    try:
       
        df_films = pd.read_sql("SELECT * FROM tmdb_films_silver", engine)
        print(f"Carregados {len(df_films)} filmes da base de dados.")
        
        # Fonte 2: Tabela de Géneros

        df_genres = pd.read_sql("SELECT * FROM tmdb_genres_silver", engine)
        print(f"Carregados {len(df_genres)} registos de géneros.")

        # Fonte 3: Ficheiro de Vencedores (a nossa segunda tabela de factos)
        winners_csv_path = script_dir / "data" /"processed_data" /"awards_winners.csv"
        
        df_winners = pd.read_csv(winners_csv_path, sep=';')
        print(f"Carregados {len(df_winners)} registros de prémios do CSV.")

        
        
    except Exception as e:
        print(f"Erro ao ler os dados 'Silver': {e}")
        return
    
    #Transformar os generos em colunas com 0 e 1 para cada genero fazendo o pivot para cada filme
    df_genres_pivot = pd.crosstab(
        df_genres['film_id'], 
        df_genres['genre_name']
    ).add_prefix('genre_')
    df_genres_pivot.reset_index(inplace=True)

    print("A filtrar e a fazer 'pivot' dos dados do OSCAR...")
    
    # Filtrar o CSV para APENAS Oscars (e assumindo "Best Picture")
    df_oscars = df_winners[
        (df_winners['award_source'].str.lower() == 'oscars') &
        (df_winners['award_category'].str.lower() == 'best picture')
    ]
    
    # Fazer o Pivot apenas com 'status' ('Winner' ou 'Nominated')
    df_oscars_pivot = pd.crosstab(
        df_oscars['imdb_id'],
        df_oscars['status'] # Isto cria colunas 'Nominated' e 'Winner'
    )

    if 'Winner' not in df_oscars_pivot.columns:
        df_oscars_pivot['Winner'] = 0
    if 'Nominated' not in df_oscars_pivot.columns:
        df_oscars_pivot['Nominated'] = 0
        
    df_oscars_pivot = df_oscars_pivot.rename(columns={
        'Winner': 'oscar_winner',
        'Nominated': 'oscar_nominated'
    }).reset_index()

    # Join 1 com Filmes + Géneros (usando o 'id' do TMDb)

    df_gold = pd.merge(
        df_films, 
        df_genres_pivot, 
        left_on='id',       # Chave da tabela de filmes (TMDb ID)
        right_on='film_id', # Chave da tabela de géneros (TMDb ID)
        how='left'          
    )

    # Join 2 com Oscars (usando o 'imdb_id')
    df_gold = pd.merge(
        df_gold, 
        df_oscars_pivot[['imdb_id', 'oscar_winner', 'oscar_nominated']], # Apenas as colunas que queremos
        on='imdb_id',       
        how='left'          
    )

    # Agora vamos preencher com 0 os NaNs nas colunas de Oscars    
    genre_cols = df_genres_pivot.columns.drop('film_id')
    oscar_cols = ['oscar_winner', 'oscar_nominated']
    all_pivot_cols = list(genre_cols) + oscar_cols

    df_gold[all_pivot_cols] = df_gold[all_pivot_cols].fillna(0).astype(int)
    # Retirando o id duplicado das colunas que vieram da base dos oscars
    if 'film_id' in df_gold.columns:
        df_gold = df_gold.drop(columns=['film_id'])


    #Salvar o dataset GOLD como CSV
    gold_dir = script_dir / "data" / "gold"
    os.makedirs(gold_dir, exist_ok=True)
    output_path = gold_dir / "oscar_ml_dataset_final.csv"
    
    df_gold.to_csv(output_path, index=False, sep=';')
    
if __name__ == "__main__":
    create_gold_dataset()
# %%

df_gold = pd.read_csv(
    script_dir / "data" / "gold" / "oscar_ml_dataset_final.csv",
    sep=';'
)
print(df_gold.head())
# %%
