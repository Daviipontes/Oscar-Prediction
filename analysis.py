#%%
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

from dotenv import load_dotenv
from pathlib import Path
from itertools import islice

script_dir = Path(__file__).resolve().parent
df_gold = pd.read_csv(
    script_dir / "data" / "gold" / "oscar_ml_dataset_final.csv",
    sep=';'
)
print(df_gold.head())
# %%
df_gold.info()
# %%
print(df_gold.columns.tolist())
# %%


# %%

# Define o estilo visual
sns.set_theme(style="whitegrid")

plt.figure(figsize=(8, 5))

# Plota a contagem de vencedores vs não vencedores
ax = sns.countplot(data=df_gold, x='oscar_winner', palette='viridis')

plt.title('Distribuição de Vencedores do Oscar (Target Class)')
plt.xlabel('Ganhou Oscar? (0=Não, 1=Sim)')
plt.ylabel('Contagem de Filmes')

# Adiciona os números em cima das barras
for p in ax.patches:
    ax.annotate(f'{int(p.get_height())}', (p.get_x() + p.get_width() / 2., p.get_height()), 
                ha='center', va='bottom')

plt.show()
# %%
plt.figure(figsize=(12, 6))

# Pega apenas os 15 países mais frequentes para o gráfico não ficar ilegível
top_countries = df_gold['country_iso_3'].value_counts().index[:15]

sns.countplot(data=df_gold[df_gold['country_iso_3'].isin(top_countries)], 
              x='country_iso_3', 
              order=top_countries, # Garante que fique ordenado do maior para o menor
              palette='magma')

plt.title('Top 15 Países no Dataset')
plt.xlabel('País (ISO-3)')
plt.ylabel('Número de Filmes')
plt.xticks(rotation=45)
plt.show()
# %%
# 1. Identifica as colunas de gênero
genre_cols = [col for col in df_gold.columns if col.startswith('genre_')]

# 2. Soma cada coluna (quantos filmes têm aquele gênero)
genre_counts = df_gold[genre_cols].sum().sort_values(ascending=False)

# 3. Plota usando barplot (já que já calculamos a soma manualmente)
plt.figure(figsize=(12, 8))
sns.barplot(x=genre_counts.values, y=genre_counts.index, palette='coolwarm')

plt.title('Frequência dos Gêneros nos Filmes')
plt.xlabel('Quantidade de Filmes')
plt.ylabel('Gênero')
plt.show()
# %%
sns.histplot(data=df_gold, x='budget', bins=30, kde=True)



# %%
import matplotlib.pyplot as plt
import seaborn as sns

# 1. Definir as colunas econômicas novamente (garantia)
econ_cols = ['inflation', 'gdp_growth', 'gdp_per_capita', 'gini_index', 'unemployment']

# 2. Filtrar apenas as linhas que têm algum dado faltando
mask_missing = df_gold[econ_cols].isnull().any(axis=1)
df_missing = df_gold[mask_missing]

# 3. Contar a frequência de ANOS com dados faltantes
# Pegamos o top 20 anos com mais buracos
missing_counts_year = df_missing['year'].value_counts().reset_index().head(20)
missing_counts_year.columns = ['year', 'missing_count']

# 4. Plotar
plt.figure(figsize=(12, 6))
sns.set_theme(style="whitegrid")

ax = sns.barplot(
    data=missing_counts_year,
    x='year', 
    y='missing_count',
    order=missing_counts_year.sort_values('year')['year'], # Ordena cronologicamente para facilitar a leitura
    palette='magma'
)

plt.title('Top 20 Anos com Mais Dados Econômicos Faltantes', fontsize=15)
plt.xlabel('Ano', fontsize=12)
plt.ylabel('Quantidade de Filmes Afetados', fontsize=12)
plt.xticks(rotation=45)

# Adiciona o número exato em cima da barra
for p in ax.patches:
    ax.annotate(f'{int(p.get_height())}', 
                (p.get_x() + p.get_width() / 2., p.get_height()), 
                ha='center', va='bottom')

plt.show()
# %%

cols_to_interpolate = ['inflation', 'gdp_growth', 'gdp_per_capita', 'unemployment']

df_gold = df_gold.sort_values(by=['country_iso_3', 'year'])

df_gold[cols_to_interpolate] = df_gold.groupby('country_iso_3')[cols_to_interpolate].transform(
    lambda x: x.interpolate(method='linear', limit_direction='both')
)

# fallback 
for col in cols_to_interpolate:
    remaining_nans = df_gold[col].isna().sum()
    if remaining_nans > 0:
        print(f"Ainda restam {remaining_nans} nulos em '{col}' (países sem dados). Preenchendo com a média global.")
        df_gold[col] = df_gold[col].fillna(df_gold[col].mean())

print("Interpolação concluída!")
#%%
# Filtra o DataFrame onde 'country_iso_2' é NaN
filmes_sem_pais = df_gold[df_gold['country_iso_2'].isna()]

# Mostra quantas linhas foram encontradas
print(f"Total de filmes sem país: {len(filmes_sem_pais)}")

# Exibe as 10 primeiras linhas (Título e Ano para facilitar a identificação)
print(filmes_sem_pais[['id', 'title', 'year', 'country_iso_2']].head(10))

# 1. Filtra filmes que NÃO têm país MAS foram indicados ao Oscar

filmes_importantes_sem_pais = df_gold[
    (df_gold['country_iso_2'].isna()) & 
    (df_gold['oscar_nominated'] == 1)
]

# 2. Mostra a quantidade
print(f"⚠️ Alerta: Existem {len(filmes_importantes_sem_pais)} filmes INDICADOS AO OSCAR sem país identificado.")

# 3. Mostra quais são eles (para você decidir se arruma manualmente)
if not filmes_importantes_sem_pais.empty:
    colunas_para_ver = ['id', 'title', 'year', 'oscar_winner', 'oscar_nominated']
    print(filmes_importantes_sem_pais[colunas_para_ver].sort_values('year'))
else:
    print("✅ Ótima notícia: Todos os filmes sem país são irrelevantes para o Oscar (não foram indicados).")

# Mantém apenas filmes que NÃO são nulos E NÃO são vazios
df_gold = df_gold[
    (df_gold['country_iso_2'].notna()) & 
    (df_gold['country_iso_2'] != '')
]
df_gold = df_gold[
    (df_gold['country_iso_3'].notna()) & 
    (df_gold['country_iso_3'] != '')
]

print(f"Total limpo: {len(df_gold)}")

#%%
# 1. Cria um novo DataFrame apenas com os vencedores
filmes_vencedores = df_gold[df_gold['oscar_winner'] == 1]

# 2. Mostra quantos são
print(f"Total de vencedores do Oscar no dataset: {len(filmes_vencedores)}")

# 3. Visualiza as colunas mais importantes (para confirmar)
cols_to_show = ['title', 'year',]
print(filmes_vencedores[cols_to_show].sort_values('year', ascending=False))

#%%
##At this point, im gonna first try without title and overview from films, based only in genres, and economical
y=df_gold['oscar_winner']
X=df_gold.drop(columns=['gini_index','oscar_winner','title','overview','imdb_id','backdrop_path','release_date','poster_path'])
X

# %%
# %%
print("--------------- FEATURES INFO ------------------\n",X.describe())
print("--------------- FEATURES HEAD ------------------\n",X.head())
print("--------------- TARGET INFO ------------------\n",y.describe())
print("--------------- TARGET HEAD ------------------\n",y.head())
# %%
X.info()
# %%
