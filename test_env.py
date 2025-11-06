#%%
import os
from dotenv import load_dotenv

print(f"Estou em: {os.getcwd()}")

# Tenta carregar o .env
success = load_dotenv()

if success:
    print("✅ Sucesso! O arquivo .env foi encontrado e lido.")
else:
    print("❌ Falha! O arquivo .env NÃO foi encontrado neste diretório.")

# Agora, vamos checar a variável
token = os.environ.get('TMDB_TOKEN')

if token:
    print(f"✅ Sucesso! A variável TMDB_TOKEN foi carregada: {token[:4]}...")
else:
    print("❌ Falha! A variável TMDB_TOKEN não foi encontrada no ambiente.")