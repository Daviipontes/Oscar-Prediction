#%%

import pandas as pd
import numpy as np
import warnings
import itertools

# Modelos
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier
from lightgbm import LGBMClassifier
from catboost import CatBoostClassifier

# Ferramentas e Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.metrics import average_precision_score

#SETUP E CARREGAMENTO

warnings.filterwarnings('ignore')

def load_data():
    try:
        df = pd.read_csv("data/gold/oscar_ml_dataset_final.csv", sep=';')
    except:
        try:
            df = pd.read_csv("oscar_ml_dataset_final.csv", sep=';')
        except FileNotFoundError:
            print("❌ Erro: Arquivo 'oscar_ml_dataset_final.csv' não encontrado.")
            return None

    # Limpeza básica de nulos
    cols_zero = [
        'budget', 'revenue', 'popularity', 'vote_average', 'vote_count', 
        'director_prev_wins', 'director_prev_nominations', 'cast_prestige',
        'runtime', 'is_top_studio', 'is_biopic'
    ]
    for c in cols_zero:
        if c in df.columns: df[c] = df[c].fillna(0)
    
    for c in ['keywords', 'studios', 'mpaa_rating']:
        if c in df.columns: df[c] = df[c].fillna('').astype(str)
            
    return df


#FEATURE ENGINEERING (Versão 3.1)

def create_features(df):
    df = df.copy()
    
    # Logaritmos
    df['log_budget'] = np.log1p(df['budget'])
    df['log_cast_prestige'] = np.log1p(df['cast_prestige'])
    
    # Contexto
    df['is_rated_R'] = df['mpaa_rating'].apply(lambda x: 1 if 'R' in str(x) else 0)
    
    if 'release_date' in df.columns:
        df['release_month'] = pd.to_datetime(df['release_date'], errors='coerce').dt.month
        df['is_gold_season'] = df['release_month'].apply(lambda x: 1 if x in [10, 11, 12] else 0)
    else:
        df['is_gold_season'] = 0

    # Keyword Score
    oscar_buzzwords = [
        'biography', 'based on true story', 'historical', 'world war', 'holocaust', 
        'slavery', 'racism', 'disability', 'dying', 'family relationships', 'lgbt', 
        'politics', 'journalism', 'hollywood', 'music', 'addiction'
    ]
    
    def calculate_keyword_score(kw_str):
        if not kw_str or kw_str == 'nan': return 0
        score = 0
        kw_lower = kw_str.lower()
        for word in oscar_buzzwords:
            if word in kw_lower: score += 1
        return score

    df['keyword_oscar_score'] = df['keywords'].apply(calculate_keyword_score)

    # Seleção Final
    manual_features = [
        'log_budget', 'popularity',
        'director_prev_wins', 'director_prev_nominations',
        'log_cast_prestige',
        'is_top_studio', 'is_biopic',
        'is_gold_season', 'is_rated_R', 'runtime',
        'keyword_oscar_score'
    ]
    
    genre_features = [c for c in df.columns if c.startswith('genre_')]
    feature_list = manual_features + genre_features
    final_features = [f for f in feature_list if f in df.columns]
    
    # Preenchimento de segurança para NaNs residuais
    df[final_features] = df[final_features].fillna(0)
    
    return df, final_features


# XGBOOST TUNING

def temporal_cv_score(df, features, params):
    """
    Função auxiliar para o Grid Search.
    """
    years = range(2000, 2025)
    aps = []
    
    # Pipeline temporário para o teste

    model = Pipeline([
        ('imputer', SimpleImputer(strategy='constant', fill_value=0)),
        ('scaler', StandardScaler()),
        ('clf', XGBClassifier(**params, scale_pos_weight=5, random_state=42, n_jobs=-1, verbosity=0, subsample=0.8, colsample_bytree=0.8))
    ])
    
    for test_year in years:
        train = df[df['year'] < test_year]
        test = df[df['year'] == test_year].copy()
        
        if test['oscar_winner'].sum() == 0: continue
        
        X_train = train[features]
        y_train = train['oscar_nominated']
        X_test = test[features]
        
        model.fit(X_train, y_train)
        probs = model.predict_proba(X_test)[:, 1]
        
        try:
            ap = average_precision_score(test['oscar_nominated'], probs)
            aps.append(ap)
        except: pass
        
    return np.mean(aps)

def tune_xgboost(df, features):
    print("\n" + "="*80)
    print("🎛️ INICIANDO GRID SEARCH NO XGBOOST")
    print("="*80)
    
    param_grid = {
        'n_estimators': [100, 200],
        'learning_rate': [0.01, 0.03],
        'max_depth': [3, 4]
    }
    
    keys, values = zip(*param_grid.items())
    combinations = [dict(zip(keys, v)) for v in itertools.product(*values)]
    
    print(f"Testando {len(combinations)} combinações...")
    
    best_score = -1
    best_params = None
    
    for i, params in enumerate(combinations):
        score = temporal_cv_score(df, features, params)
        if score > best_score:
            best_score = score
            best_params = params
            
    print(f"✅ Melhores Parâmetros: {best_params} (MAP: {best_score:.4f})")
    return best_params

# 4BENCHMARK DE MODELOS (Com Pipeline e Re-treino)

def run_benchmark(df, features, xgb_tuned_params=None):
    print("\n" + "="*80)
    print(f"BENCHMARK DE MODELOS (2000-2024)")
    print("="*80)
    
    if xgb_tuned_params is None:
        xgb_tuned_params = {'n_estimators': 200, 'max_depth': 4, 'learning_rate': 0.03}
    
    # Classificadores Base
    clfs = {
        "LogReg": LogisticRegression(class_weight='balanced', max_iter=2000, solver='liblinear'),
        "RForest": RandomForestClassifier(n_estimators=200, max_depth=6, class_weight='balanced', random_state=42, n_jobs=-1),
        "XGBoost": XGBClassifier(**xgb_tuned_params, scale_pos_weight=5, random_state=42, n_jobs=-1, verbosity=0, subsample=0.8),
        "LightGBM": LGBMClassifier(n_estimators=200, max_depth=4, learning_rate=0.03, class_weight='balanced', random_state=42, n_jobs=-1, verbose=-1),
        "CatBoost": CatBoostClassifier(iterations=200, depth=4, learning_rate=0.03, auto_class_weights='Balanced', random_state=42, verbose=0, allow_writing_files=False)
    }
    
    # Criacao dos Pipelines
    pipelines = {}
    for name, clf in clfs.items():
        pipelines[name] = Pipeline([
            ('imputer', SimpleImputer(strategy='constant', fill_value=0)),
            ('scaler', StandardScaler()),
            ('clf', clf)
        ])
    
    global_results = {name: {'recalls': [], 'aps': []} for name in pipelines}
    
    header = f"{'ANO':<6} | {'VENCEDOR':<20}"
    for name in pipelines:
        header += f" | {name[:3]} Rank"
    print(header)
    print("-" * (len(header) + 5))
    
    years = range(2000, 2025)
    
    for test_year in years:
        train = df[df['year'] < test_year]
        test = df[df['year'] == test_year].copy()
        
        real_winner = test[test['oscar_winner'] == 1]
        real_nominees = test[test['oscar_nominated'] == 1]
        
        if real_winner.empty: continue
        

        X_train = train[features]
        y_train = train['oscar_nominated']
        X_test = test[features]

        
        row_str = f"{test_year:<6} | {str(real_winner.iloc[0]['title'])[:20]:<20}"
        
        for name, pipe in pipelines.items():
            pipe.fit(X_train, y_train)
            probs = pipe.predict_proba(X_test)[:, 1]
            test[f'prob_{name}'] = probs
            
            ranked = test.sort_values(f'prob_{name}', ascending=False).reset_index(drop=True)
            
            try:
                winner_rank = ranked[ranked['id'] == real_winner.iloc[0]['id']].index[0] + 1
            except IndexError:
                winner_rank = 999
            
            indicator = "*" if winner_rank == 1 else ("+" if winner_rank <= 10 else " ")
            row_str += f" | {winner_rank:<3} {indicator}"
            
            top_10_ids = set(ranked.head(10)['id'])
            real_ids = set(real_nominees['id'])
            found = len(top_10_ids.intersection(real_ids))
            recall = found / len(real_ids) if len(real_ids) > 0 else 0
            
            global_results[name]['recalls'].append(recall)
            global_results[name]['aps'].append(average_precision_score(test['oscar_nominated'], probs))
            
        print(row_str)

    print("-" * 80)
    print("RESULTADO FINAL:")
    
    best_model_name = None
    best_score = -1
    
    print(f"{'MODELO':<10} | {'RECALL (Top 10)':<15} | {'MAP (Precision)':<15}")
    for name, metrics in global_results.items():
        avg_recall = np.mean(metrics['recalls'])
        avg_ap = np.mean(metrics['aps'])
        print(f"{name:<10} | {avg_recall:.1%}           | {avg_ap:.3f}")
        
        if avg_ap > best_score:
            best_score = avg_ap
            best_model_name = name
            
    print(f"\nMelhor Modelo: {best_model_name}")
    
    print(f"\nRetreinando {best_model_name} com todo o historico (2000-2024)...")
    
    final_pipeline = pipelines[best_model_name]
    
    full_history = df[df['year'] <= 2024]
    X_full = full_history[features]
    y_full = full_history['oscar_nominated']
    
    final_pipeline.fit(X_full, y_full)
    
    return final_pipeline, best_model_name


# 5. PREDIÇÃO 2026 

def predict_2026(df, features, trained_pipeline, model_name):
    print("\n" + "="*80)
    print(f"🔮 PREVISÃO 2026 (Usando {model_name})")
    print("="*80)
    
    future = df[df['year'] >= 2025].copy()
    
    if future.empty:
        print("⚠️ Nenhum filme futuro encontrado.")
        return

    X_future = future[features]
    
    probs = trained_pipeline.predict_proba(X_future)[:, 1]
    
    future['prob_win'] = probs
    future['final_score'] = future['prob_win'] * (1 + 0.05 * future['keyword_oscar_score'])
    
    ranking = future.sort_values('final_score', ascending=False)
    
    cols = ['title', 'final_score', 'prob_win', 'keyword_oscar_score', 'is_top_studio', 'cast_prestige']
    if 'genre_Drama' in future.columns: cols.append('genre_Drama')
        
    print(ranking[cols].head(15).to_string(index=False))
    
    ranking.to_csv(f"previsao_2026_{model_name}.csv", index=False)
    print(f"\n✅ Salvo: previsao_2026_{model_name}.csv")


if __name__ == "__main__":
    df_raw = load_data()
    if df_raw is not None:
        df_clean, feats = create_features(df_raw)
        

        best_xgb_params = tune_xgboost(df_clean, feats)

        best_pipeline, best_name = run_benchmark(df_clean, feats, best_xgb_params)

        predict_2026(df_clean, feats, best_pipeline, best_name)
# %%
