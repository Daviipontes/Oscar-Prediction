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
    MAX_PAGES_PER_YEAR = 15
   
    discover_url = "https://api.themoviedb.org/3/discover/movie"
    movie_detail_url_base = "https://api.themoviedb.org/3/movie/"
    
    # Loop externo: Itera pelos anos
    for year in range(start_year, end_year + 1):
        print(f"\n--- Iniciando extração para o ano: {year} ---")
        current_page = 1
        total_pages = 1
        
        while current_page <= total_pages and current_page <= MAX_PAGES_PER_YEAR:
           
            params = {
                'include_adult': 'false',
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
                        detail_url = f"{movie_detail_url_base}{movie_id}"
                        detail_params = {'append_to_response': 'credits,keywords,release_dates'} # <--- O SEGREDO

                        detail_response = requests.get(
                            detail_url, 
                            headers=HEADERS, 
                            params=detail_params # Adiciona isto
                        )
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
                time.sleep(2)
                continue

#%%
if __name__ == "__main__":
    

    START_YEAR = 2000
    END_YEAR = 2025 
    
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

#%%
# 1. Seus dados de entrada (A lista que você forneceu)
raw_targets = """341;2000;Oscars;Best Picture;Gladiator;Winner;tt0172495
342;2000;Oscars;Best Picture;Chocolat;Nominated;tt0127886
343;2000;Oscars;Best Picture;Crouching Tiger, Hidden Dragon;Nominated;tt0190332
344;2000;Oscars;Best Picture;Erin Brockovich;Nominated;tt0195685
345;2000;Oscars;Best Picture;Traffic;Nominated;tt0181865
346;2001;Oscars;Best Picture;A Beautiful Mind;Winner;tt0268978
347;2001;Oscars;Best Picture;Gosford Park;Nominated;tt0280707
348;2001;Oscars;Best Picture;In the Bedroom;Nominated;tt0247425
349;2001;Oscars;Best Picture;The Lord of the Rings: The Fellowship of the Ring;Nominated;tt0120737
350;2001;Oscars;Best Picture;Moulin Rouge!;Nominated;tt0203009
351;2002;Oscars;Best Picture;Chicago;Winner;tt0299658
352;2002;Oscars;Best Picture;Gangs of New York;Nominated;tt0217505
353;2002;Oscars;Best Picture;The Hours;Nominated;tt0274558
354;2002;Oscars;Best Picture;The Lord of the Rings: The Two Towers;Nominated;tt0167261
355;2002;Oscars;Best Picture;The Pianist;Nominated;tt0253474
356;2003;Oscars;Best Picture;The Lord of the Rings: The Return of the King;Winner;tt0167260
357;2003;Oscars;Best Picture;Lost in Translation;Nominated;tt0335266
358;2003;Oscars;Best Picture;Master and Commander: The Far Side of the World;Nominated;tt0311113
359;2003;Oscars;Best Picture;Mystic River;Nominated;tt0327056
360;2003;Oscars;Best Picture;Seabiscuit;Nominated;tt0329575
361;2004;Oscars;Best Picture;Million Dollar Baby;Winner;tt0405159
362;2004;Oscars;Best Picture;The Aviator;Nominated;tt0338751
363;2004;Oscars;Best Picture;Finding Neverland;Nominated;tt0309941
364;2004;Oscars;Best Picture;Ray;Nominated;tt0350258
365;2004;Oscars;Best Picture;Sideways;Nominated;tt0375063
366;2005;Oscars;Best Picture;Crash;Winner;tt0375679
367;2005;Oscars;Best Picture;Brokeback Mountain;Nominated;tt0388795
368;2005;Oscars;Best Picture;Capote;Nominated;tt0379725
369;2005;Oscars;Best Picture;Good Night, and Good Luck.;Nominated;tt0433383
370;2006;Oscars;Best Picture;The Departed;Winner;tt0407887
371;2006;Oscars;Best Picture;Babel;Nominated;tt0449467
372;2006;Oscars;Best Picture;Letters from Iwo Jima;Nominated;tt0498380
373;2006;Oscars;Best Picture;Little Miss Sunshine;Nominated;tt0449059
374;2006;Oscars;Best Picture;The Queen;Nominated;tt0436069
375;2007;Oscars;Best Picture;No Country for Old Men;Winner;tt0477348
376;2007;Oscars;Best Picture;Atonement;Nominated;tt0485291
377;2007;Oscars;Best Picture;Juno;Nominated;tt0467406
378;2007;Oscars;Best Picture;Michael Clayton;Nominated;tt0465538
379;2007;Oscars;Best Picture;There Will Be Blood;Nominated;tt0469494
380;2008;Oscars;Best Picture;Slumdog Millionaire;Winner;tt1010048
381;2008;Oscars;Best Picture;The Curious Case of Benjamin Button;Nominated;tt0421715
382;2008;Oscars;Best Picture;Frost/Nixon;Nominated;tt0870111
383;2008;Oscars;Best Picture;Milk;Nominated;tt1013753
384;2008;Oscars;Best Picture;The Reader;Nominated;tt0976051
385;2009;Oscars;Best Picture;The Hurt Locker;Winner;tt0887912
386;2009;Oscars;Best Picture;Avatar;Nominated;tt0499549
387;2009;Oscars;Best Picture;The Blind Side;Nominated;tt1235807
388;2009;Oscars;Best Picture;District 9;Nominated;tt1136608
389;2009;Oscars;Best Picture;An Education;Nominated;tt1174732
390;2009;Oscars;Best Picture;Inglourious Basterds;Nominated;tt0361748
391;2009;Oscars;Best Picture;Precious: Based on the Novel "Push" by Sapphire;Nominated;tt0929429
392;2009;Oscars;Best Picture;A Serious Man;Nominated;tt1019452
393;2009;Oscars;Best Picture;Up;Nominated;tt1049413
394;2009;Oscars;Best Picture;Up in the Air;Nominated;tt1193138
395;2010;Oscars;Best Picture;The King's Speech;Winner;tt1504320
396;2010;Oscars;Best Picture;Black Swan;Nominated;tt0947798
397;2010;Oscars;Best Picture;The Fighter;Nominated;tt0498465
398;2010;Oscars;Best Picture;Inception;Nominated;tt1375666
399;2010;Oscars;Best Picture;The Kids Are All Right;Nominated;tt1403061
400;2010;Oscars;Best Picture;127 Hours;Nominated;tt1542344
401;2010;Oscars;Best Picture;The Social Network;Nominated;tt1285016
402;2010;Oscars;Best Picture;Toy Story 3;Nominated;tt0435761
403;2010;Oscars;Best Picture;True Grit;Nominated;tt1403865
404;2010;Oscars;Best Picture;Winter's Bone;Nominated;tt1399683
405;2011;Oscars;Best Picture;The Artist;Winner;tt1655442
406;2011;Oscars;Best Picture;The Descendants;Nominated;tt1033575
407;2011;Oscars;Best Picture;Extremely Loud & Incredibly Close;Nominated;tt0477302
408;2011;Oscars;Best Picture;The Help;Nominated;tt1454029
409;2011;Oscars;Best Picture;Hugo;Nominated;tt0970179
410;2011;Oscars;Best Picture;Midnight in Paris;Nominated;tt1605783
411;2011;Oscars;Best Picture;Moneyball;Nominated;tt1210166
412;2011;Oscars;Best Picture;The Tree of Life;Nominated;tt1007418
413;2011;Oscars;Best Picture;War Horse;Nominated;tt1568911
414;2012;Oscars;Best Picture;Argo;Winner;tt1024648
415;2012;Oscars;Best Picture;Amour;Nominated;tt1602620
416;2012;Oscars;Best Picture;Beasts of the Southern Wild;Nominated;tt2125435
417;2012;Oscars;Best Picture;Django Unchained;Nominated;tt1853728
418;2012;Oscars;Best Picture;Life of Pi;Nominated;tt0454876
419;2012;Oscars;Best Picture;Lincoln;Nominated;tt0443272
420;2012;Oscars;Best Picture;Les Misérables;Nominated;tt1790736
421;2012;Oscars;Best Picture;Silver Linings Playbook;Nominated;tt1045658
422;2012;Oscars;Best Picture;Zero Dark Thirty;Nominated;tt1790885
423;2013;Oscars;Best Picture;12 Years a Slave;Winner;tt2024544
424;2013;Oscars;Best Picture;American Hustle;Nominated;tt1800241
425;2013;Oscars;Best Picture;Captain Phillips;Nominated;tt1535109
426;2013;Oscars;Best Picture;Dallas Buyers Club;Nominated;tt0790636
427;2013;Oscars;Best Picture;Gravity;Nominated;tt1454468
428;2013;Oscars;Best Picture;Her;Nominated;tt1798709
429;2013;Oscars;Best Picture;Nebraska;Nominated;tt1821549
430;2013;Oscars;Best Picture;Philomena;Nominated;tt2431112
431;2013;Oscars;Best Picture;The Wolf of Wall Street;Nominated;tt0993846
432;2014;Oscars;Best Picture;Birdman or (The Unexpected Virtue of Ignorance);Winner;tt2562232
433;2014;Oscars;Best Picture;American Sniper;Nominated;tt2179136
434;2014;Oscars;Best Picture;Boyhood;Nominated;tt1065073
435;2014;Oscars;Best Picture;The Grand Budapest Hotel;Nominated;tt2278388
436;2014;Oscars;Best Picture;The Imitation Game;Nominated;tt2084970
437;2014;Oscars;Best Picture;Selma;Nominated;tt1020072
438;2014;Oscars;Best Picture;The Theory of Everything;Nominated;tt2980516
439;2014;Oscars;Best Picture;Whiplash;Nominated;tt2582802
440;2015;Oscars;Best Picture;Spotlight;Winner;tt1895587
441;2015;Oscars;Best Picture;The Big Short;Nominated;tt1596363
442;2015;Oscars;Best Picture;Bridge of Spies;Nominated;tt3682448
443;2015;Oscars;Best Picture;Brooklyn;Nominated;tt3811152
444;2015;Oscars;Best Picture;Mad Max: Fury Road;Nominated;tt1392190
445;2015;Oscars;Best Picture;The Martian;Nominated;tt3659388
446;2015;Oscars;Best Picture;The Revenant;Nominated;tt1663202
447;2015;Oscars;Best Picture;Room;Nominated;tt3170832
448;2016;Oscars;Best Picture;Moonlight;Winner;tt4975722
449;2016;Oscars;Best Picture;Arrival;Nominated;tt2543164
450;2016;Oscars;Best Picture;Fences;Nominated;tt2671706
451;2016;Oscars;Best Picture;Hacksaw Ridge;Nominated;tt2119532
452;2016;Oscars;Best Picture;Hell or High Water;Nominated;tt2582782
453;2016;Oscars;Best Picture;Hidden Figures;Nominated;tt4846340
454;2016;Oscars;Best Picture;La La Land;Nominated;tt3783958
455;2016;Oscars;Best Picture;Lion;Nominated;tt3741834
456;2016;Oscars;Best Picture;Manchester by the Sea;Nominated;tt4034228
457;2017;Oscars;Best Picture;The Shape of Water;Winner;tt5580390
458;2017;Oscars;Best Picture;Call Me by Your Name;Nominated;tt5726616
459;2017;Oscars;Best Picture;Darkest Hour;Nominated;tt4555426
460;2017;Oscars;Best Picture;Dunkirk;Nominated;tt5013056
461;2017;Oscars;Best Picture;Get Out;Nominated;tt5052448
462;2017;Oscars;Best Picture;Lady Bird;Nominated;tt4925292
463;2017;Oscars;Best Picture;Phantom Thread;Nominated;tt5776858
464;2017;Oscars;Best Picture;The Post;Nominated;tt6294822
465;2017;Oscars;Best Picture;Three Billboards Outside Ebbing, Missouri;Nominated;tt5027774
466;2018;Oscars;Best Picture;Green Book;Winner;tt6966692
467;2018;Oscars;Best Picture;Black Panther;Nominated;tt1825683
468;2018;Oscars;Best Picture;BlacKkKlansman;Nominated;tt7349662
469;2018;Oscars;Best Picture;Bohemian Rhapsody;Nominated;tt1727824
470;2018;Oscars;Best Picture;The Favourite;Nominated;tt5083738
471;2018;Oscars;Best Picture;Roma;Nominated;tt6155170
472;2018;Oscars;Best Picture;A Star Is Born;Nominated;tt1517451
473;2018;Oscars;Best Picture;Vice;Nominated;tt6266538
474;2019;Oscars;Best Picture;Parasite;Winner;tt6751668
475;2019;Oscars;Best Picture;Ford v Ferrari;Nominated;tt1950186
476;2019;Oscars;Best Picture;The Irishman;Nominated;tt1302006
477;2019;Oscars;Best Picture;Jojo Rabbit;Nominated;tt2584384
478;2019;Oscars;Best Picture;Joker;Nominated;tt7286456
479;2019;Oscars;Best Picture;Little Women;Nominated;tt3281548
480;2019;Oscars;Best Picture;Marriage Story;Nominated;tt7653254
481;2019;Oscars;Best Picture;1917;Nominated;tt8579674
482;2019;Oscars;Best Picture;Once Upon a Time in Hollywood;Nominated;tt7131622
483;2020;Oscars;Best Picture;Nomadland;Winner;tt9770150
484;2020;Oscars;Best Picture;The Father;Nominated;tt10272386
485;2020;Oscars;Best Picture;Judas and the Black Messiah;Nominated;tt9784798
486;2020;Oscars;Best Picture;Mank;Nominated;tt10618286
487;2020;Oscars;Best Picture;Minari;Nominated;tt10633456
488;2020;Oscars;Best Picture;Promising Young Woman;Nominated;tt9620292
489;2020;Oscars;Best Picture;Sound of Metal;Nominated;tt5363618
490;2020;Oscars;Best Picture;The Trial of the Chicago 7;Nominated;tt1070874
491;2021;Oscars;Best Picture;CODA;Winner;tt10304186
492;2021;Oscars;Best Picture;Belfast;Nominated;tt12789558
493;2021;Oscars;Best Picture;Don't Look Up;Nominated;tt11286314
494;2021;Oscars;Best Picture;Drive My Car;Nominated;tt14039582
495;2021;Oscars;Best Picture;Dune;Nominated;tt1160419
496;2021;Oscars;Best Picture;King Richard;Nominated;tt9620212
497;2021;Oscars;Best Picture;Licorice Pizza;Nominated;tt11271038
498;2021;Oscars;Best Picture;Nightmare Alley;Nominated;tt7740496
499;2021;Oscars;Best Picture;The Power of the Dog;Nominated;tt10293406
500;2021;Oscars;Best Picture;West Side Story;Nominated;tt3581652
501;2022;Oscars;Best Picture;Everything Everywhere All at Once;Winner;tt6710474
502;2022;Oscars;Best Picture;All Quiet on the Western Front;Nominated;tt1016150
503;2022;Oscars;Best Picture;Avatar: The Way of Water;Nominated;tt1630029
504;2022;Oscars;Best Picture;The Banshees of Inisherin;Nominated;tt11813216
505;2022;Oscars;Best Picture;Elvis;Nominated;tt3704428
506;2022;Oscars;Best Picture;The Fabelmans;Nominated;tt14050738
507;2022;Oscars;Best Picture;Tár;Nominated;tt14444726
508;2022;Oscars;Best Picture;Top Gun: Maverick;Nominated;tt1745960
509;2022;Oscars;Best Picture;Triangle of Sadness;Nominated;tt7322224
510;2022;Oscars;Best Picture;Women Talking;Nominated;tt13640912
511;2023;Oscars;Best Picture;Oppenheimer;Winner;tt15398776
512;2023;Oscars;Best Picture;American Fiction;Nominated;tt23561236
513;2023;Oscars;Best Picture;Anatomy of a Fall;Nominated;tt17009710
514;2023;Oscars;Best Picture;Barbie;Nominated;tt1517268
515;2023;Oscars;Best Picture;The Holdovers;Nominated;tt14849194
516;2023;Oscars;Best Picture;Killers of the Flower Moon;Nominated;tt5537002
517;2023;Oscars;Best Picture;Maestro;Nominated;tt5535276
518;2023;Oscars;Best Picture;Past Lives;Nominated;tt13238346
519;2023;Oscars;Best Picture;Poor Things;Nominated;tt14230458
520;2023;Oscars;Best Picture;The Zone of Interest;Nominated;tt7160072
521;2024;Oscars;Best Picture;Anora;Winner;tt29102782
522;2024;Oscars;Best Picture;The Brutalist;Nominated;tt10799718
523;2024;Oscars;Best Picture;A Complete Unknown;Nominated;tt2106778
524;2024;Oscars;Best Picture;Conclave;Nominated;tt14150820
525;2024;Oscars;Best Picture;Dune: Part Two;Nominated;tt15239678
526;2024;Oscars;Best Picture;Emilia Pérez;Nominated;tt22584746
527;2024;Oscars;Best Picture;I'm Still Here;Nominated;tt31693175
528;2024;Oscars;Best Picture;Nickel Boys;Nominated;tt10480172
529;2024;Oscars;Best Picture;The Substance;Nominated;tt12198080
530;2024;Oscars;Best Picture;Wicked;Nominated;tt1300730"""

# 2. Parse da lista (extraindo o IMDB ID que é a chave única e segura)
targets_to_check = []
for line in raw_targets.strip().split('\n'):
    parts = line.split(';')
    # O índice 4 é o título, índice 6 é o IMDB ID (ttXXXXXXX)
    if len(parts) >= 7:
        targets_to_check.append({
            'title': parts[4],
            'year': int(parts[1]),
            'imdb_id': parts[6].strip()
        })

# 3. Verifica quais já foram baixados no passo anterior (bulk)
# Montamos um set com os 'imdb_id' que já estão na lista all_my_movies
current_ids = set()
for m in all_my_movies:
    # O objeto do TMDB geralmente tem o campo 'imdb_id' se veio de /movie/{id}
    if m.get('imdb_id'):
        current_ids.add(m.get('imdb_id'))

print(f"Verificando {len(targets_to_check)} filmes alvo contra {len(current_ids)} filmes já extraídos...")

# 4. Identifica os faltantes
movies_to_fetch = [t for t in targets_to_check if t['imdb_id'] not in current_ids]

if not movies_to_fetch:
    print("✅ Todos os filmes alvo já foram encontrados na busca automática!")
else:
    print(f"⚠️ Detectados {len(movies_to_fetch)} filmes faltando. Iniciando busca manual via API...")

    # Endpoint para converter IMDB_ID -> TMDB_ID
    find_url_base = "https://api.themoviedb.org/3/find/"
    movie_detail_url_base = "https://api.themoviedb.org/3/movie/"

    for target in movies_to_fetch:
        imdb_id = target['imdb_id']
        print(f"  -> Buscando manualmente: {target['title']} ({imdb_id})...")
        
        try:
            # Passo A: Achar o ID do TMDB usando o ID do IMDB
            find_url = f"{find_url_base}{imdb_id}"
            params = {'external_source': 'imdb_id'}
            
            resp = requests.get(find_url, headers=HEADERS, params=params)
            resp.raise_for_status()
            find_data = resp.json()
            
            results = find_data.get('movie_results', [])
            
            if not results:
                print(f"     ❌ ERRO: ID {imdb_id} não encontrado no TMDB.")
                continue
                
            tmdb_id = results[0]['id']
            
            # Passo B: Baixar os detalhes COMPLETOS desse ID (para ter o mesmo formato do resto)
            detail_url = f"{movie_detail_url_base}{tmdb_id}"
            detail_resp = requests.get(detail_url, headers=HEADERS)
            detail_resp.raise_for_status()
            
            movie_details = detail_resp.json()
            
            # Passo C: Adicionar à lista principal
            all_my_movies.append(movie_details)
            print(f"     ✅ Adicionado com sucesso! (TMDB ID: {tmdb_id})")
            
            # Respeita o rate limit
            time.sleep(0.25)
            
        except Exception as e:
            print(f"     ❌ Falha ao buscar {imdb_id}: {e}")

print(f"Total final de filmes após verificação manual: {len(all_my_movies)}")

#%%
import requests
import time
import os
missing_targets_manual = [
    {'title': 'The Zone of Interest', 'year': 2023, 'imdb_id': 'tt7160072'},
    {'title': 'Anora', 'year': 2024, 'imdb_id': 'tt29102782'},
    {'title': 'The Brutalist', 'year': 2024, 'imdb_id': 'tt10799718'},
    {'title': 'A Complete Unknown', 'year': 2024, 'imdb_id': 'tt2106778'},
    {'title': 'Conclave', 'year': 2024, 'imdb_id': 'tt14150820'},
    {'title': 'Emilia Pérez', 'year': 2024, 'imdb_id': 'tt22584746'},
    {'title': "I'm Still Here", 'year': 2024, 'imdb_id': 'tt31693175'},
    {'title': 'Nickel Boys', 'year': 2024, 'imdb_id': 'tt10480172'},
    {'title': 'The Substance', 'year': 2024, 'imdb_id': 'tt12198080'},
    {'title': 'Wicked', 'year': 2024, 'imdb_id': 'tt1300730'},
    {'title': 'Women Talking', 'year': 2022, 'imdb_id': 'tt13640912'},
    {'title': 'The Fabelmans', 'year': 2022, 'imdb_id': 'tt14050738'},
    {'title': 'King Richard', 'year': 2021, 'imdb_id': 'tt9620212'},
    {'title': 'CODA', 'year': 2021, 'imdb_id': 'tt10304186'},
    {'title': 'Roma', 'year': 2018, 'imdb_id': 'tt6155170'},
    {'title': 'Brooklyn', 'year': 2015, 'imdb_id': 'tt3811152'},
    {'title': 'Philomena', 'year': 2013, 'imdb_id': 'tt2431112'},
    {'title': 'The Tree of Life', 'year': 2011, 'imdb_id': 'tt1007418'},
    {'title': 'The Kids Are All Right', 'year': 2010, 'imdb_id': 'tt1403061'},
    {'title': 'The Queen', 'year': 2006, 'imdb_id': 'tt0436069'},
    {'title': 'Finding Neverland', 'year': 2004, 'imdb_id': 'tt0309941'}
]
# --- CONFIGURAÇÃO ---
# Certifique-se de que suas chaves/headers estão definidos
# HEADERS = { ... } (Use os mesmos do seu script principal)

def recover_missing_movies_hybrid(missing_list, existing_list_ref):
    """
    Tenta recuperar filmes usando estratégia híbrida e TRAZ OS DADOS EXTRAS (Credits, Keywords, etc).
    """
    print(f"\n--- 🚑 Iniciando Recuperação Híbrida de {len(missing_list)} filmes ---")
    
    find_url_base = "https://api.themoviedb.org/3/find/"
    search_url_base = "https://api.themoviedb.org/3/search/movie"
    movie_detail_url_base = "https://api.themoviedb.org/3/movie/"
    
    # [CORREÇÃO] Definir os parâmetros extras aqui também!
    detail_params = {
        'append_to_response': 'credits,keywords,release_dates'
    }
    
    recovered_movies = []
    
    for target in missing_list:
        title = target['title']
        year = target['year']
        imdb_id = target.get('imdb_id')
        
        print(f"🔎 Procurando: '{title}' ({year})...")
        
        tmdb_id = None
        

        if imdb_id:
            try:
                find_url = f"{find_url_base}{imdb_id}"
                params = {'external_source': 'imdb_id'}
                resp = requests.get(find_url, headers=HEADERS, params=params)
                if resp.status_code == 200:
                    results = resp.json().get('movie_results', [])
                    if results:
                        tmdb_id = results[0]['id']
                        print(f"   ✅ Encontrado via IMDB ID!")
            except:
                pass 

        if not tmdb_id:
            try:
                params = {'query': title, 'year': year, 'include_adult': 'false'}
                resp = requests.get(search_url_base, headers=HEADERS, params=params)
                if resp.status_code == 200:
                    results = resp.json().get('results', [])
                    if results:
                        tmdb_id = results[0]['id']
                        print(f"   ✅ Encontrado via Busca de Nome + Ano!")
            except Exception as e:
                print(f"   ⚠️ Erro na busca por nome: {e}")


        if not tmdb_id:
            try:
                params = {'query': title, 'year': year - 1, 'include_adult': 'false'}
                resp = requests.get(search_url_base, headers=HEADERS, params=params)
                if resp.status_code == 200:
                    results = resp.json().get('results', [])
                    if results:
                        tmdb_id = results[0]['id']
                        print(f"   ✅ Encontrado via Busca de Nome + Ano Anterior!")
            except:
                pass

        if tmdb_id:
            try:
                detail_url = f"{movie_detail_url_base}{tmdb_id}"
                
                # [CORREÇÃO CRÍTICA] Passamos os params aqui!
                resp = requests.get(detail_url, headers=HEADERS, params=detail_params)
                
                if resp.status_code == 200:
                    movie_data = resp.json()
                    
                    # Garante que o IMDB ID esteja lá
                    if not movie_data.get('imdb_id') and imdb_id:
                        movie_data['imdb_id'] = imdb_id
                        
                    recovered_movies.append(movie_data)
                    existing_list_ref.append(movie_data)
                else:
                    print(f"   ❌ Erro ao baixar detalhes do ID {tmdb_id}")
            except Exception as e:
                print(f"   ❌ Erro de conexão nos detalhes: {e}")
        else:
            print(f"   💀 FALHA TOTAL: Não foi possível encontrar '{title}'.")
            
        time.sleep(0.25) 

    print(f"\n✅ Recuperação concluída. {len(recovered_movies)} filmes salvos.")
    return recovered_movies
if __name__ == "__main__":
    
    # ... (Seu código anterior que preenche all_my_movies via get_all_movies_by_year) ...
    # Suponha que all_my_movies já tenha os filmes normais extraídos.
    
    print(f"Total antes da recuperação: {len(all_my_movies)}")
    
    # >>> CHAMADA DA FUNÇÃO AQUI <<<
    # Isso vai adicionar os filmes faltantes (Wicked, Anora, etc.) com os dados de CREDITS dentro de all_my_movies
    recover_missing_movies_hybrid(missing_targets_manual, all_my_movies)
    
    print(f"Total depois da recuperação: {len(all_my_movies)}")
# %%

def process_film(film): 
    film_id = film.get('id')
    imdb_id = film.get('imdb_id')
    
    # Validação inicial
    if not imdb_id: 
        # print(f"  -> AVISO: Filme ID {film_id} ignorado (sem imdb_id).")
        return None, None 

    result = {}
    
    # --- 1. DADOS BÁSICOS ---
    result['id'] = film_id
    result['imdb_id'] = imdb_id
    result['title'] = film.get('title')
    result['adult'] = film.get('adult')
    result['backdrop_path'] = film.get('backdrop_path')
    result['budget'] = film.get('budget')
    result['revenue'] = film.get('revenue')
    result['original_language'] = film.get('original_language')
    result['overview'] = film.get('overview')
    result['popularity'] = film.get('popularity')
    result['poster_path'] = film.get('poster_path')
    result['video'] = film.get('video')
    result['vote_count'] = film.get('vote_count')
    result['vote_average'] = film.get('vote_average')
    
    # Extração de Data e Ano
    release_date_str = film.get('release_date', '')
    if release_date_str:
        result['release_date'] = release_date_str
        try:
            result['year'] = int(release_date_str[:4])
        except ValueError:
            result['year'] = None
    else:
        result['release_date'] = None
        result['year'] = None

    # Extração de País
    production_countries = film.get('production_countries', [])
    result['country_iso_2'] = production_countries[0].get('iso_3166_1') if production_countries else None

    # --- 2. DADOS TÉCNICOS (DIRETOR) ---
    credits = film.get('credits', {})
    crew = credits.get('crew', [])
    cast = credits.get('cast', [])
    
    # Diretor
    directors = [m for m in crew if m.get('job') == 'Director']
    if directors:
        result['director_id'] = directors[0].get('id')
        result['director_name'] = directors[0].get('name')
    else:
        result['director_id'] = None
        result['director_name'] = None

    # --- 3. DADOS DE ELENCO (TOP 3) ---
    top_cast = cast[:3] 
    
    result['actor_1_id'] = top_cast[0]['id'] if len(top_cast) > 0 else None
    result['actor_1_name'] = top_cast[0]['name'] if len(top_cast) > 0 else None
    
    result['actor_2_id'] = top_cast[1]['id'] if len(top_cast) > 1 else None
    result['actor_2_name'] = top_cast[1]['name'] if len(top_cast) > 1 else None
    
    result['actor_3_id'] = top_cast[2]['id'] if len(top_cast) > 2 else None
    result['actor_3_name'] = top_cast[2]['name'] if len(top_cast) > 2 else None

    # --- 4. CONTEXTO (ESTÚDIOS, KEYWORDS, RATING) ---
    # Estúdios
    production_companies = film.get('production_companies', [])
    studios = [c['name'] for c in production_companies]
    result['studios'] = "|".join(studios) 

    # Keywords
    kw_data = film.get('keywords', {})
    if isinstance(kw_data, dict):
        keywords_list = kw_data.get('keywords', [])
    else:
        keywords_list = [] # Caso venha num formato inesperado
    result['keywords'] = "|".join([k['name'] for k in keywords_list])

    # MPAA Rating (US)
    release_dates = film.get('release_dates', {}).get('results', [])
    us_rating = None
    for country in release_dates:
        if country['iso_3166_1'] == 'US':
            for date in country['release_dates']:
                if date.get('certification'):
                    us_rating = date['certification']
                    break
        if us_rating: break
    result['mpaa_rating'] = us_rating

    # --- 5. PROCESSAMENTO DE GÉNEROS (MOVIDO PARA ANTES DO RETURN) ---
    genres = film.get('genres', []) 
    processed_genres = [] 
    
    for genre_dict in genres:
        processed_genre = {
            'film_id': film_id,
            'genre_id': genre_dict.get('id'),
            'genre_name': genre_dict.get('name') 
        }
        processed_genres.append(processed_genre)

    # --- 6. RETORNO FINAL ---
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
#%%

import wbgapi as wb
import pandas as pd

def fetch_and_process_econ_data_for_merge(min_year, max_year):
    """
    Fetches World Bank data in the correct format (Long) for merging.
    """
    # 1. Define the mapping (Indicator ID -> Your Column Name)
    indicators = {
        'NY.GDP.MKTP.KD.ZG': 'gdp_growth',
        'SL.UEM.TOTL.ZS': 'unemployment',
        'FP.CPI.TOTL.ZG': 'inflation',
        'NY.GDP.PCAP.KD': 'gdp_per_capita',
        'SI.POV.GINI': 'gini_index'
    }
    
    print(f"--- Fetching World Bank Data ({min_year}-{max_year}) ---")

    try:
        raw_df = wb.data.DataFrame(
            indicators.keys(), 
            time=range(min_year, max_year + 1), 
            skipAggs=True, 
            columns='series' # <--- THIS PREVENTS THE KEYERROR
        )
    except Exception as e:
        print(f"Error fetching WB data: {e}")
        return pd.DataFrame(columns=['country_iso_3', 'year'] + list(indicators.values()))

    # 3. Reset index to move 'economy' and 'time' from Index to Columns
    raw_df.reset_index(inplace=True)
    
    # 4. Clean and Rename Columns
    # wbgapi returns 'economy' (ISO3) and 'time' (e.g., 'YR2020')
    
    # Clean up Year (remove 'YR' and convert to int)
    # The 'time' column comes from wbgapi like 'YR1990', 'YR1991', etc.
    raw_df['year'] = raw_df['time'].str.replace('YR', '').astype(int)
    
    # Rename Economy to your standard column name
    raw_df.rename(columns={'economy': 'country_iso_3'}, inplace=True)
    
    # Rename the indicator codes to your readable names
    raw_df.rename(columns=indicators, inplace=True)
    
    # Drop the original 'time' string column as we now have 'year' (int)
    if 'time' in raw_df.columns:
        raw_df.drop(columns=['time'], inplace=True)
    
    print(f"Econ data fetched: {len(raw_df)} rows. Columns: {list(raw_df.columns)}")
    return raw_df

# %%
# Check total items
print(f"Total items in list: {len(processed_films)}")

# Check unique IDs
unique_ids = set(film['id'] for film in processed_films)
print(f"Unique IDs in list: {len(unique_ids)}")

# Check unique IMDB IDs (since you also have a unique constraint there)
unique_imdb = set(film['imdb_id'] for film in processed_films)
print(f"Unique IMDB IDs in list: {len(unique_imdb)}")
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
    
    -- Dados Básicos
    overview TEXT,
    original_language VARCHAR(10),
    release_date DATE,
    year INTEGER,              -- Novo: Ano extraído
    runtime INTEGER,           -- Novo: Duração (útil se tiveres)
    
    -- Métricas
    budget BIGINT,
    revenue BIGINT,
    popularity DOUBLE PRECISION,
    vote_count INTEGER,
    vote_average DOUBLE PRECISION,
    
    -- Equipa Técnica e Elenco (Ouro para o Modelo)
    director_id BIGINT,
    director_name TEXT,
    
    actor_1_id BIGINT,
    actor_1_name TEXT,
    actor_2_id BIGINT,
    actor_2_name TEXT,
    actor_3_id BIGINT,
    actor_3_name TEXT,
    
    -- Contexto de Indústria
    studios TEXT,              -- Ex: "A24|Plan B"
    keywords TEXT,             -- Ex: "biography|politics"
    mpaa_rating TEXT,          -- Ex: "R", "PG-13"
    country_iso_2 VARCHAR(5),
    
    -- Assets
    poster_path TEXT,
    backdrop_path TEXT,
    video BOOLEAN,
    adult BOOLEAN,
    
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
    cur.execute("DROP TABLE IF EXISTS tmdb_films_silver CASCADE;")

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

#%%

# Substitua a função 'sanitize_and_correct_dataframe' antiga por esta:

def sanitize_and_correct_dataframe(df_gold, awards_path):
    print("\n--- 🧹 Iniciando Saneamento Avançado (Correção de Anos) ---")
    
    # 1. REMOVER DUPLICATAS DE ID
    if 'imdb_id' in df_gold.columns:
        df_gold = df_gold.drop_duplicates(subset=['imdb_id'], keep='first')
    else:
        df_gold = df_gold.drop_duplicates(subset=['id'], keep='first')

    # 2. CARREGAR GABARITO
    try:
        df_awards = pd.read_csv(awards_path, sep=';')
        
        # Normaliza colunas do gabarito
        df_awards['imdb_id'] = df_awards['imdb_id'].str.strip()
        df_awards['award_year'] = pd.to_numeric(df_awards['award_year'], errors='coerce').fillna(0).astype(int)
        
        # Filtra apenas Oscar Best Picture
        oscars_ref = df_awards[
            (df_awards['award_source'].str.strip().str.lower() == 'oscars') &
            (df_awards['award_category'].str.strip().str.lower() == 'best picture')
        ].copy()
        
   
        # Isso é o pulo do gato: vamos corrigir o ano do filme para bater com a cerimônia!
        status_map = dict(zip(oscars_ref['imdb_id'], oscars_ref['status'].str.lower()))
        year_map = dict(zip(oscars_ref['imdb_id'], oscars_ref['award_year']))
        
        print(f"Gabarito carregado: {len(oscars_ref)} registros.")

        # 3. CORREÇÃO DOS DADOS NO GOLD
        df_gold['oscar_nominated'] = 0
        df_gold['oscar_winner'] = 0
        
        # Limpeza de ID
        if df_gold['imdb_id'].dtype == object:
            df_gold['imdb_id'] = df_gold['imdb_id'].str.strip()


        count_year_fix = 0
        for idx, row in df_gold.iterrows():
            imdb = row['imdb_id']
            
            if imdb in year_map:
                ceremony_year = year_map[imdb]
                current_year = row['year']
                
                # Se a diferença for pequena (ex: filme 2008, cerimônia 2009), atualizamos
                # Se for muito grande, pode ser erro de ID, então ignoramos
                if abs(ceremony_year - current_year) <= 2:
                    if current_year != ceremony_year:
                        df_gold.at[idx, 'year'] = ceremony_year
                        count_year_fix += 1
                
            # Atualiza status (Winner/Nominated)
            if imdb in status_map:
                df_gold.at[idx, 'oscar_nominated'] = 1
                if status_map[imdb] == 'winner':
                    df_gold.at[idx, 'oscar_winner'] = 1

        print(f"✅ Anos corrigidos (alinhados à cerimônia): {count_year_fix}")
        print(f"✅ Vencedores marcados: {df_gold['oscar_winner'].sum()}")

        # 4. AUDITORIA FINAL DE DUPLICIDADE
        winners = df_gold[df_gold['oscar_winner'] == 1]
        dup_years = winners['year'].value_counts()
        dup_years = dup_years[dup_years > 1]
        
        if not dup_years.empty:
            print("\n🚨 AINDA EXISTEM ANOS COM MÚLTIPLOS VENCEDORES (Verifique IDs duplicados):")
            print(dup_years)
            for y in dup_years.index:
                print(f"Ano {y}:")
                print(winners[winners['year'] == y][['title', 'imdb_id']])
        else:
            print("\n🎉 Sucesso: Apenas 1 vencedor por ano!")

    except Exception as e:
        print(f"❌ Erro crítico na correção: {e}")
        
    return df_gold

#%%
def add_context_features(df):
    """
    Cria features baseadas em texto (Keywords e Studios)
    """
    print("--- 🏭 Criando features de Contexto (Studios & Keywords) ---")
    
    # 1. Top Studios
    # Lista de estúdios "Oscar-friendly" ou Big Majors
    prestige_studios = ["A24", "Searchlight", "Warner Bros", "Universal", "Paramount", "Sony", "Disney", "Netflix", "Amazon"]
    
    def check_studio(studio_str):
        if pd.isna(studio_str): return 0
        studio_str = str(studio_str).lower()
        for ps in prestige_studios:
            if ps.lower() in studio_str:
                return 1
        return 0
    
    df['is_top_studio'] = df['studios'].apply(check_studio)

    # 2. Is Biopic / True Story
    def check_biopic(kw_str):
        if pd.isna(kw_str): return 0
        kw_str = str(kw_str).lower()
        if "biography" in kw_str or "based on true story" in kw_str or "based on a true story" in kw_str or "historical" in kw_str:
            return 1
        return 0

    df['is_biopic'] = df['keywords'].apply(check_biopic)
    
    return df
def calculate_director_stats(df_gold, df_awards):
    """
    Calcula histórico do realizador (ID > Nome)
    """
    print("--- 🎬 Calculando estatísticas do Diretor ---")
    
    # Filtra Oscars relevantes
    relevant_awards = df_awards[
        (df_awards['award_source'].str.lower() == 'oscars') & 
        (df_awards['award_category'].isin(['Best Director', 'Best Picture']))
    ].copy()
    
    # Mapa de Referência (Filme -> Diretor)
    director_ref = df_gold[['imdb_id', 'director_id', 'director_name', 'year']].drop_duplicates(subset=['imdb_id'])
    
    # Join
    awards_merged = pd.merge(relevant_awards, director_ref, on='imdb_id', how='left')
    awards_merged['award_year'] = pd.to_numeric(awards_merged['award_year'], errors='coerce').fillna(0)

    # Chave Única de Diretor
    def get_key(row):
        if pd.notna(row.get('director_id')) and row.get('director_id') != 0:
            return str(int(row['director_id']))
        if pd.notna(row.get('director_name')):
            return str(row['director_name']).strip().lower()
        return None

    awards_merged['director_key'] = awards_merged.apply(get_key, axis=1)
    df_gold['director_key'] = df_gold.apply(get_key, axis=1)
    
    # Build History
    history = {}
    for _, row in awards_merged.dropna(subset=['director_key']).iterrows():
        k = row['director_key']
        y = row['award_year']
        win = (str(row['status']).lower() == 'winner')
        if k not in history: history[k] = []
        history[k].append({'year': y, 'win': win})

    # Apply to Gold
    wins, noms = [], []
    for _, row in df_gold.iterrows():
        k = row['director_key']
        curr_y = row['year']
        if k in history:
            past = [r for r in history[k] if r['year'] < curr_y]
            wins.append(sum(1 for r in past if r['win']))
            noms.append(len(past))
        else:
            wins.append(0); noms.append(0)
            
    df_gold['director_prev_wins'] = wins
    df_gold['director_prev_nominations'] = noms
    return df_gold.drop(columns=['director_key'])

def calculate_cast_stats(df_gold, df_awards):
    """
    Calcula o prestígio do elenco cruzando Actor IDs com histórico de vitórias.
    Heurística: Best Actor/Actress -> Actor 1. Supporting -> Actor 2/3.
    """
    print("--- 🎭 Calculando estatísticas do Elenco (Cast Prestige) ---")
    
    # 1. Filtra Prémios de Atuação
    acting_awards = df_awards[
        (df_awards['award_source'].str.lower() == 'oscars') & 
        (df_awards['award_category'].str.contains('Actor|Actress|Supporting', case=False, na=False))
    ].copy()
    
    # 2. Mapa de Referência (Filme -> Atores)
    actor_ref = df_gold[[
        'imdb_id', 
        'actor_1_id', 'actor_1_name',
        'actor_2_id', 'actor_2_name',
        'actor_3_id', 'actor_3_name'
    ]].drop_duplicates(subset=['imdb_id'])
    
    awards_merged = pd.merge(acting_awards, actor_ref, on='imdb_id', how='left')
    awards_merged['award_year'] = pd.to_numeric(awards_merged['award_year'], errors='coerce').fillna(0)
    
    # 3. Construir Histórico de Vitórias por ATOR
    # Dicionário: Actor_Key -> List of Wins/Noms
    actor_history = {} # Key: str(actor_id)
    
    def register_stat(actor_id, year, win):
        if pd.isna(actor_id) or actor_id == 0: return
        k = str(int(actor_id))
        if k not in actor_history: actor_history[k] = []
        actor_history[k].append({'year': year, 'win': win})

    for _, row in awards_merged.iterrows():
        cat = str(row['award_category']).lower()
        year = row['award_year']
        win = (str(row['status']).lower() == 'winner')
        
        # HEURÍSTICA DE ATRIBUIÇÃO
        # Se for "Lead", damos crédito ao Actor 1
        if 'supporting' not in cat: 
            register_stat(row['actor_1_id'], year, win)
        else:
            # Se for "Supporting", damos crédito ao Actor 2 e 3 (aproximação)
            register_stat(row['actor_2_id'], year, win)
            register_stat(row['actor_3_id'], year, win)

    # 4. Calcular Score Cumulativo para o Filme Atual
    # Cast Prestige = Soma das vitórias passadas dos 3 atores principais
    cast_prestige_list = []
    
    for _, row in df_gold.iterrows():
        curr_year = row['year']
        total_wins = 0
        
        for i in [1, 2, 3]:
            a_id = row.get(f'actor_{i}_id')
            if pd.notna(a_id) and a_id != 0:
                k = str(int(a_id))
                if k in actor_history:
                    # Conta apenas vitórias passadas
                    past_wins = sum(1 for r in actor_history[k] if r['year'] < curr_year and r['win'])
                    total_wins += past_wins
        
        cast_prestige_list.append(total_wins)
        
    df_gold['cast_prestige'] = cast_prestige_list
    return df_gold
# %%
from sqlalchemy import create_engine
import pandas as pd
import pycountry

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

        # Extract Year
        df_films['release_date'] = pd.to_datetime(df_films['release_date'], errors='coerce')
        df_films['year'] = df_films['release_date'].dt.year.fillna(0).astype(int)

        # Convert ISO-2 (TMDB) to ISO-3 (World Bank)
        def get_iso3(iso2):
            if not iso2: return None
            try:
                return pycountry.countries.get(alpha_2=iso2).alpha_3
            except:
                return None

        # Ensure 'country_iso_2' exists (it should if you updated the SQL table)
        if 'country_iso_2' in df_films.columns:
            df_films['country_iso_3'] = df_films['country_iso_2'].apply(get_iso3)
        else:
            print("WARNING: 'country_iso_2' column missing. Economic data merge will fail.")
            df_films['country_iso_3'] = None

            min_year = df_films['year'].min()
        max_year = df_films['year'].max()
        
        # Safety check
        if min_year < 1960: min_year = 1960 
        if max_year > 2024: max_year = 2024
        
        # Call your helper function here
        df_econ = fetch_and_process_econ_data_for_merge(int(min_year), int(max_year))

        
        
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

    df_gold = pd.merge(
        df_gold,
        df_econ,
        left_on=['country_iso_3', 'year'],
        right_on=['country_iso_3', 'year'],
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
    # A. Contexto (Studios & Biopic)
    df_gold = add_context_features(df_gold)
    
    # B. Diretor Stats
    df_gold = calculate_director_stats(df_gold, df_winners)
    
    # C. Cast Prestige (Novo!)
    df_gold = calculate_cast_stats(df_gold, df_winners)
    # 7. Saneamento Final (Target Class)
    df_gold['oscar_nominated'] = 0
    df_gold['oscar_winner'] = 0
    df_gold = sanitize_and_correct_dataframe(df_gold, winners_csv_path)
    df_gold = calculate_director_stats(df_gold, df_winners)
    #Salvar o dataset GOLD como CSV
    gold_dir = script_dir / "data" / "gold"
    os.makedirs(gold_dir, exist_ok=True)
    output_path = gold_dir / "oscar_ml_dataset_final.csv"

    
    
    static_cols = [
            'id', 'imdb_id', 'title', 'year', 'oscar_winner', 'oscar_nominated',
            'director_name', 'director_prev_wins', 'director_prev_nominations',
            'cast_prestige', 'is_top_studio', 'is_biopic',
            'budget', 'revenue', 'popularity', 'vote_average', 'vote_count',
            'mpaa_rating', 'runtime'
        ]
        
        # --- CORREÇÃO DO ERRO ---
        # Forçamos list() para garantir que é concatenação e não soma vetorial
    cols_order = static_cols + list(genre_cols)
        
    # Mantém outras colunas que existam mas não estão na lista
    existing_cols = [c for c in df_gold.columns if c not in cols_order]
    final_cols = cols_order + existing_cols
    
    df_gold[final_cols].to_csv(output_path, index=False, sep=';')
    print(f"🎉 Dataset Gold gerado com sucesso: {output_path}")
    print(df_gold[['title', 'cast_prestige', 'is_top_studio', 'is_biopic']].head())

if __name__ == "__main__":
    create_gold_dataset()

    # %%
