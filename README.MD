# DailyMotion to Data Platform

## Descrizione del Progetto

Servizio per l'integrazione dei dati tra l'API DailyMotion e BigQuery. Il progetto si occupa dell'estrazione automatizzata dei dati analitici dalla piattaforma DailyMotion e il loro caricamento su BigQuery.

## DailyMotion API Integration

### Funzionamento

#### 1. Autenticazione OAuth

L'autenticazione viene effettuata utilizzando le credenziali OAuth `client_id` e `client_secret` attraverso il flusso **Client Credentials Grant**.

```python
# Inizializzazione dell'autenticazione con credenziali
auth = Authentication.from_credential(
    os.environ.get("DM_CLIENT_API"),
    os.environ.get("DM_CLIENT_SECRET"),
    scope=['create_reports', 'delete_reports', 'manage_reports']
)

# Ottenimento del token
token = auth.get_token()
```

#### 2. Generazione Report

Viene creata un'istanza della classe `DailyMotion` utilizzando l'oggetto di autenticazione. Attraverso il metodo `get_report_file()` viene eseguita una query GraphQL all'endpoint `https://partner.api.dailymotion.com/graphql` per generare e ottenere gli URL di download dei report in formato **.csv**.

```python
# Inizializzazione del client
client = DailyMotion(authentication=auth)

# Query GraphQL per generare report
query = '''mutation ($input: AskPartnerReportFileInput!) {
  askPartnerReportFile(input: $input) {
    reportFile {
      reportToken
    }
  }
}'''

variables = {
    "input": {
        "metrics": ["VIEWS", "TIME_WATCHED_SECONDS"],
        "dimensions": ["HOUR", "VIDEO_ID"],
        "startDate": "2025-06-27",
        "endDate": "2025-07-27",
        "product": "CONTENT"
    }
}

# Generazione e polling dei report
report_links = client.get_report_file(
    query=query, 
    variable=variables,
    max_retry=10,
    delay=30
)
```

#### 3. Elaborazione Dati

I dati vengono elaborati utilizzando la libreria `pandas`: ogni report CSV viene scaricato, letto e combinato in un unico `DataFrame` per l'elaborazione successiva.

```python
import pandas as pd

dataframes = []
for link in report_links:
    logging.info(f"Processing CSV from: {link}")
    df = pd.read_csv(link, parse_dates=['hour'])
    dataframes.append(df)

# Combinazione di tutti i DataFrame
final_df = pd.concat(dataframes, ignore_index=True)
```

## Architettura Tecnica

L'applicazione è strutturata nei seguenti moduli principali:

### Authentication
Gestisce l'autenticazione OAuth2 con le API DailyMotion. La classe supporta due modalità di autenticazione:
- **Client Credentials Flow**: Per applicazioni server-to-server
- **Password Flow**: Per applicazioni che richiedono credenziali utente

Funzionalità principali:
- `from_credential()`: Factory method per autenticazione client credentials
- `from_password()`: Factory method per autenticazione password-based
- `get_token()`: Ottiene un token valido, gestendo automaticamente refresh e persistenza

### Token
Gestisce il ciclo di vita dei token di accesso:
- **Persistenza**: Salvataggio automatico su file JSON
- **Validazione**: Controllo scadenza token
- **Refresh**: Rinnovo automatico token scaduti
- **Formato**: Gestione header di autorizzazione

### DailyMotion
Classe principale per l'interazione con le API DailyMotion:
- **GraphQL**: Esecuzione query GraphQL con gestione errori
- **REST API**: Supporto per endpoint REST
- **Report Generation**: Generazione asincrona report con polling automatico
- **Token Refresh**: Gestione automatica refresh token prima delle richieste

Metodi principali:
- `graph_ql()`: Esecuzione query GraphQL
- `rest()`: Chiamate REST API
- `get_report_file()`: Generazione completa report con polling

### Gestione Asincrona Report

Il sistema implementa un meccanismo di polling con **exponential backoff** per la gestione dei report asincroni:

1. **Generazione**: Invio query di generazione report
2. **Estrazione Token**: Parsing automatico dei report token dalla risposta
3. **Polling**: Controllo periodico stato report con delay crescente
4. **Download**: Estrazione link di download al completamento

```python
# Configurazione polling
report_links = client.get_report_file(
    query=report_query,
    variable=report_variables,
    max_retry=15,      # Massimo 15 tentativi
    delay=60           # Delay iniziale 60 secondi
)
```
### Classe di gestione dei dati  `DailyMotionDataHandle()`
Classe finale per la gestione dei dati tramite client API DailyMotion:
- **GraphQL**: Esecuzione query GraphQL con gestione errori
- **REST API**: Supporto per endpoint REST
- **Report Generation**: Generazione asincrona report con polling automatico
- **Token Refresh**: Gestione automatica refresh token prima delle richieste
- 
Attributi Pricipali
- `data`: L'attributo data espone esternamente una copia dei dati analizzati

Metodi principali:
- `init()`: Esecutore principale
  - Esegue le chiamate API per la ricezione dei dati
  - Imposta un Database con i dati ricevuti
  - Esegue il refinig dei dati
  - 
> [!IMPORTANT]
> **Il refining non gestisce l'eliminazione dei duplicati**

## Limitazioni e Considerazioni

> [!WARNING]
> **Limitazioni API DailyMotion:**
> - **Generazione Asincrona**: I report vengono generati in modo asincrono
> - **Formato**: I dati vengono estratti esclusivamente in formato CSV
> - **Fuso Orario**: Tutti i report sono generati nel fuso orario UTC
> - **Concorrenza**: Massimo **2 report** possono essere generati contemporaneamente
> - **Tempo Generazione**: Un report può richiedere fino a **2 ore** per essere completato
> - **Retention**: I report vengono automaticamente eliminati dopo **48 ore**

### Limitazioni Query
- **Intervallo Date**: Minimo 1 giorno, massimo **30 giorni** per richiesta
- **Dimensioni**: Massimo **8 dimensioni** per singolo report

## Implementazioni future
- **Richieste Asincrone** Implementare una logica di richieste asincorne per il download dei file.csv e delle REST <br> 
```python
import aiohttp
import asyncio

async def fetch_all(urls: list[str]) -> list[str]:
    async with aiohttp.ClientSession() as session:
        # prepare url task
        tasks = [session.get(url) for url in urls]
        # execute all request
        responses = await asyncio.gather(*tasks)
        # extract and return response
        return [await {resp.json()} for resp in responses]

urls = [...#list of urls]
asyncio.run(fetch_all(urls))
```
- **Caching** sulle richieste già effettuate.

```python
import functools

@functools.cache
@functools.lru_cache
...

```

## Variabili d'Ambiente Richieste

```dotenv
# Credenziali DailyMotion
DM_CLIENT_API=your_client_id
DM_CLIENT_SECRET=your_client_secret

# Endpoint API
DM_BASE_URL=https://partner.api.dailymotion.com
DM_AUTH_URL=${DM_BASE_URL}/oauth/v1/token
DM_GRAPH_URL=${DM_BASE_URL}/graphql
DM_REST_URL=${DM_BASE_URL}/rest
```

## Esempio Completo

```python
import os
from DailyMotion import Authentication, DailyMotion

# Configurazione autenticazione
auth = Authentication.from_credential(
    client_api=os.environ.get("DM_CLIENT_API"),
    client_secret=os.environ.get("DM_CLIENT_SECRET"),
    scope=['create_reports', 'manage_reports']
)

# Inizializzazione client
dm_client = DailyMotion(authentication=auth)

# Query per report analytics
query = '''mutation Report($video: AskPartnerReportFileInput!) {
  report1: askPartnerReportFile(input: $video) {
    reportFile { reportToken }
  }
 }'''
variables = {
    "video": {
        "metrics": [
            ...
        ],
        "dimensions": [
            ...
        ],
        "startDate": "2025-07-30",
        "endDate": "2025-07-30",
        "product": "CONTENT"
    }
}

data_handler = DailyMotionDataHandle(
    DailyMotion(
        Authentication.from_credential(
            os.environ.get("DM_CLIENT_API"),
            os.environ.get("DM_CLIENT_SECRET"),
            scope=['create_reports', 'delete_reports', 'manage_reports']
        )
    )
)
data_handler.fetch(query, variables)

print(data_handler.data.tail())

```
il `data_handler.data` è un DataFrame, e si può convertire in json
```python
data_handler.data.to_json(orient='records', lines=True) # -> list[dict[str,Any]
# convert dataframe into list of dict [{'key': value, ...}, ...]
```

> [!CAUTION]
> **L'esecuzione in media dura dai 10 ai 15 minuti per un giorno**
