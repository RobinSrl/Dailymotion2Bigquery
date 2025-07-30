# DailyMotion to Data Platform

## Descrizione del Progetto

Servizio per l'integrazione dei dati tra l'API DailyMotion e BigQuery. Il progetto si occupa dell'estrazione
automatizzata dei dati analitici dalla piattaforma DailyMotion e il loro caricamento su BigQuery.

## DailyMotion 
### Funzionamento

1. **Autenticazione OAuth** <br>
   L'autenticazione viene effettuata utilizzando le credenziali OAuth `client_id` e `client_secret`
   ```python
   # Inizializzazione dell'autenticazione con credenziali
    token = Authentication.from_credential(
        os.environ.get("DM_CLIENT_API"),
        os.environ.get("DM_CLIENT_SECRET"),
        scope=['create_reports', 'delete_reports', 'manage_reports']
    ).get_token()
   ```

2. **Generazione Report** <br>
   Viene creata un'istanza della classe `DailyMotion` utilizzando il token di autenticazione. <br>
   Attraverso il metodo `get_reports()` viene eseguita una query GraphQL all'endpoint
   `GET https://partner.api.dailymotion.com/graphql`
   per ottenere gli URL di download dei report in formato **.csv**.
   ```python
   client = DailyMotion(token)
   
   # Esempio di query GraphQL per generare report
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
   report_nodes = client.get_reports(query=query, variables=variables)
   ```

3. **Elaborazione Dati** <br>
   I dati vengono elaborati utilizzando la libreria `pandas`: ogni report CSV viene letto e combinato in un unico
   `DataFrame`

    ```python
    for link in report_links:
        logging.debug(f"Reading CSV from {link}")
        df = pd.read_csv(link, parse_dates=['hour'])
        dataframes.append(df)
    ```

### Architettura Tecnica

L'applicazione è strutturata nei seguenti moduli principali:

- **Authentication**: Gestisce l'autenticazione OAuth con le API DailyMotion. La classe fornisce il metodo
  `from_credential()` che accetta le credenziali client e lo scope dei permessi richiesti. Effettua la chiamata
  all'endpoint OAuth per ottenere il token di accesso tramite `get_token()` e gestisce automaticamente il refresh del
  token alla scadenza (ricreandolo).

- **DailyMotion**: Classe core per l'interazione con le API DailyMotion. Gestisce l'invio di query GraphQL all'endpoint
  `/graphql` utilizzando il token di autenticazione.

### Note

> [!WARNING]
> nella documentazione di DailyMotion vengono specificati questi limiti:
> - I report vengono generati in modo asincrono
> - I dati vengono estratti e raccolti in un file CSV
> - I report CSV sono generati nel fuso orario UTC
> - È possibile generare un massimo di due report contemporaneamente
> - Un report può richiedere fino a due ore per essere generato
> - È richiesto un intervallo di date per ogni richiesta. Non può essere inferiore a 1 giorno o superiore a 180 giorni.
> - È possibile passare un massimo di otto dimensioni in un singolo report
> - I report vengono eliminati dopo 48 ore

