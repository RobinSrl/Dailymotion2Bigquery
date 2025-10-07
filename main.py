import logging, os, time, asyncio, datetime,  pandas as pd
from functools import partial
from typing import Any
from dailymotion import Authentication, DailymotionClient, recursive_search_key
from bigquery_transfer import transfer, get_rows
from slack_chat import notify, notify_on_exception
from zoneinfo import ZoneInfo

logging.basicConfig(
    level=logging.DEBUG if bool(os.getenv("DEBUG", False)) else logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s" +
           (" [\"%(pathname)s\" line %(lineno)d]" if os.getenv("DEBUG", False) else ""),
    datefmt="%Y-%m-%d %H:%M:%S"
)


MAX_CONCURRENCY = 10


class DailyMotionDataHandle(object):
    """A comprehensive data handler for DailyMotion analytics and content information.

     This class provides a high-level interface for fetching, processing, and enriching
     DailyMotion data by combining GraphQL report generation with REST API calls to
     retrieve detailed metadata. It handles the complete data pipeline from report
     generation to final enriched dataset creation.

     """

    def __init__(self, client: DailymotionClient, logger:logging.Logger = None):
        """Initialize the DailyMotion data handler with client and logging configuration.

        Sets up the handler with an authenticated API client and configures logging
        for operation tracking. Initializes internal data storage as an empty DataFrame.

        Args:
            client (DailyMotionClient): Authenticated DailyMotion API client with
                                            valid OAuth tokens and required permissions
            logger (logging.Logger, optional): Logger instance for tracking operations,
                                             debugging, and error reporting. Uses module
                                             logger if not specified.
        """
        self.__client = client
        self.__logger = logger if logger is not None else logging.getLogger(f"{__name__}.{__class__.__name__}")
        self.__data = pd.DataFrame()

    @property
    def data(self) -> pd.DataFrame:
        """Get a read-only copy of the processed and enriched dataset.

       Returns a copy of the internal DataFrame to prevent external modifications
       while allowing access to the processed data for analysis and export.

       Returns:
           pd.DataFrame: Copy of the enriched dataset containing analytics data
                        merged with video, playlist, and player metadata. Includes
                        processed timestamps and cleaned data.

       Note:
           This property returns a copy to maintain data integrity. For large
           datasets, consider the memory implications of copying the DataFrame.
       """
        return self.__data.copy()

    def fetch(self, *, metrics:list[str], dimension:list[str], start_date:datetime.date, end_date:datetime.date, product:str = None) -> None:
        """Initialize and process the complete DailyMotion data pipeline.

        Orchestrates the full workflow of report generation, data extraction,
        metadata enrichment, and data refinement. This method coordinates all
        the private methods to produce a final enriched dataset.
        """
        init_query, init_variables = self._prepare_query(metrics=metrics, dimension=dimension, start_date=start_date, end_date=end_date, product=product)
        self.__fetch_main_data_form_graphql(init_query, init_variables)
        #TODO: Crea un unica query per prendere i report sia di earnings sia di views
        self.cluster_data_by_day(init_variables)

        self.__logger.info("Fetch details from REST API for video, playlist, player IDs")
        df_info_from_id={
            'video': self.async_fetch_rest_details_by_id('video', self.data['video_id'].dropna().unique(), ['id', 'title', 'description', 'duration', 'created_time', 'tags', 'url']),
            'playlist': self.async_fetch_rest_details_by_id('playlist', self.data['playlist_id'].dropna().unique(), ['id', 'name']),
            'player': self.async_fetch_rest_details_by_id('player', self.data['player_id'].dropna().unique(), ['id', 'label'])
        }
        self.__logger.info(self.__data.columns.tolist())
        # Iterate over each unique, non-null playlist ID
        # LEFT JOIN (sql) to merge the dataframe
        merged_df = self.data
        merged_df = self._get_earnings(df_to_merge=merged_df)

        if df_info_from_id['video'] is not None and not df_info_from_id['video'].empty and 'video_id' in merged_df.columns.tolist():
            merged_df = merged_df.merge(df_info_from_id['video'], on='video_id', how='left')

        if df_info_from_id['playlist'] is not None and not df_info_from_id['playlist'].empty and 'playlist_id' in merged_df.columns.tolist():
            merged_df = merged_df.merge(df_info_from_id['playlist'], on='playlist_id', how='left')
        else:
            merged_df['playlist_name'] = None  # if there is no playlist_id in the cluster_df, fill playlist_name with NaN

        if df_info_from_id['player'] is not None and not df_info_from_id['player'].empty and 'player_id' in merged_df.columns.tolist():
            merged_df = merged_df.merge(df_info_from_id['player'], on='player_id', how='left')
        else:
            merged_df['player_label'] = None  # if there is no player_id in the cluster_df, fill player_label with NaN

        self.__data = self.__refining(merged_df)

    @notify_on_exception
    def __fetch_main_data_form_graphql(self, query: str, variables: dict[str, Any]) -> None:
        """
        Executes a GraphQL report mutation via the Dailymotion API, retrieves CSV report download links,
        downloads the CSV(s), and loads the content into a single pandas DataFrame.

        Args:
            query (str): The GraphQL mutation string used to generate the report.
            variables (dict[str, Any]): A dictionary of GraphQL input parameters (e.g. filters, dates, dimensions).

        Returns:
            pd.DataFrame: A concatenated DataFrame containing all rows from the downloaded report(s).

        Raises:
            DailymotionClientException: If authentication, report generation, or CSV download fails.
        """

        # Execute the GraphQL report mutation and get the list of CSV report download links
        _start = time.time()
        report_links = self.__client.get_report_file(query=query, variable=variables)
        self.__logger.info(f"report links: {report_links}")

        # If multiple tokens are returned, each link corresponds to a token.
        # You may enhance logic here to handle tokens + variable mapping if needed

        dataframes = []

        """ WARNING:
        1st Massive blocking time
        """
        # Download and read each CSV file into a separate DataFrame
        for link in report_links:
            self.__logger.info(f"Reading CSV from {link}")
            dataframes.append(pd.read_csv(link, dtype={6: str}))

        if not dataframes:
            raise ValueError(f"No dataframes created")

        notify(f"Dailymotion ha generato il report in {int((time.time() - _start))} secondi")
        self.__data = pd.concat(dataframes, ignore_index=True)

    @notify_on_exception
    def __fetch_details_from_rest(self, name: str, ids: list[str], fields: list[str]) -> pd.DataFrame:
        """
        Retrieve resource details from the Dailymotion REST API for a list of IDs and return as a single DataFrame.

        Args:
            name (str): The resource type to fetch (e.g. `"video"`, `"playlist"`, `"player"`).
            ids (list[str]): A list of resource IDs to retrieve.
            fields (list[str]): A list of field names to request from the API for each resource.

        Returns:
            pd.DataFrame: A DataFrame containing one row per successfully fetched item,
                          with columns prefixed by `name.lower()`.
        """
        dataframe_fetched_by_ids = pd.DataFrame()

        """ WARNING:
        2nd Massive blocking time
        """
        for item_id in ids:  # every id (unique)
            try:
                # Fetch details via REST API
                rest_response = self.__client.rest('/%s/%s' % (name, item_id), fields=fields)

                # Convert single record dict to a one‑row DataFrame and append to existing DataFrame
                dataframe_fetched_by_ids = pd.concat(
                    [dataframe_fetched_by_ids, pd.DataFrame([rest_response])],
                    ignore_index=True
                )
            except Exception as e:
                self.__logger.warning("Warning on %s with id %s:\n\t%s" % (name, item_id, e))
                continue
        return dataframe_fetched_by_ids.add_prefix("%s_" % name.lower())

    @notify_on_exception
    def async_fetch_rest_details_by_id(self, name: str, ids: list[str], fields: list[str]) -> pd.DataFrame:
        async def async_wrapper():
            # Crea un semaforo per limitare la concorrenza
            semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

            async def fetch_single_item(item_id: str) -> dict | None:
                """Fetch details per un singolo item con controllo della concorrenza"""
                async with semaphore:
                    try:
                        # Esegue la chiamata sincrona in un thread separato
                        loop = asyncio.get_event_loop()
                        rest_response = await loop.run_in_executor(
                            None,  # Usa il default ThreadPoolExecutor
                            partial(
                                self.__client.rest,
                                f"{name.strip().strip('/').lower()}/{item_id.strip('/')}",
                                fields=fields
                            )
                        )
                        return rest_response
                    except Exception as e:
                        logging.warning(f"Warning on {name} with id {item_id}:\n\t{e}")
                        return None

            # Crea tutte le task per le richieste
            tasks = [fetch_single_item(item_id) for item_id in ids]

            # Esegue tutte le richieste in parallelo
            results = await asyncio.gather(*tasks, return_exceptions=False)

            # Filtra i risultati validi (non None) e crea il DataFrame
            valid_results = [result for result in results if result is not None]

            if valid_results:
                dataframe_fetched_by_ids = pd.DataFrame(valid_results)
            else:
                dataframe_fetched_by_ids = pd.DataFrame()

            return dataframe_fetched_by_ids.add_prefix("%s_" % name.lower())

        # Esegue il wrapper asincrono e restituisce il risultato
        return asyncio.run(async_wrapper())

    @notify_on_exception
    def __refining(self, df:pd.DataFrame) -> pd.DataFrame:
        if 'video_created_time' in df.columns:
            df["video_created_time"] = pd.to_datetime(df["video_created_time"], unit="s", utc=True)

        if 'video_media_type' in df.columns:
            df.rename(columns={'video_media_type': 'media_type'}, inplace=True)

        # if 'view_through_rate' in df.columns:
        #     # convert view_through_rate from percentage to decimal
        #     df['view_through_rate'] = df['view_through_rate'] / 100

        if not 'estimated_earnings_eur' in df.columns:
            df['estimated_earnings_eur'] = '0'
        else:
            df['estimated_earnings_eur'] = df['estimated_earnings_eur'].fillna(0).astype(str)

        df['video_duration'] = df['video_duration'].fillna(0).astype(int)

        df['day'] = pd.to_datetime(df['day']).dt.strftime('%Y-%m-%d')
        df["video_created_time"] = pd.to_datetime(df["video_created_time"]).dt.strftime("%Y-%m-%dT%H:%M:%S")

        return df.sort_values(by=['day', 'video_id'])

    def __safe_data_clustering(self, group_by: list[str] | str, aggregation: dict[str, str] = None) -> pd.DataFrame:

        # Create copy and fill NaN with -1
        df_copy = self.__data.fillna(-1)

        # Group data and aggregate
        df_grouped = (df_copy
                      .groupby(group_by)
                      .agg(aggregation if aggregation is not None else {})
                      .reset_index())

        return df_grouped.replace(-1, None)

    def cluster_data_by_day(self, variables: dict[str, Any] = None):
        if 'hour' in self.__data.columns:
            # convert datetime in UTC into Date with timezone Europe/Rome
            self.__data['hour'] = pd.to_datetime(self.__data['hour'], utc=True, errors='coerce').dt.tz_convert(
                'Europe/Rome').dt.date

        dimension = recursive_search_key(variables, 'dimensions')
        dimension = list(dict.fromkeys([item.lower() for sublist in dimension for item in sublist]))
        dimension = [dim for dim in dimension if dim in self.__data.columns]

        # grouping data by day
        self.__data = self.__safe_data_clustering(dimension, {'views': 'sum', 'time_watched_seconds': 'sum',
                                                              'view_through_rate': 'mean'})

        if 'hour' in self.__data.columns:
            self.__data.rename(columns={'hour': 'day'}, inplace=True)

    @staticmethod
    def _prepare_query(*, metrics:list[str], dimension:list[str], start_date:datetime.date, end_date:datetime.date, product:str = None):

        query = '''mutation MultiReport($item: AskPartnerReportFileInput!) {
              report1: askPartnerReportFile(input: $item) {
                reportFile { reportToken }
              }
             }'''

        if isinstance(metrics, str):
            metrics = [metrics]
        elif metrics is None:
            metrics = []

        if isinstance(dimension, str):
            dimension = [dimension]
        elif dimension is None:
            dimension = []

        variables = {
            "item": {
                "metrics": [met.upper() for met in metrics],
                "dimensions": [dim.upper() for dim in dimension],
                "startDate": start_date.strftime('%Y-%m-%d'),
                "endDate": end_date.strftime('%Y-%m-%d'),
                "product": product.upper() if product is not None else 'ALL'
            }
        }
        return query, variables

    def _get_earnings(self,*, df_to_merge:pd.DataFrame=None):
        dimension = ['day', 'video_id', "visitor_page_url", "visitor_device_type"]
        query, variable = self._prepare_query(metrics=['ESTIMATED_EARNINGS_EUR'],
                            dimension=dimension,
                            start_date=(datetime.date.today() - datetime.timedelta(days=2)),
                            end_date=datetime.date.today(),
                            product="ALL")
        _start = time.time()
        report_links = self.__client.get_report_file(query=query, variable=variable)
        self.__logger.info(f"report links: {report_links}")

        dataframes = []

        for link in report_links:
            self.__logger.info(f"Reading CSV from {link}")
            dataframes.append(pd.read_csv(link, dtype={6: str}))

        if not dataframes:
            raise ValueError(f"No dataframes created")

        notify(f"Dailymotion ha generato il report per earnings in {int((time.time() - _start))} secondi")

        df = pd.concat(dataframes, ignore_index=True)
        df['day'] = pd.to_datetime(df['day'], utc=True, errors='coerce').dt.tz_convert(
            'Europe/Rome').dt.date
        if df_to_merge is None:
            return df

        return df_to_merge.merge(df, on=dimension, how='left')


start_time = time.time()
if __name__ == "__main__":
    date_format = "%d/%m/%Y %H:%M:%S %z"
    notify(f"[{datetime.datetime.now(ZoneInfo('Europe/Rome')).strftime(date_format)}]  Start script", text_level="debug")

    yesterday_date = datetime.date.today() - datetime.timedelta(days=1)

    auth = Authentication.from_credential(
        os.getenv("DM_CLIENT_API"),
        os.getenv("DM_CLIENT_SECRET"),
        scope=['create_reports', 'delete_reports', 'manage_reports']
    )

    data_handler = DailyMotionDataHandle(DailymotionClient(auth))
    data_handler.fetch(metrics=['VIEWS', 'TIME_WATCHED_SECONDS', 'VIEW_THROUGH_RATE'],
                       dimension=["HOUR","VIDEO_ID",  "MEDIA_TYPE","VISITOR_PAGE_URL","VISITOR_DEVICE_TYPE","PLAYER_ID","PLAYLIST_ID"],
                       start_date=(yesterday_date - datetime.timedelta(days=1)),
                       end_date= yesterday_date,
    )
    df = data_handler.data.reset_index(drop=True)
    df = df[df['day'] == yesterday_date.strftime('%Y-%m-%d')]

    notify_on_exception(transfer)(df)

    notify(f"_{len(df)} records_ sono stati trasferiti su "
           f"<https://console.cloud.google.com/bigquery?project=smart-data-platform-dev-401609&ws=!1m9!1m3!3m2!1ssmart-data-platform-dev-401609!2scustom!1m4!4m3!1ssmart-data-platform-dev-401609!2srobin_custom!3sdailymotion_default_data&inv=1&invt=AbkQ8g|BigQuery>"
           f" in {int(round((time.time() - start_time) / 60))} minuti"
           f"\n\n Guarda il report "
           f"<https://lookerstudio.google.com/reporting/36146039-563a-4f18-8ebd-f32f62f5d2d7/page/AOWBF|Dati Video>")

    logging.info("Executed in %d seconds" % (time.time() - start_time) )
