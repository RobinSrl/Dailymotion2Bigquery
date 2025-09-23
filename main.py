import datetime
import logging, os, time
from typing import Any
import pandas as pd
from dailymotion import Authentication, DailymotionClient
from bigquery_transfer import transfer

logging.basicConfig(
    level=logging.DEBUG if bool(os.getenv("DEBUG", False)) else logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s" +
           (" [\"%(pathname)s\" line %(lineno)d]" if os.getenv("DEBUG", False) else ""),
    datefmt="%Y-%m-%d %H:%M:%S"
)

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

    def fetch(self, init_query:str, init_variables:dict[str, Any]) -> None:
        """Initialize and process the complete DailyMotion data pipeline.

        Orchestrates the full workflow of report generation, data extraction,
        metadata enrichment, and data refinement. This method coordinates all
        the private methods to produce a final enriched dataset.
        """

        self.__fetch_main_data_form_graphql(init_query, init_variables)

        self.__logger.info("Fetch details from REST API for video, playlist, player IDs")
        df_info_from_id={
            'video': self.__fetch_details_from_rest('video', self.data['video_id'].dropna().unique(), ['id', 'title', 'description', 'duration', 'created_time', 'tags', 'url']),
            'playlist': self.__fetch_details_from_rest('playlist', self.data['playlist_id'].dropna().unique(), ['id', 'name']),
            'player': self.__fetch_details_from_rest('player', self.data['player_id'].dropna().unique(), ['id', 'label'])
        }
        self.__logger.info(self.__data.columns.tolist())
        # Iterate over each unique, non-null playlist ID
        # LEFT JOIN (sql) to merge the dataframe
        merged_df = self.data

        if df_info_from_id['video'] is not None and not df_info_from_id['video'].empty and 'video_id' in merged_df.columns.tolist():
            merged_df = merged_df.merge(df_info_from_id['video'], on='video_id', how='left')

        if df_info_from_id['playlist'] is not None and not df_info_from_id['playlist'].empty and 'playlist_id' in merged_df.columns.tolist():
            merged_df = merged_df.merge(df_info_from_id['playlist'], on='playlist_id', how='left')

        if df_info_from_id['player'] is not None and not df_info_from_id['player'].empty and 'player_id' in merged_df.columns.tolist():
            merged_df = merged_df.merge(df_info_from_id['player'], on='player_id', how='left')

        self.__refining(merged_df)

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

        self.__data = pd.concat(dataframes, ignore_index=True)

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
        #TODO: Make this with async httpx
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

    def __refining(self, df:pd.DataFrame, **kwargs):

        df["video_created_time"] = (pd.to_datetime(df["video_created_time"], unit="s", utc=True)
                                    .dt.tz_convert('Europe/Rome')
                                    .dt.strftime("%Y-%m-%dT%H:%M:%S%z")
                                    )

        """
        TODO: Questa variabile deve esserci ma le chiamate devono essere fatte con una dimanesione di date DAY non HOUR
            QUindi bisogna efettuare una chiamata con granularità HOUR e una DAY per l'entrate
        """
        df["estimated_earings_eur"] = None

        self.__data = df


start_time = time.time()
if __name__ == "__main__":
    yesterday_date = datetime.date.today() - datetime.timedelta(days=1)

    query = '''mutation MultiReport($video: AskPartnerReportFileInput!) {
      report1: askPartnerReportFile(input: $video) {
        reportFile { reportToken }
      }
     }'''

    variables ={
        #TODO: inserire "ESTIMATED_EARNINGS_EUR" questa metrica deve essere fatta con un altra variabile perchè non
        # è supportata dalla segnetazione oraria ma solo da quella giornaliera o mensile
        "video": {
          "metrics": [
            "VIEWS",
            "TIME_WATCHED_SECONDS",
            "VIEW_THROUGH_RATE"
          ],
              "dimensions": [
                "DAY",
                "VIDEO_ID",
                "MEDIA_TYPE",
                "VISITOR_PAGE_URL", #estrarre subdomain in viste
                "VISITOR_DEVICE_TYPE",
                "PLAYER_ID",
                "PLAYLIST_ID"
              ],
              "startDate": yesterday_date.strftime('%Y-%m-%d'),
              "endDate": yesterday_date.strftime('%Y-%m-%d'),
              "product": "CONTENT"
        }
    }

    auth = Authentication.from_credential(
        os.getenv("DM_CLIENT_API"),
        os.getenv("DM_CLIENT_SECRET"),
        scope=['create_reports', 'delete_reports', 'manage_reports']
    )

    data_handler = DailyMotionDataHandle(DailymotionClient(auth))
    data_handler.fetch(query, variables)
    df = data_handler.data.reset_index(drop=True)
    transfer(df)
    logging.info("Executed in %d seconds" % (time.time() - start_time) )
