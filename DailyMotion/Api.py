import functools
from json import JSONDecodeError
import requests, json, os, time
from typing import Self, Any, Literal, Generator

## Client Exception/Error
class DailymotionClientException(Exception):
    """Base exception class for DailyMotion client errors.
    
    Attributes:
        _code (int): Numeric error code, defaults to -1
        message (str): Human readable error message
    """
    _code: int = -1

    def __init__(self, message=''):
        self.message = 'Error %s: %s' % (int(self._code), message or 'Client error')
        super(DailymotionClientException, self).__init__(self.message)

class DailymotionApiException(DailymotionClientException):
    """Exception raised for general API errors (code: 1000)"""
    _code: int = 1000

class DailymotionAuthException(DailymotionClientException):
    """Exception raised for authentication errors (code: 2000)"""
    _code: int = 2000

class DailymotionTokenExpired(DailymotionAuthException):
    """Exception raised when authentication token has expired (code: 2100)"""
    _code: int = 2100

class DailymotionReportException(DailymotionClientException):
    """Exception raised for report generation errors (code: 3000)"""
    _code: int = 3000


class Token(object):
    """Manages DailyMotion API authentication tokens and their persistence.
    
    This class handles token storage, loading, and validation for API authentication.
    Tokens are stored in JSON format and include access token, refresh token, expiry,
    scope and token type.

    Attributes:
        access_token (str): The current access token
        refresh_token (str): Token used to refresh expired access tokens
        expires_in (int): Token expiration timestamp
        scope (str): Authorization scopes granted to token
        token_type (str): Type of authentication token
        file_path (str): Path to token storage file
    """

    def __init__(self, access_token, refresh_token, expires_in, scope, token_type, file_path='dm_token.json'):
        """Initialize Token instance with given credentials"""
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.expires_in = expires_in
        self.scope = scope
        self.token_type = token_type
        self.__file_path = file_path

    def dump(self) -> None:
        """Save current token data to file in JSON format"""
        with open(self.__file_path, 'w') as f:
            json.dump({
                'access_token': self.access_token,
                'refresh_token': self.refresh_token,
                'expires_in': self.expires_in + int(time.time()),
                'scope': self.scope,
                'token_type': self.token_type
            }, f)

    @classmethod
    def load(cls, file_path='dm_token.json') -> Self:
        """Load token data from file and create new Token instance
        
        Args:
            file_path: Path to token storage file
            
        Returns:
            Token: New Token instance initialized with loaded data
        """
        with open(file_path, 'r') as f:
            data = json.load(f)
            return cls(
                data['access_token'],
                data['refresh_token'],
                data['expires_in'],
                data['scope'],
                data['token_type'],
                file_path
            )

    def is_expired(self) -> bool:
        """Check if the current token has expired
        
        Returns:
            bool: True if current time is past expiration, False otherwise
        """
        return int(time.time()) >= self.expires_in

    def get_authorization(self):
        """Get formatted authorization header value
        
        Returns:
            str: Authorization header in format '{token_type} {access_token}'
        """
        return '%s %s' % (self.token_type, self.access_token)

class Authentication(object):
    """Handles DailyMotion API authentication using different auth flows.
    
    This class supports both password-based and client credentials authentication.
    It manages token generation, refresh and storage.

    Attributes:
        client_api (str): API client ID
        client_secret (str): API client secret 
        grant_type (str): Authentication type ('password' or 'client_credentials')
        scope (list[str]): List of authorization scopes
        username (str, optional): Username for password auth
        password (str, optional): Password for password auth
    """

    def __init__(self, client_api: str, client_secret: str, *, grant_type: Literal["client_credentials", "password"],
                 scope: list[str], **kwargs):
        """Initialize Authentication with API credentials and auth type"""
        if client_api is None or client_secret is None:
            raise DailymotionClientException(
                'client_api and client_secret are required\npassed: {"client_api": %s, "client_secret": %s}' % (
                    client_api, client_secret))

        if grant_type == "password":
            if kwargs.get('username', None) is None or kwargs.get('password', None) is None:
                raise DailymotionAuthException(
                    'username and password are required for password grant type\npassed: {"username": %s, "password": %s}' % (
                        kwargs.get('username', None), kwargs.get('password', None)))

        self.client_api = client_api
        self.__client_secret = client_secret
        self.grant_type = grant_type
        self.scope = scope

        self.username = kwargs.get('username', None)
        self.password = kwargs.get('password', None)

    @classmethod
    def from_password(cls, client_api: str, client_secret: str, *, username: str, password: str, scope: list[str]):
        """Create instance configured for password authentication"""
        return cls(client_api, client_secret, grant_type='password', scope=scope, username=username, password=password)

    @classmethod
    def from_credential(cls, client_api: str, client_secret: str, scope: list[str]):
        """Create instance configured for client credentials authentication"""
        return cls(client_api, client_secret, grant_type='client_credentials', scope=scope)

    def __generate_token(self) -> Token:
        """Generate new access token via authentication request
        
        Returns:
            Token: New token instance
            
        Raises:
            DailymotionAuthException: If authentication fails
        """
        data = {
            'grant_type': self.grant_type,
            'client_id': self.client_api,
            'client_secret': self.__client_secret,
            'scope': ' '.join(self.scope)
        }

        if self.grant_type == 'password':
            if self.username is None or self.password is None:
                raise DailymotionAuthException('Username and password are required for password grant type')

            data['username'] = self.username
            data['password'] = self.password

        response = requests.post(
            "%s" % os.environ.get('DM_AUTH_URL'),
            data=data
        )

        if response.status_code == 200:
            data = json.loads(response.text)
            return Token(data['access_token'], data['refresh_token'], data['expires_in'], data['scope'],
                         data['token_type'])
        else:
            raise DailymotionAuthException('Authentication failed')

    def refresh_token(self) -> Token:
        """Refresh expired token using refresh token
        
        Returns:
            Token: New refreshed token instance
            
        Raises:
            DailymotionAuthException: If refresh fails
        """
        data = {
            'grant_type': 'refresh_token',
            'client_id': self.client_api,
            'client_secret': self.__client_secret,
            'refresh_token': Token.load().access_token
        }

        response = requests.post(
            "%s" % os.environ.get('DM_AUTH_URL'),
            data=data
        )

        if response.status_code == 200:
            data = json.loads(response.text)
            return Token(data['access_token'], data['refresh_token'], data['expires_in'], data['scope'],
                         data['token_type'])
        else:
            raise DailymotionAuthException('Token refresh failed')

    def get_token(self) -> Token:
        """Get valid authentication token, refreshing or generating new if needed
        
        Returns:
            Token: Valid authentication token instance
            
        Raises:
            DailymotionTokenExpired: If token refresh fails
        """
        try:
            token = Token.load()
            if token.is_expired():
                try:
                    token = self.refresh_token()
                    token.dump()
                except DailymotionAuthException:
                    raise DailymotionTokenExpired('Token was expired %d' % token.expires_in)
        except (DailymotionTokenExpired, FileNotFoundError) as e:
            token = self.__generate_token()
            token.dump()

        return token

class DailyMotion(object):
    """
    DailyMotion API client for interacting with GraphQL endpoints and downloading report files.
    Executing GraphQL queries (e.g. report generation and download)
    Managing polling logic with exponential backoff to wait for report readiness

    Args:
        authentication (Authentication): An authentication object that provides the token.

    Example usage:
        dm = DailyMotion(authentication=auth)
        report = dm.get_report_file(query, variable, max_retry=5, delay=2)
    """

    def __init__(self, authentication: Authentication):
        """Initialize API client with optional auth token"""

        self.__auth = authentication
        # Retrieve token from authentication or from kwargs fallback
        self.token = self.__auth.get_token()

        # httpClientSession with headers
        self.__set_http_client()

    def __set_http_client(self):
        """Set the HTTP client session used for making API requests"""
        self._client = requests.Session()
        self._client.headers.update({
            'Content-Type': 'application/json',
            'Authorization': self.token.get_authorization() if self.token else ''
        })

    def graph_ql(self, *, query: str, variable: dict[str, Any]) -> dict[str, Any]:
        """Execute GraphQL query against the API
        
        Args:
            query: GraphQL query string
            variable: Query variables
            
        Returns:
            dict: JSON response from API
        """

        try:
            response = self._client.post("%s" % (os.environ.get("DM_GRAPH_URL")),
                                     json={'query': query, 'variables': variable}).json()

            # Raise error if GraphQL response contains errors
            if response.get('errors'):
                raise DailymotionReportException(response['errors'][0]['message'])
            return response
        except JSONDecodeError as e:
            raise DailymotionClientException('Error decoding JSON response: %s' % e)

    def rest(self, path:str, fields:list[str]=None, **kwargs)-> dict[str,Any]:
        fields = {'fields': ','.join(fields) if fields else ''}
        data_merged = kwargs.get('data', {}) if isinstance(kwargs.get('data', {}), dict) else {}
        data_merged.update(kwargs.get('params', {}) if isinstance(kwargs.get('params', {}), dict) else {})

        try:
            response = self._client.request(method='POST',
                                            url="%s/%s" % ( os.environ.get("DM_REST_URL"), path.strip('/') ),
                                            data=data_merged | fields,
                                            headers={'Content-Type': 'application/x-www-form-urlencoded'}
                                            ).json()

            # Raise error if GraphQL response contains errors
            if response.get('error'):
                raise DailymotionApiException(response['error'])
            return response
        except JSONDecodeError as e:
            raise DailymotionClientException('Error decoding JSON response: %s' % e)

    def get_report_file(self, query:str, variable:dict[str,Any], **kwargs) -> list[str]:
        """
        Generate a report and poll for downloadable report file links.

        Args:
            query: The GraphQL query to trigger report generation
            variable: Variables for the report generation query
            kwargs:
                max_retry (int, optional): Maximum number of retries (infinite if not set)
                delay (int, optional): Base delay (in seconds) for exponential backoff

        Returns:
            list[str]: List of download links to the generated reports
        """
         # Initial request to generate report token
        response_tokens = self.graph_ql(query=query, variable=variable)

        #take all report token generated with recoursive search
        report_tokens = _recursive_search_key(response_tokens, 'reportToken')

        # Generate a GraphQL query and variable set to fetch report download links by token
        # generated for multiple report token
        automated_report_query, automated_report_variable = self.__generate_graphql_to_get_report_file_by_token(list(report_tokens))

        iteration = 0
        report_link_response = {}
        # Polling and exponential Backoff policy to get requests
        while (kwargs.get('max_retry', 0) <= iteration) if kwargs.get('max_retry') else True: # if max_retry is not set is infinte loop

            # Try to fetch report generated using the automated query and variables.
            report_link_response = self.graph_ql(query=automated_report_query, variable=automated_report_variable)

            # If there is no 'IN_PROGRESS' status, it means report is ready → exit loop
            if 'IN_PROGRESS' not in  tuple(_recursive_search_key(report_link_response, 'status')) :
                break

            # Implement an exponential delay to wait for report generation from Dailymotion
            exponetial_delay = kwargs.get('delay', 1) * (2 ** iteration)
            time.sleep(exponetial_delay)
            iteration += 1

        return list(_recursive_search_key(report_link_response, 'link'))

    def get_video_info(self, video_id:str, fields:list[str]):
        return self.rest(path="video/%s" % video_id, fields=fields)

    @staticmethod
    def __generate_graphql_to_get_report_file_by_token(tokens:list[str]) -> tuple[str, dict[Any, Any]]:
        """Generate a GraphQL query and its corresponding variable dictionary
        to retrieve the status and download links of one or more report files
        based on their report tokens.

        This function dynamically builds a query with one `reportFile` block
        per token, assigning each a unique variable name (e.g., $reportToken1,
        $reportToken2, ...), and constructs the associated variables dictionary
        required by the GraphQL query.

        Args:
            tokens (list[str]): A list of report token strings generated by a previous
                               report generation query.

        Returns:
            tuple[str, dict[Any, Any]]:
                - A GraphQL query string requesting the status and download link
                  for each report token.
                - A dictionary of variables to be used in the query, mapping each
                  dynamic token variable (e.g., "reportToken1") to its actual value.
        """
        variables = {}
        query_part = ''
        for i, report_token in enumerate(tokens, 1):
            query_part += '''
                report%s: reportFile(reportToken: $reportToken%s) {
                      status
                      downloadLinks {
                        edges {
                          node { link }
                        }
                      }
                }''' % (i,i)

            variables[("reportToken%s" % i)] = report_token

        query = '''query (%s){
            partner{%s
            }
        }''' % (
            ' '.join(list(map(lambda name: '$%s:String!' % name, variables.keys()))),
            query_part
        )

        return query, variables

def _recursive_search_key(node: dict|list, key: str) -> Generator:
    """Recursively search for all values of a given key in a nested dictionary or list.

    Args:
        node: Dictionary or list to search
        key: Key to search for

    Returns:
        list: List of all values associated with the key
    """
    if isinstance(node, dict):
        for dict_key, dict_value in node.items():
            if dict_key == key:
                yield dict_value
            yield from _recursive_search_key(dict_value, key)

    elif isinstance(node, list):
        for item in node:
            yield from _recursive_search_key(item, key)