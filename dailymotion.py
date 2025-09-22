import functools
from json import JSONDecodeError
import requests, json, os, time
from typing import Self, Any, Literal, Generator


## Client Exception/Error

class DailymotionClientException(Exception):
    """Base exception class for DailyMotion client errors.

    This is the foundational exception class from which all DailyMotion client-specific
    exceptions inherit. It provides a standardized error reporting mechanism with
    numeric error codes and descriptive messages.

    Attributes:
        _code (int): Numeric error code for categorizing errors, defaults to -1
        message (str): Human readable error message describing the specific error
    """
    _code: int = -1

    def __init__(self, message=''):
        """Initialize the exception with an error message.

        Args:
            message (str, optional): Custom error message. If empty, uses default message.
        """
        self.message = 'Error %s: %s' % (int(self._code), message or 'Client error')
        super(DailymotionClientException, self).__init__(self.message)

    @property
    def code(self) -> int:
        return self.get_code()

    def get_code(self) -> int:
        return self._code


class DailymotionApiException(DailymotionClientException):
    """Exception raised for general API errors (code: 1000).

    This exception is thrown when API operations fail due to server-side issues,
    invalid requests, or other API-related problems that don't fall under
    authentication or report generation categories.
    """
    _code: int = 1000


class DailymotionAuthException(DailymotionClientException):
    """Exception raised for authentication errors (code: 2000).

    This exception covers all authentication-related failures including invalid
    credentials, failed token generation, and authorization issues.
    """
    _code: int = 2000


class DailymotionTokenExpired(DailymotionAuthException):
    """Exception raised when authentication token has expired (code: 2100).

    This specific authentication exception is thrown when an access token
    has expired and cannot be refreshed, requiring new authentication.
    """
    _code: int = 2100


class DailymotionReportException(DailymotionClientException):
    """Exception raised for report generation errors (code: 3000).

    This exception is thrown when report generation, processing, or retrieval
    operations fail due to various reasons including invalid queries or
    server-side report processing issues.
    """
    _code: int = 3000

## Utility functions

def _recursive_search_key(node: dict | list, key: str) -> Generator:
    """Recursively search for all values of a given key in nested data structures.

    Traverses nested dictionaries and lists to find all occurrences of a specific
    key and yields their values. This is particularly useful for extracting
    specific data from complex JSON responses like GraphQL results.

    The function handles both dictionary and list structures, recursively
    descending into nested elements to find all matching keys at any depth.

    Args:
        node (dict|list): Dictionary or list to search through recursively
        key (str): Key name to search for in dictionaries

    Yields:
        Any: All values associated with the specified key found at any nesting level

    Example:
        ```python
        data = {
            'user': {'id': 123, 'profile': {'id': 456}},
            'items': [{'id': 789}, {'id': 101112}]
        }
        list(_recursive_search_key(data, 'id'))  # Returns: [123, 456, 789, 101112]
        ```
    """
    if isinstance(node, dict):
        for dict_key, dict_value in node.items():
            if dict_key == key:
                yield dict_value
            yield from _recursive_search_key(dict_value, key)

    elif isinstance(node, list):
        for item in node:
            yield from _recursive_search_key(item, key)

def _refresh_token_if_expired(func):
    """Decorator that checks if the token is expired and refreshes it before executing the decorated method.

    This decorator ensures that the decorated function is a method (not a static function)
    by requiring 'self' as the first parameter. It automatically handles token expiration
    by checking the token status before each API call and refreshing it if necessary.

    Args:
        func: The method to be decorated (must be an instance method with self parameter)

    Returns:
        function: Wrapper function that handles token refresh logic

    Raises:
        TypeError: If the decorated function is not a method (missing self parameter)

    Example:
        @refresh_token_if_expired
        def api_call(self):
            return self._client.get("/api/endpoint")
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Ensure this is called on an instance method (check if first argument is self)
        if not args or not hasattr(args[0], '__dict__'):
            raise TypeError(f"'{func.__name__}' must be an instance method")

        self = args[0]

        # Check if the instance has an Authentication object
        auth = None
        for attr_name, attr_value in self.__dict__.items():
            if hasattr(attr_value, '__class__') and attr_value.__class__.__name__ == 'Authentication':
                auth = attr_value
                break

        if auth is None:
            raise TypeError(f"'{func.__name__}' requires an Authentication instance in the object")

        # Check if the current token is expired and refresh it if needed
        if self.token.is_expired():
            self.token = auth.get_token()
            self._client.headers.update({
                'Authorization': self.token.get_authorization()
            })

        # Execute the original method
        return func(*args, **kwargs)

    return wrapper

## Main Classes

class Token(object):
    """Manages DailyMotion API authentication tokens and their persistence.

    This class handles the complete lifecycle of authentication tokens including
    creation, storage, loading, validation, and formatting. Tokens are persisted
    to FileSystem in JSON format to maintain authentication state across sessions.
    The class manages both access tokens for API requests and refresh tokens
    for obtaining new access tokens when they expire.

    Attributes:
        access_token (str): The current access token used for API authentication
        refresh_token (str): Token used to refresh expired access tokens
        expires_in (int): Unix timestamp when the token expires
        scope (str): Space or comma separated authorization scopes granted to this token
        token_type (str): Type of authentication token (typically 'Bearer')
        file_path (str): Filesystem path where token data is stored
    """

    def __init__(self, access_token, refresh_token, expires_in, scope, token_type, file_path='dm_token.json'):
        """Initialize Token instance with authentication credentials.

        Args:
            access_token (str): The access token for API requests
            refresh_token (str): Token for refreshing expired access tokens
            expires_in (int): Token expiration time in seconds from epoch
            scope (str): Authorization scopes granted to this token
            token_type (str): Type of token (e.g., 'Bearer')
            file_path (str, optional): Path for token storage file. Defaults to 'dm_token.json'.
        """
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.expires_in = expires_in
        self.scope = scope
        self.token_type = token_type
        self.__file_path = file_path

    def dump(self) -> None:
        """Save current token data to file in JSON format.

        Persists all token information to the configured file path. The expiration
        time is converted to an absolute timestamp by adding the current time.
        This allows the token to be properly validated when loaded later.

        Raises:
            IOError: If the file cannot be written to disk
        """
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
        """Load token data from file and create new Token instance.

        Reads previously saved token data from disk and reconstructs a Token
        instance with that data. This enables token persistence across
        application restarts.

        Args:
            file_path (str, optional): Path to token storage file. Defaults to 'dm_token.json'.

        Returns:
            Token: New Token instance initialized with loaded data

        Raises:
            FileNotFoundError: If the token file doesn't exist
            json.JSONDecodeError: If the file contains invalid JSON
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
        """Check if the current token has expired.

        Compares the current system time against the token's expiration timestamp
        to determine if the token is still valid for API requests.

        Returns:
            bool: True if current time is past expiration, False if token is still valid
        """
        return int(time.time()) >= self.expires_in

    def get_authorization(self):
        """Get formatted authorization header value.

        Constructs the authorization header value in the format expected by
        the DailyMotion API, combining the token type and access token.

        Returns:
            str: Authorization header value in format '{token_type} {access_token}'
        """
        return '%s %s' % (self.token_type, self.access_token)


class Authentication(object):
    """Handles DailyMotion API authentication using different OAuth2 flows.

    This class supports both password-based and client credentials authentication
    flows as defined by the OAuth2 specification. It manages the complete
    authentication lifecycle including initial token generation, token refresh,
    and secure credential storage.

    The class supports two authentication methods:
    1. Password flow: Uses username/password for user authentication
    2. Client credentials flow: Uses only client credentials for app authentication

    Attributes:
        client_api (str): API client ID provided by DailyMotion
        client_secret (str): API client secret (kept private)
        grant_type (str): OAuth2 grant type ('password' or 'client_credentials')
        scope (list[str]): List of authorization scopes requested
        username (str, optional): Username for password-based authentication
        password (str, optional): Password for password-based authentication
    """

    def __init__(self, client_api: str, client_secret: str, *, grant_type: Literal["client_credentials", "password"],
                 scope: list[str], **kwargs):
        """Initialize Authentication with API credentials and authentication type.

        Args:
            client_api (str): DailyMotion API client identifier
            client_secret (str): DailyMotion API client secret
            grant_type (Literal["client_credentials", "password"]): OAuth2 grant type
            scope (list[str]): List of permission scopes to request
            **kwargs: Additional arguments including username/password for password flow

        Raises:
            DailymotionClientException: If required credentials are missing
            DailymotionAuthException: If password flow lacks username/password
        """
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
        """Create Authentication instance configured for password-based flow.

        Factory method that creates an Authentication instance specifically
        configured for the OAuth2 password grant type, which requires user
        credentials (username and password) for authentication.

        Args:
            client_api (str): DailyMotion API client ID
            client_secret (str): DailyMotion API client secret
            username (str): User's DailyMotion username
            password (str): User's DailyMotion password
            scope (list[str]): List of permission scopes to request

        Returns:
            Authentication: Configured Authentication instance for password flow
        """
        return cls(client_api, client_secret, grant_type='password', scope=scope, username=username, password=password)

    @classmethod
    def from_credential(cls, client_api: str, client_secret: str, scope: list[str]):
        """Create Authentication instance configured for client credentials flow.

        Factory method that creates an Authentication instance for the OAuth2
        client credentials grant type, which uses only the client credentials
        without requiring user authentication.

        Args:
            client_api (str): DailyMotion API client ID
            client_secret (str): DailyMotion API client secret
            scope (list[str]): List of permission scopes to request

        Returns:
            Authentication: Configured Authentication instance for client credentials flow
        """
        return cls(client_api, client_secret, grant_type='client_credentials', scope=scope)

    def __generate_token(self) -> Token:
        """Generate new access token via OAuth2 authentication request.

        Makes an HTTP POST request to the DailyMotion OAuth2 token endpoint
        with the appropriate credentials based on the configured grant type.
        Creates and returns a Token instance with the response data.

        Returns:
            Token: New token instance containing access and refresh tokens

        Raises:
            DailymotionAuthException: If authentication request fails or credentials are invalid
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
        """Refresh expired token using the stored refresh token.

        Uses the OAuth2 refresh token flow to obtain a new access token
        when the current one has expired. Loads the existing token from
        disk and uses its refresh token to get new credentials.

        Returns:
            Token: New refreshed token instance with updated credentials

        Raises:
            DailymotionAuthException: If token refresh fails or refresh token is invalid
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
        """Get valid authentication token, refreshing or generating new if needed.

        This method implements the complete token management logic:
        1. Attempts to load existing token from disk
        2. Checks if token is expired and refreshes if needed
        3. Generates new token if no valid token exists
        4. Saves the token to disk for future use

        Returns:
            Token: Valid authentication token ready for API requests

        Raises:
            DailymotionTokenExpired: If token refresh fails and token cannot be renewed
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


class DailymotionClient(object):
    """
    DailyMotion API client for interacting with GraphQL endpoints and REST API.

    This client provides a comprehensive interface to the DailyMotion API, supporting
    both GraphQL queries for complex operations like report generation and REST API
    calls for simpler data retrieval. The client handles authentication automatically,
    including token refresh when needed.

    Key features:
    - Automatic token management and refresh
    - GraphQL query execution with error handling
    - REST API interaction with proper formatting
    - Report generation with polling and exponential backoff
    - Video information retrieval

    The client uses a decorator pattern to ensure all API calls are made with
    valid authentication tokens, automatically refreshing them when expired.

    Args:
        authentication (Authentication): Authentication instance that provides token management

    Example:
        ```python
        auth = Authentication.from_credential(client_id, client_secret, ['read'])
        dm = DailyMotion(authentication=auth)

        # Execute GraphQL query
        result = dm.graph_ql(query="query { viewer { id } }", variable={})

        # Get video information
        video_info = dm.get_video_info("x123456", ["title", "description"])

        # Generate and download report
        report_links = dm.get_report_file(query, variables, max_retry=5, delay=2)
        ```
    """

    def __init__(self, authentication: Authentication):
        """Initialize API client with authentication.

        Sets up the HTTP client session with proper headers and retrieves
        an initial authentication token for API requests.

        Args:
            authentication (Authentication): Authentication instance for token management
        """

        self.__auth = authentication
        # Retrieve initial token from authentication system
        self.token = self.__auth.get_token()

        # Initialize HTTP client session with proper headers
        self.__set_http_client()

    def __set_http_client(self):
        """Set up the HTTP client session used for making API requests.

        Configures a requests Session with appropriate headers including
        Content-Type for JSON requests and Authorization header with
        the current authentication token.
        """
        self._client = requests.Session()
        self._client.headers.update({
            'Content-Type': 'application/json',
            'Authorization': self.token.get_authorization() if self.token else ''
        })

    @_refresh_token_if_expired
    def graph_ql(self, *, query: str, variable: dict[str, Any]) -> dict[str, Any]:
        """Execute GraphQL query against the DailyMotion API.

        Sends a GraphQL query to the API endpoint with the provided variables.
        Handles authentication automatically and processes the response,
        raising appropriate exceptions for GraphQL errors.

        Args:
            query (str): GraphQL query string to execute
            variable (dict[str, Any]): Variables to be used in the GraphQL query

        Returns:
            dict[str, Any]: JSON response data from the GraphQL API

        Raises:
            DailymotionReportException: If the GraphQL response contains errors
            DailymotionClientException: If JSON decoding fails
        """

        try:
            response = self._client.post("%s" % (os.environ.get("DM_GRAPH_URL")),
                                         json={'query': query, 'variables': variable}).json()

            # Check for GraphQL errors in response and raise exception if found
            if response.get('errors'):
                raise DailymotionReportException(response['errors'][0]['message'])
            return response
        except JSONDecodeError as e:
            raise DailymotionClientException('Error decoding JSON response: %s' % e)

    @_refresh_token_if_expired
    def rest(self, path: str, fields: list[str] = None, **kwargs) -> dict[str, Any]:
        """Execute REST API request to DailyMotion API.

        Makes a REST API call to the specified path with optional field selection
        and additional parameters. Handles form-encoded data submission and
        merges various parameter sources.

        Args:
            path (str): API endpoint path (without leading slash)
            fields (list[str], optional): List of fields to include in response
            **kwargs: Additional parameters including 'data' and 'params' dictionaries

        Returns:
            dict[str, Any]: JSON response from the REST API

        Raises:
            DailymotionApiException: If the API response contains an error
            DailymotionClientException: If JSON decoding fails
        """
        fields = {'fields': ','.join(fields) if fields else ''}
        data_merged = kwargs.get('data', {}) if isinstance(kwargs.get('data', {}), dict) else {}
        data_merged.update(kwargs.get('params', {}) if isinstance(kwargs.get('params', {}), dict) else {})

        try:
            response = self._client.request(method='POST',
                                            url="%s/%s" % (os.environ.get("DM_REST_URL"), path.strip('/')),
                                            data=data_merged | fields,
                                            headers={'Content-Type': 'application/x-www-form-urlencoded'}
                                            ).json()

            # Check for API errors in response and raise exception if found
            if response.get('error'):
                raise DailymotionApiException(response['error'])
            return response
        except JSONDecodeError as e:
            raise DailymotionClientException('Error decoding JSON response: %s' % e)

    def get_report_file(self, query: str, variable: dict[str, Any], **kwargs) -> list[str]:
        """
        Generate a report and poll for downloadable report file links.

        This method implements a complete report generation workflow:
        1. Executes the provided GraphQL query to initiate report generation
        2. Extracts report tokens from the response
        3. Polls the API using exponential backoff until reports are ready
        4. Returns download links for all generated reports

        The polling mechanism uses exponential backoff to avoid overwhelming
        the API while waiting for report processing to complete.

        Args:
            query (str): GraphQL query to trigger report generation
            variable (dict[str,Any]): Variables for the report generation query
            **kwargs: Optional parameters for polling behavior:
                max_retry (int, optional): Maximum number of polling attempts (infinite if not set)
                delay (int, optional): Base delay in seconds for exponential backoff (default: 1)

        Returns:
            list[str]: List of HTTP URLs for downloading the generated report files

        Raises:
            DailymotionReportException: If report generation fails
            DailymotionClientException: If API communication fails
        """
        # Execute initial GraphQL query to generate report tokens
        response_tokens = self.graph_ql(query=query, variable=variable)

        # Extract all report tokens from the nested response structure
        report_tokens = _recursive_search_key(response_tokens, 'reportToken')

        # Generate GraphQL query to poll for report completion status
        automated_report_query, automated_report_variable = self.__generate_graphql_to_get_report_file_by_token(
            list(report_tokens))

        iteration = 0
        report_link_response = {}
        # Implement polling loop with exponential backoff
        while (kwargs.get('max_retry', 0) <= iteration) if kwargs.get(
                'max_retry') else True:  # Infinite loop if max_retry not set

            # Poll API for report status and download links
            report_link_response = self.graph_ql(query=automated_report_query, variable=automated_report_variable)

            # Check if any reports are still in progress
            if 'IN_PROGRESS' not in tuple(_recursive_search_key(report_link_response, 'status')):
                break

            # Apply exponential backoff delay before next polling attempt
            exponetial_delay = kwargs.get('delay', 1) * (2 ** iteration)
            time.sleep(exponetial_delay)
            iteration += 1

        # Extract and return all download links from the final response
        return list(_recursive_search_key(report_link_response, 'link'))

    @staticmethod
    def __generate_graphql_to_get_report_file_by_token(tokens: list[str]) -> tuple[str, dict[Any, Any]]:
        """Generate GraphQL query to retrieve report files by their tokens.

        Creates a dynamic GraphQL query that can fetch the status and download
        links for multiple reports simultaneously. Each report token gets its
        own query block with a unique variable name, allowing efficient bulk
        retrieval of report information.

        The generated query structure allows checking multiple reports in a single
        API call, which is more efficient than individual requests for each report.

        Args:
            tokens (list[str]): List of report token strings from previous report generation

        Returns:
            tuple[str, dict[Any, Any]]: A tuple containing:
                - GraphQL query string with dynamic report blocks
                - Variables dictionary mapping token variables to their actual values

        Example:
            For tokens ['token1', 'token2'], generates:
            ```
            query ($reportToken1: String!, $reportToken2: String!) {
                partner {
                    report1: reportFile(reportToken: $reportToken1) {
                        status
                        downloadLinks { edges { node { link } } }
                    }
                    report2: reportFile(reportToken: $reportToken2) {
                        status
                        downloadLinks { edges { node { link } } }
                    }
                }
            }
            ```
        """
        variables = {}
        query_part = ''
        # Build query blocks for each report token
        for i, report_token in enumerate(tokens, 1):
            query_part += '''
                report%s: reportFile(reportToken: $reportToken%s) {
                      status
                      downloadLinks {
                        edges {
                          node { link }
                        }
                      }
                }''' % (i, i)

            variables[("reportToken%s" % i)] = report_token

        # Construct complete GraphQL query with variable declarations
        query = '''query (%s){
            partner{%s
            }
        }''' % (
            ' '.join(list(map(lambda name: '$%s:String!' % name, variables.keys()))),
            query_part
        )

        return query, variables

__all__ = ['Authentication',
           'DailymotionClient',
           'DailymotionApiException',
           'DailymotionAuthException',
           'DailymotionClientException',
           'DailymotionReportException',
           'DailymotionTokenExpired'
           ]