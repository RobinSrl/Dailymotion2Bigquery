import os
from unittest import TestCase
import dailymotion as dm

class Dailymotion(TestCase):

    def setUp(self):
        self.apiClientId = os.getenv("DM_CLIENT_API")
        self.apiClientSecret = os.getenv("DM_CLIENT_SECRET")
        self._client = self._auth = None

    @property
    def auth(self):
        if self._auth is None:
            self._auth = dm.Authentication.from_credential(
                self.apiClientId,
                self.apiClientSecret,
                scope=['create_reports', 'delete_reports', 'manage_reports']
            )
        return self._auth

    @property
    def client(self):
        if self._client is None:
            self._client = dm.DailymotionClient(self.auth)
        return self._client



    def test_endpoints(self):
        self.assertIsNotNone(dm.BASE_ENDPOINT)
        self.assertEqual(dm.BASE_ENDPOINT, os.getenv("DM_BASE_URL",  'https://partner.api.dailymotion.com').strip('/'))

    def test_scopes(self):

        valid_scopes = {"access_ads", "access_revenue", "create_reports", "delete_reports", "email", "manage_reports",
                        "manage_likes", "manage_players", "manage_playlists", "manage_podcasts",
                        "manage_subscriptions", "manage_subtitles", "manage_videos", "read_reports", "userinfo"}
        scope = {'create_reports', 'delete_reports', 'manage_reports'}

        self.assertTrue(
            scope.issubset(valid_scopes),
            f"Invalid scopes: {scope - valid_scopes}"
        )
        self.assertRaises(dm.DailymotionClientException, dm.Authentication.from_credential,
            self.apiClientId,
            self.apiClientSecret,
            scope=list(scope) + ['foo']
        )

    #TODO: The following test makes a call to the real API. Implement the mock to increase performance and have more control.

    def test_auth(self):
        self.assertIsNotNone(self.auth)
        self.assertIsInstance(self.auth, dm.Authentication)
        self.assertEqual(self.auth.client_api, os.getenv("DM_CLIENT_API"))
        self.assertIsNotNone(self.auth.get_token())
        self.assertIsInstance(self.auth.get_token(), dm.Token)


        self.assertRaises(dm.DailymotionAuthException, dm.Authentication, ## pass wrong credentials
                          self.apiClientId,
                          self.apiClientSecret,
                          scope=['create_reports'],
                          grant_type='password',
                          username=None,
                          password=None
                          )
        self.assertRaises(dm.DailymotionClientException, dm.Authentication, ## pass wrong grant type
                          self.apiClientId,
                          self.apiClientSecret,
                          scope=['create_reports'],
                          grant_type='pippo',
                          password=None,
                          username=None
                          )
        self.assertRaises(dm.DailymotionClientException, dm.Authentication, ## pass no scope
                          self.apiClientId,
                          self.apiClientSecret,
                          scope=None,
                          grant_type='client_credentials',
                          password=None,
                          username=None
                          )

    def test_client(self):
        self.assertIsNotNone(self.client)
        self.assertIsInstance(self.client, dm.DailymotionClient)
        # TODO: continue testing client

    def test_rest(self):
        fields = "id,title,description,duration,created_time,tags,url".split(',')

        rest_resp = self.client.rest('video/x9xt9o4', fields=fields)
        self.assertIsNotNone(rest_resp)
        for fild in fields:
            self.assertIn(fild, rest_resp)

