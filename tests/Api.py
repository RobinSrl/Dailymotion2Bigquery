import os
from dataclasses import fields
from unittest import TestCase
import dailymotion as dm

class Dailymotion(TestCase):

    def setUp(self):
        self.apiClientId = os.getenv("DM_CLIENT_API")
        self.apiClientSecret = os.getenv("DM_CLIENT_SECRET")

        self.auth = dm.Authentication.from_credential(
            self.apiClientId,
            self.apiClientSecret,
            scope=['create_reports', 'delete_reports', 'manage_reports']
        )
        self.client = dm.DailymotionClient(self.auth)

    def _test_auth(self):
        self.assertIsNotNone(self.auth)
        self.assertIsInstance(self.auth, dm.Authentication)
        self.assertEqual(self.auth.client_api, os.getenv("DM_CLIENT_API"))

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

        self.assertIsNotNone(self.auth.get_token())
        self.assertIsInstance(self.auth.get_token(), dm.Token)

    def _test_client(self):
        self.assertIsNotNone(self.client)
        self.assertIsInstance(self.client, dm.DailymotionClient)
        # TODO: continue testing client

    def test_rest(self):
        fields = "id,title,description,duration,created_time,tags,url".split(',')

        rest_resp = self.client.rest('video/x9xt9o4', fields=fields)
        self.assertIsNotNone(rest_resp)
        for fild in fields:
            self.assertIn(fild, rest_resp)

