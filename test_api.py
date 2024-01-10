import json
from flask_testing import TestCase
from main import app

class MyTest(TestCase):
        def test_get_users(self):
                response = self.client.get('/')
                self.assertEqual(response.status_code, 200)
                self.assertIn('users', json.loads(response.data))