from fastapi.testclient import TestClient
from app import app  # Import your FastAPI application instance
from conftest import test_db_session, TestingSessionLocal, regular_token1


class TestLanguageAndScriptEndpoints:
    def test_list_languages(self, client, regular_token1, test_db_session):
        # Test listing languages
        headers = {"Authorization": f"Bearer {regular_token1}"}
        response = client.get("/v3/language", headers=headers)
        assert response.status_code == 200
        assert response.json() == [{"iso639": "eng", "name": "english"}]

    def test_list_scripts(self, client, regular_token1, test_db_session):
        # Test listing scripts
        headers = {"Authorization": f"Bearer {regular_token1}"}
        response = client.get("/v3/script", headers=headers)
        assert response.status_code == 200
        assert response.json() == [{"iso15924": "Latn", "name": "latin"}]
