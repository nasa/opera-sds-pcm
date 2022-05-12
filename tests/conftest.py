import pytest
import requests


@pytest.fixture(autouse=True)
def deny_network_requests(monkeypatch):
    monkeypatch.delattr(requests.sessions.Session, requests.sessions.Session.request.__name__)
