import splash.utils.requests_utils as ru  # Access the imported Settings within the target module

def test_get_proxy_returns_expected_dict(monkeypatch):
    monkeypatch.setattr(ru.Settings, "HTTP_PROXY", "http://proxy.example.com:8080")
    monkeypatch.setattr(ru.Settings, "NO_PROXY", "localhost,127.0.0.1")

    expected = {
        "http": "http://proxy.example.com:8080",
        "https": "http://proxy.example.com:8080",
        "no_proxy": "localhost,127.0.0.1"
    }

    result = ru.get_proxy()
    assert result == expected
