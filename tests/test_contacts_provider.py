from data.repository import repository
from providers.contacts_provider import GoogleSheetsContactsProvider


def test_fetch_contacts_uses_repository(monkeypatch) -> None:
    expected = [
        {
            "student_name": "Joao Silva",
            "class_name": "7A",
            "phone1": "18999991111",
            "phone2": "",
            "phone3": "",
        }
    ]

    monkeypatch.setattr(repository, "carregar_contatos", lambda: expected)

    provider = GoogleSheetsContactsProvider()

    assert provider.fetch_contacts() == expected
