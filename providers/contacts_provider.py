from data.repository import repository


class GoogleSheetsContactsProvider:
    def fetch_contacts(self) -> list[dict[str, str]]:
        return repository.carregar_contatos()
