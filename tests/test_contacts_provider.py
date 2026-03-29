from providers.contacts_provider import GoogleSheetsContactsProvider


def test_records_to_contacts_normalizes_name_and_phones() -> None:
    provider = GoogleSheetsContactsProvider()
    records = [
        {
            "Nome do Aluno": "Joao Silva",
            "Turma": "7A",
            "Telefone 1": "(18) 99999-1111",
            "Telefone 2": "123",
            "Celular 3": "18 99999-2222",
        }
    ]

    contacts = provider._records_to_contacts(records)

    assert contacts == [
        {
            "student_name": "Joao Silva",
            "class_name": "7A",
            "phone1": "18999991111",
            "phone2": "18999992222",
            "phone3": "",
        }
    ]
