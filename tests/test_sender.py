from collections.abc import Iterator
from unittest.mock import Mock, call

import pytest

from services import sender
from services.sender import send_campaign


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    sleep_mock = Mock()
    monkeypatch.setattr(sender.time, "sleep", sleep_mock)
    monkeypatch.setattr(sender.repository, "save_message", Mock())


def test_send_campaign_marks_pending_item_as_sent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sender.evolution_api_service, "validate_configuration", lambda: None)
    send_mock = Mock(
        return_value={
            "success": True,
            "provider_message_id": "abc123",
            "error": None,
            "used_fallback": False,
        }
    )
    monkeypatch.setattr(sender.evolution_api_service, "send_text_message", send_mock)

    campaign = [
        {
            "campaign_id": "campaign_001",
            "student_name": "Joao Silva",
            "class_name": "7A",
            "phone": "14999991111",
            "message": "Teste",
            "status": "pending",
        }
    ]

    result = send_campaign(campaign)

    assert result[0]["status"] == "sent"
    assert result[0]["attempt_number"] == 1
    assert result[0]["sent_at"]
    assert result[0]["provider"] == "evolution"
    assert result[0]["provider_message_id"] == "abc123"
    assert result[0]["phone"] == "5514999991111@s.whatsapp.net"
    assert result[0]["dry_run"] is False
    assert campaign[0]["status"] == "pending"
    send_mock.assert_called_once_with(
        "5514999991111@s.whatsapp.net",
        "Teste",
        dry_run=False,
    )


def test_send_campaign_uses_fallback_metadata_when_service_reports_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sender.evolution_api_service, "validate_configuration", lambda: None)
    send_mock = Mock(
        return_value={
            "success": True,
            "provider_message_id": "msg-2",
            "error": None,
            "used_fallback": True,
        }
    )
    monkeypatch.setattr(sender.evolution_api_service, "send_text_message", send_mock)

    campaign = [
        {
            "campaign_id": "campaign_002",
            "student_name": "Maria Souza",
            "class_name": "8B",
            "phone": "(14) 99999-0001",
            "message": "Mensagem de teste",
            "status": "pending",
        }
    ]

    result = send_campaign(campaign)

    assert result[0]["status"] == "sent"
    assert result[0]["provider_message_id"] == "msg-2"
    assert result[0]["used_fallback"] is True
    send_mock.assert_called_once()


def test_send_campaign_marks_invalid_item_as_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    validate_mock = Mock()
    send_mock = Mock()
    monkeypatch.setattr(sender.evolution_api_service, "validate_configuration", validate_mock)
    monkeypatch.setattr(sender.evolution_api_service, "send_text_message", send_mock)

    campaign = [
        {
            "campaign_id": "campaign_003",
            "student_name": "Joao Silva",
            "class_name": "7A",
            "phone": "",
            "message": "Teste",
            "status": "pending",
        }
    ]

    result = send_campaign(campaign)

    assert result[0]["status"] == "failed"
    assert result[0]["attempt_number"] == 1
    assert result[0]["sent_at"]
    assert result[0]["provider"] == "evolution"
    assert result[0]["provider_message_id"] is None
    assert result[0]["failure_reason"] == "telefone_invalido"
    validate_mock.assert_called_once()
    send_mock.assert_not_called()


def test_send_campaign_marks_failed_when_provider_returns_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sender.evolution_api_service, "validate_configuration", lambda: None)
    monkeypatch.setattr(
        sender.evolution_api_service,
        "send_text_message",
        lambda phone, text, dry_run: {
            "success": False,
            "provider_message_id": None,
            "error": "erro no provider",
            "used_fallback": True,
        },
    )

    campaign = [
        {
            "campaign_id": "campaign_004",
            "student_name": "Ana",
            "class_name": "9A",
            "phone": "14999992222",
            "message": "Teste",
            "status": "pending",
        }
    ]

    result = send_campaign(campaign)

    assert result[0]["status"] == "failed"
    assert result[0]["failure_reason"] == "erro no provider"
    assert result[0]["used_fallback"] is True


def test_send_campaign_increments_attempt_number(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sender.evolution_api_service, "validate_configuration", lambda: None)
    monkeypatch.setattr(
        sender.evolution_api_service,
        "send_text_message",
        lambda phone, text, dry_run: {
            "success": True,
            "provider_message_id": "provider-1",
            "error": None,
            "used_fallback": False,
        },
    )

    campaign = [
        {
            "campaign_id": "campaign_005",
            "student_name": "Maria Souza",
            "class_name": "8B",
            "phone": "18999990001",
            "message": "Teste",
            "status": "pending",
            "attempt_number": 1,
        }
    ]

    result = send_campaign(campaign)

    assert result[0]["status"] == "sent"
    assert result[0]["attempt_number"] == 2
    assert result[0]["provider_message_id"] == "provider-1"


def test_send_campaign_fails_fast_on_invalid_evolution_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        sender.evolution_api_service,
        "validate_configuration",
        lambda: (_ for _ in ()).throw(ValueError("config invalida")),
    )

    campaign = [
        {
            "campaign_id": "campaign_006",
            "student_name": "Carlos",
            "class_name": "6A",
            "phone": "14999995555",
            "message": "Teste",
            "status": "pending",
        }
    ]

    with pytest.raises(ValueError, match="config invalida"):
        send_campaign(campaign)


def test_send_campaign_supports_dry_run(monkeypatch: pytest.MonkeyPatch) -> None:
    validate_mock = Mock()
    send_mock = Mock(
        return_value={
            "success": True,
            "provider_message_id": None,
            "error": None,
            "used_fallback": False,
        }
    )
    monkeypatch.setattr(sender.evolution_api_service, "validate_configuration", validate_mock)
    monkeypatch.setattr(sender.evolution_api_service, "send_text_message", send_mock)

    campaign = [
        {
            "campaign_id": "campaign_007",
            "student_name": "Aluno 1",
            "class_name": "1A",
            "phone": "14999990001",
            "message": "Teste 1",
            "status": "pending",
        }
    ]

    result = send_campaign(campaign, dry_run=True)

    assert result[0]["status"] == "sent"
    assert result[0]["dry_run"] is True
    validate_mock.assert_not_called()
    send_mock.assert_called_once_with(
        "5514999990001@s.whatsapp.net",
        "Teste 1",
        dry_run=True,
    )


def test_send_campaign_applies_delays_between_messages(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sender.evolution_api_service, "validate_configuration", lambda: None)
    monkeypatch.setattr(
        sender.evolution_api_service,
        "send_text_message",
        lambda phone, text, dry_run: {
            "success": True,
            "provider_message_id": "ok",
            "error": None,
            "used_fallback": False,
        },
    )
    sleep_mock = Mock()
    randint_values: Iterator[int] = iter([21, 22])

    monkeypatch.setattr(sender.time, "sleep", sleep_mock)
    monkeypatch.setattr(sender.random, "randint", lambda start, end: next(randint_values))

    campaign = [
        {
            "campaign_id": "campaign_008",
            "student_name": "Aluno 1",
            "class_name": "1A",
            "phone": "14999990001",
            "message": "Teste 1",
            "status": "pending",
        },
        {
            "campaign_id": "campaign_008",
            "student_name": "Aluno 2",
            "class_name": "1A",
            "phone": "14999990002",
            "message": "Teste 2",
            "status": "pending",
        },
        {
            "campaign_id": "campaign_008",
            "student_name": "Aluno 3",
            "class_name": "1A",
            "phone": "14999990003",
            "message": "Teste 3",
            "status": "pending",
        },
    ]

    send_campaign(campaign)

    assert sleep_mock.call_args_list == [call(21), call(22)]


def test_send_campaign_adds_extra_delay_every_ten_messages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sender.evolution_api_service, "validate_configuration", lambda: None)
    monkeypatch.setattr(
        sender.evolution_api_service,
        "send_text_message",
        lambda phone, text, dry_run: {
            "success": True,
            "provider_message_id": "ok",
            "error": None,
            "used_fallback": False,
        },
    )
    sleep_mock = Mock()
    randint_values: Iterator[int] = iter([20] * 9 + [20, 80])

    monkeypatch.setattr(sender.time, "sleep", sleep_mock)
    monkeypatch.setattr(sender.random, "randint", lambda start, end: next(randint_values))

    campaign = [
        {
            "campaign_id": "campaign_009",
            "student_name": f"Aluno {index}",
            "class_name": "2A",
            "phone": f"14999990{index:03d}",
            "message": f"Teste {index}",
            "status": "pending",
        }
        for index in range(1, 12)
    ]

    send_campaign(campaign)

    assert sleep_mock.call_count == 10
    assert sleep_mock.call_args_list[9] == call(100)
