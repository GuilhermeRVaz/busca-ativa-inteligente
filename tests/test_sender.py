from collections.abc import Iterator
from unittest.mock import Mock, call

import pytest
import requests

from services import sender
from services.sender import send_campaign


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    sleep_mock = Mock()
    monkeypatch.setattr(sender.time, "sleep", sleep_mock)


def _mock_response(
    status_code: int,
    payload: dict | None = None,
    *,
    json_error: Exception | None = None,
) -> Mock:
    response = Mock()
    response.status_code = status_code
    if json_error is not None:
        response.json.side_effect = json_error
    else:
        response.json.return_value = payload or {}
    return response


def test_send_campaign_marks_pending_item_as_sent(monkeypatch: pytest.MonkeyPatch) -> None:
    post_mock = Mock(return_value=_mock_response(200, {"key": {"id": "abc123"}}))
    monkeypatch.setattr(sender.requests, "post", post_mock)

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
    assert campaign[0]["status"] == "pending"
    post_mock.assert_called_once_with(
        sender.EVOLUTION_ENDPOINT,
        headers=sender.EVOLUTION_HEADERS,
        json={
            "number": "5514999991111@s.whatsapp.net",
            "text": "Teste",
        },
        timeout=sender.REQUEST_TIMEOUT_SECONDS,
    )


def test_send_campaign_uses_fallback_payload_when_primary_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    post_mock = Mock(
        side_effect=[
            _mock_response(400, {"error": "payload invalido"}),
            _mock_response(200, {"messageId": "msg-2"}),
        ]
    )
    monkeypatch.setattr(sender.requests, "post", post_mock)

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
    assert post_mock.call_count == 2
    assert post_mock.call_args_list == [
        call(
            sender.EVOLUTION_ENDPOINT,
            headers=sender.EVOLUTION_HEADERS,
            json={
                "number": "5514999990001@s.whatsapp.net",
                "text": "Mensagem de teste",
            },
            timeout=sender.REQUEST_TIMEOUT_SECONDS,
        ),
        call(
            sender.EVOLUTION_ENDPOINT,
            headers=sender.EVOLUTION_HEADERS,
            json={
                "number": "5514999990001@s.whatsapp.net",
                "textMessage": {"text": "Mensagem de teste"},
            },
            timeout=sender.REQUEST_TIMEOUT_SECONDS,
        ),
    ]


def test_send_campaign_marks_invalid_item_as_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    post_mock = Mock()
    monkeypatch.setattr(sender.requests, "post", post_mock)

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
    post_mock.assert_not_called()


def test_send_campaign_marks_failed_when_response_contains_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    post_mock = Mock(
        side_effect=[
            _mock_response(200, {"error": "erro no provider"}),
            _mock_response(200, {"error": "erro no provider"}),
        ]
    )
    monkeypatch.setattr(sender.requests, "post", post_mock)

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
    assert post_mock.call_count == 2


def test_send_campaign_increments_attempt_number(monkeypatch: pytest.MonkeyPatch) -> None:
    post_mock = Mock(return_value=_mock_response(200, {"id": "provider-1"}))
    monkeypatch.setattr(sender.requests, "post", post_mock)

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


def test_send_campaign_marks_failed_on_request_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    post_mock = Mock(side_effect=requests.Timeout("timeout"))
    monkeypatch.setattr(sender.requests, "post", post_mock)

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

    result = send_campaign(campaign)

    assert result[0]["status"] == "failed"
    assert result[0]["sent_at"]
    assert result[0]["provider"] == "evolution"
    assert result[0]["provider_message_id"] is None


def test_send_campaign_applies_delays_between_messages(monkeypatch: pytest.MonkeyPatch) -> None:
    post_mock = Mock(return_value=_mock_response(200, {"id": "ok"}))
    sleep_mock = Mock()
    randint_values: Iterator[int] = iter([21, 22])

    monkeypatch.setattr(sender.requests, "post", post_mock)
    monkeypatch.setattr(sender.time, "sleep", sleep_mock)
    monkeypatch.setattr(sender.random, "randint", lambda start, end: next(randint_values))

    campaign = [
        {
            "campaign_id": "campaign_007",
            "student_name": "Aluno 1",
            "class_name": "1A",
            "phone": "14999990001",
            "message": "Teste 1",
            "status": "pending",
        },
        {
            "campaign_id": "campaign_007",
            "student_name": "Aluno 2",
            "class_name": "1A",
            "phone": "14999990002",
            "message": "Teste 2",
            "status": "pending",
        },
        {
            "campaign_id": "campaign_007",
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
    post_mock = Mock(return_value=_mock_response(200, {"id": "ok"}))
    sleep_mock = Mock()
    randint_values: Iterator[int] = iter([20] * 9 + [20, 80])

    monkeypatch.setattr(sender.requests, "post", post_mock)
    monkeypatch.setattr(sender.time, "sleep", sleep_mock)
    monkeypatch.setattr(sender.random, "randint", lambda start, end: next(randint_values))

    campaign = [
        {
            "campaign_id": "campaign_008",
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
