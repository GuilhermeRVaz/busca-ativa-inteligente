from pathlib import Path

from openpyxl import Workbook

from providers.absences_provider import ConsolidatedAbsencesProvider


def test_fetch_absences_reads_target_day(tmp_path: Path) -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.append(["Cabecalho antigo"])
    sheet.append(["Turma", "Nome", "RA", 24, 25])
    sheet.append(["7A", "Joao Silva", "123", 0, 1])
    sheet.append(["8B", "Maria Souza", "456", 1, 0])

    report_path = tmp_path / "consolidado.xlsx"
    workbook.save(report_path)

    provider = ConsolidatedAbsencesProvider()
    absences = provider.fetch_absences(report_path=report_path, day=25)

    assert absences == [
        {
            "student_name": "Joao Silva",
            "class_name": "7A",
            "absence_days": "25",
        }
    ]
