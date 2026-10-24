import json
from collections import Counter
from pathlib import Path
import sys

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lab import ROOT, load_selected, mapper, finish_summary, salary_value, combo_stats
from source_tools import is_variant


def row(id, name="C++ developer", skills=None, **kwargs):
    return {"id": str(id), "name": name, "professional_roles_id": "96",
        "key_skills": skills if skills is not None else ["C++", "Linux", "Git"],
        "area_name": "Москва", "published_at": "2023-10-01", "experience": "От 1 года до 3 лет",
        "salary_from": 100000, "salary_to": 200000, "salary_currency": "RUR", "salary_gross": False, **kwargs}


def db_counts(path):
    with duckdb.connect(config={"threads": 2}) as con:
        load_selected(con, str(path))
        con.execute((ROOT / "sql/counts.sql").read_text(encoding="utf-8"))
        return {k: Counter({tuple(s): n for s, n in con.execute(f"SELECT skills,n FROM counts{k}").fetchall()}) for k in (1, 2, 3)}


def test_manual_counts_are_document_frequencies(tmp_path):
    # Repeated skill spelling inside a vacancy contributes once, not twice.
    rows = [row(1, skills=["C++", "Linux", "linux", " Git "]),
        row(2, skills=["Linux", "C++"]), row(3, name="Разработчик 1С"),
        row(4, name="Программист C", skills=[])]
    expected = {
        1: Counter({("c++",): 2, ("linux",): 2, ("git",): 1}),
        2: Counter({("c++", "linux"): 2, ("c++", "git"): 1, ("git", "linux"): 1}),
        3: Counter({("c++", "git", "linux"): 1}),
    }
    p = tmp_path / "rows.jsonl"
    p.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in rows), encoding="utf-8")
    combined, ids, hist, total = mapper(rows, True)
    naive, *_ = mapper(rows, False)
    assert combined == expected == db_counts(p)
    assert {k: Counter(v) for k, v in naive.items()} == expected
    assert len(ids) == 3 and hist[0] == 1
    assert finish_summary(combined, ids, hist, total)["contributions"] == {"1": 5, "2": 4, "3": 1}


def test_python_and_sql_match_unicode_and_false_positives(tmp_path):
    names = ["C++ developer", "Разработчик С++/Qt", "Junior C++ Developer", "Разработчик на С",
        "Программист Си", "c developer", "Программист C", "Разработчик C#", "Программист С #",
        "1 С программист", "Программист 1-С", "ABAP-разработчик c переквалификацией на 1С",
        "IOS разработчик Objective-C/Swift", "Программист ПЛИС (им. А.С. Попова)",
        "Программист с опытом Linux", "Разработчик MyC++", "C+++ developer",
        "C++ / 1C developer", "С-developer", "C++ тестировщик"]
    rows = [row(i, name=name) for i, name in enumerate(names)]
    rows[-1]["professional_roles_id"] = "124"
    p = tmp_path / "rows.jsonl"
    p.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in rows), encoding="utf-8")
    with duckdb.connect() as con:
        load_selected(con, str(p))
        actual = {r[0] for r in con.execute("SELECT id FROM selected").fetchall()}
    assert actual == {r["id"] for r in rows if is_variant(r)}


def test_all_real_source_rows_independently_match():
    p = ROOT / "data/it_vacancies.jsonl"
    if not p.exists():
        pytest.skip("Run lab.py prepare first")
    combined, ids, hist, n = mapper([json.loads(line) for line in p.open(encoding="utf-8")], True)
    assert combined == db_counts(p)
    summary = finish_summary(combined, ids, hist, n)
    assert summary["source_rows"] == 68770
    assert summary["selected_rows"] == 869


def test_salary_eligibility_and_regional_counts_include_unpaid_rows():
    good = row(1)
    assert salary_value(good) == 150000
    assert salary_value({**good, "salary_to": None}) == 100000
    for change in ({"salary_gross": True}, {"salary_gross": None}, {"salary_currency": "USD"},
        {"salary_from": 0}, {"salary_to": 50000}, {"salary_from": None, "salary_to": None}):
        assert salary_value({**good, **change}) is None
    rows = [{**good, "skills": ["c++", "linux"]}, {**good, "salary_from": None, "salary_to": None, "skills": ["c++", "linux"]}]
    counts, salaries = combo_stats(rows)
    assert counts[("c++", "linux")] == 2
    assert salaries[("c++", "linux")] == [150000]

