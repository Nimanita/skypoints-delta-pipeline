"""country_tables: parts that need no Delta (name safety, worker count, thread pool, country mapping)."""
import threading

import pytest

from pipeline_utils.silver import country_tables as ct


# ----------------------------------------------------------- _safe_table_name

@pytest.mark.parametrize("name, expected", [
    ("member_in", "member_in"),
    ("silver.member_in", "skypoints.silver.member_in"),          # 2 parts: catalog is added
    ("skypoints.silver.member_in", "skypoints.silver.member_in"),
])
def test_safe_table_name_accepts(name, expected):
    assert ct._safe_table_name(name) == expected


@pytest.mark.parametrize("name", [
    "a; DROP TABLE x", "a b", "a-b", "a.b.c.d", "a..b", "", "a`b", "a'b", "x.y.z; --", "a\nb",
])
def test_safe_table_name_rejects(name):
    with pytest.raises(ValueError):
        ct._safe_table_name(name)


# --------------------------------------------------------- choose_worker_count

@pytest.mark.parametrize("countries, max_workers, expected", [
    (0, None, 1), (1, None, 1), (2, None, 2), (5, None, 5),
    (8, None, 8), (50, None, ct.MAX_PARALLEL_MERGES),
    (50, 3, 3), (2, 10, 2), (50, 0, ct.MAX_PARALLEL_MERGES),     # 0 / None = use the default
])
def test_choose_worker_count(countries, max_workers, expected):
    rows = {f"C{i}": 1 for i in range(countries)}
    assert ct.choose_worker_count(rows, max_workers) == expected


# --------------------------------------------------------------- _run_parallel

def test_run_parallel_runs_every_task_biggest_first():
    started, rows = [], {"SMALL": 1, "BIG": 1000, "MID": 50}
    tasks = {code: (lambda c=code: started.append(c)) for code in rows}
    ct._run_parallel(tasks, rows, max_workers=1)          # one worker = submission order is visible
    assert started == ["BIG", "MID", "SMALL"]


def test_run_parallel_uses_threads_when_allowed():
    seen, barrier = set(), threading.Barrier(3, timeout=10)

    def task():
        seen.add(threading.get_ident())
        barrier.wait()                                    # only passes if 3 tasks run at the same time

    rows = {"A": 1, "B": 1, "C": 1}
    ct._run_parallel({c: task for c in rows}, rows, max_workers=3)
    assert len(seen) == 3


def test_run_parallel_raises_first_error():
    def boom():
        raise RuntimeError("merge failed")

    with pytest.raises(RuntimeError, match="merge failed"):
        ct._run_parallel({"A": boom, "B": lambda: None}, {"A": 2, "B": 1})


def test_run_parallel_with_no_tasks_is_a_noop():
    ct._run_parallel({}, {})


# ------------------------------------------------------ get_country_target_tables

@pytest.fixture()
def reference(spark, monkeypatch):
    """Point COUNTRY_TABLE at a temp view so no catalog is needed."""
    def make(rows, columns="country_code STRING"):
        spark.createDataFrame(rows, columns).createOrReplaceTempView("country_ref_test")
        monkeypatch.setattr(ct, "COUNTRY_TABLE", "country_ref_test")
    yield make
    spark.catalog.dropTempView("country_ref_test")


def test_default_table_names_when_no_target_column(spark, reference):
    reference([("IN",), ("US",), ("U-K",), (None,), ("IN",)])
    assert ct.get_country_target_tables(spark) == {
        "IN": "skypoints.silver.member_in",
        "US": "skypoints.silver.member_us",
        "U-K": "skypoints.silver.member_u_k",       # non-alphanumerics become _
    }


def test_target_column_wins_and_blank_falls_back(spark, reference):
    reference([("IN", "skypoints.silver.custom_india"), ("US", None), ("UK", "")],
              "country_code STRING, target_table STRING")
    assert ct.get_country_target_tables(spark) == {
        "IN": "skypoints.silver.custom_india",
        "US": "skypoints.silver.member_us",
        "UK": "skypoints.silver.member_uk",
    }


def test_unsafe_target_table_is_refused(spark, reference):
    reference([("IN", "x; DROP TABLE y")], "country_code STRING, target_table STRING")
    with pytest.raises(ValueError):
        ct.get_country_target_tables(spark)


def test_read_known_country_codes(spark, reference):
    reference([("IN",), ("US",)])
    out = ct.read_known_country_codes(spark).collect()
    assert {(r["country_code"], r["is_known_country"]) for r in out} == {("IN", True), ("US", True)}


# ------------------------------------------------------------------ constants

def test_country_table_ddl_matches_clean_cols():
    ddl_columns = [line.split()[0] for line in ct.COUNTRY_TABLE_COLUMNS_DDL.strip().splitlines()]
    assert ddl_columns == ct.CLEAN_COLS


def test_merge_templates_have_the_safety_conditions():
    assert "s.file_date >= t.file_date" in ct.UPSERT_COUNTRY_SQL
    assert "t.file_date <= s.file_date" in ct.DELETE_MOVER_SQL
    assert "s.file_date >= t.file_date" in ct.UPDATE_LOOKUP_SQL
    for sql in (ct.UPSERT_COUNTRY_SQL, ct.DELETE_MOVER_SQL, ct.UPDATE_LOOKUP_SQL):
        assert "country_code" not in sql.split("WHEN")[0]       # never in the ON clause
