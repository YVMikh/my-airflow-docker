import sys
from datetime import date
from pathlib import Path

import pytest

import pandas as pd
from airflow.exceptions import AirflowFailException

DAGS_DIR = Path(__file__).resolve().parents[1] / "airflow" / "dags"
sys.path.insert(0, str(DAGS_DIR))

from almaty_weather_metar_sync import (  # noqa: E402
    _chunks,
    _parse_metar_csv,
    _clean,
    DATA_COLUMNS,
    REQUEST_VARIABLES,
)


@pytest.mark.parametrize(
    "start, end, days, expected",
    [
        pytest.param(
            date(2026, 1, 1),
            date(2026, 1, 7),
            3,
            [
                (date(2026, 1, 1), date(2026, 1, 4)),
                (date(2026, 1, 4), date(2026, 1, 7)),
            ],
            id="exact-division",
        ),
        pytest.param(
            date(2026, 1, 1),
            date(2026, 1, 8),
            3,
            [
                (date(2026, 1, 1), date(2026, 1, 4)),
                (date(2026, 1, 4), date(2026, 1, 7)),
                (date(2026, 1, 7), date(2026, 1, 8)),
            ],
            id="partial-last-chunk",
        ),
        pytest.param(
            date(2026, 1, 1),
            date(2026, 1, 3),
            90,
            [
                (date(2026, 1, 1), date(2026, 1, 3)),
            ],
            id="period-shorter-than-chunk",
        ),
        pytest.param(
            date(2026, 1, 1),
            date(2026, 1, 1),
            90,
            [],
            id="empty-period",
        ),
        pytest.param(
            date(2024, 2, 28),
            date(2024, 3, 2),
            2,
            [
                (date(2024, 2, 28), date(2024, 3, 1)),
                (date(2024, 3, 1), date(2024, 3, 2)),
            ],
            id="leap-year-boundary",
        ),
    ],
)
def test_chunks(start, end, days, expected):
    assert list(_chunks(start, end, days)) == expected


def make_metar_csv(**overrides):
    row = {
        "station": "UAAA",
        "valid": "2026-01-01 12:00",
        **dict.fromkeys(REQUEST_VARIABLES, "null"),
        "tmpf": "32.5",
        "dwpf": "25.0",
        "skyc1": "FEW",
        "lon": "77.04",
        "lat": "43.35",
        "elevation": "681",
    }
    row.update(overrides)

    return pd.DataFrame([row]).to_csv(index=False)


def test_parse_metar_valid_csv():
    df = _parse_metar_csv(make_metar_csv())

    assert len(df) == 1
    assert list(df.columns) == DATA_COLUMNS
    assert df.loc[0, "station"] == "UAAA"
    assert df.loc[0, "valid"] == pd.Timestamp("2026-01-01 12:00")
    assert df.loc[0, "tmpf"] == pytest.approx(32.5)
    assert df.loc[0, "skyc1"] == "FEW"
    assert df.loc[0, "elevation"] == 681


def test_parse_metar_empty_response():
    df = _parse_metar_csv("")

    assert df.empty
    assert list(df.columns) == DATA_COLUMNS


def test_parse_metar_invalid_response():
    with pytest.raises(AirflowFailException):
        _parse_metar_csv("Service temporarily unavailable")


def test_parse_metar_missing_required_column():
    csv_text = make_metar_csv()
    df = pd.read_csv(pd.io.common.StringIO(csv_text))
    df = df.drop(columns=["tmpf"])

    with pytest.raises(AirflowFailException):
        _parse_metar_csv(df.to_csv(index=False))


def test_parse_metar_invalid_values():
    csv_text = make_metar_csv(
        valid="invalid-date",
        tmpf="invalid-number",
        dwpf="null",
        skyc1="",
    )

    df = _parse_metar_csv(csv_text)

    assert pd.isna(df.loc[0, "valid"])
    assert pd.isna(df.loc[0, "tmpf"])
    assert pd.isna(df.loc[0, "dwpf"])
    assert pd.isna(df.loc[0, "skyc1"])


def test_clean_filters_and_sorts():
    df = pd.DataFrame({
        "station": ["UAAA", "UAAA", "OTHER", "UAAA"],
        "valid": [
            pd.Timestamp("2026-01-01 15:00"),
            pd.NaT,
            pd.Timestamp("2026-01-01 10:00"),
            pd.Timestamp("2026-01-01 09:00"),
        ],
    })

    clean, rejected = _clean(
        df, date(2026, 1, 1), date(2026, 1, 2)
    )

    assert rejected == 2
    assert len(clean) == 2
    assert clean["station"].tolist() == ["UAAA", "UAAA"]
    assert clean["valid"].tolist() == [
        pd.Timestamp("2026-01-01 09:00"),
        pd.Timestamp("2026-01-01 15:00"),
    ]
    assert clean.index.tolist() == [0, 1]


@pytest.mark.parametrize(
    "invalid_timestamp",
    [
        "2025-12-31 23:59",
        "2026-01-02 00:00",
    ],
    ids=["before-start", "at-exclusive-end"],
)
def test_clean_rejects_out_of_range(invalid_timestamp):
    df = pd.DataFrame({
        "station": ["UAAA"],
        "valid": [pd.Timestamp(invalid_timestamp)],
    })

    with pytest.raises(AirflowFailException):
        _clean(df, date(2026, 1, 1), date(2026, 1, 2))


def test_clean_empty_dataframe():
    df = pd.DataFrame(columns=DATA_COLUMNS)

    clean, rejected = _clean(
        df, date(2026, 1, 1), date(2026, 1, 2)
    )

    assert clean.empty
    assert rejected == 0
    assert list(clean.columns) == DATA_COLUMNS
