import pytest
from src.transform.metrics import ON_TIME_THRESHOLD_SECONDS, on_time_pct, percentile


def test_on_time_pct_all_on_time():
    assert on_time_pct([0, 60, -30, 299]) == 1.0


def test_on_time_pct_all_late():
    assert on_time_pct([600, 700, 800]) == 0.0


def test_on_time_pct_mixed():
    result = on_time_pct([0, 600, 300, 900])
    assert result == pytest.approx(0.5)


def test_on_time_pct_with_none():
    result = on_time_pct([None, 0, None, 600])
    assert result == pytest.approx(0.5)


def test_on_time_pct_all_none():
    assert on_time_pct([None, None]) is None


def test_on_time_pct_empty():
    assert on_time_pct([]) is None


def test_on_time_pct_boundary_exactly_at_threshold():
    # exactly 300s delay is on-time (<=threshold)
    assert on_time_pct([ON_TIME_THRESHOLD_SECONDS]) == 1.0


def test_on_time_pct_boundary_one_over_threshold():
    # 301s is late
    assert on_time_pct([ON_TIME_THRESHOLD_SECONDS + 1]) == 0.0


def test_on_time_pct_negative_delay_counts_as_on_time():
    # early arrivals (negative delay) should be on-time
    assert on_time_pct([-300]) == 1.0
    assert on_time_pct([-301]) == 0.0


def test_percentile_median():
    result = percentile([10.0, 20.0, 30.0, 40.0, 50.0], 50)
    assert result == pytest.approx(30.0)


def test_percentile_p95():
    values = list(range(1, 101, 1))
    result = percentile([float(v) for v in values], 95)
    assert result == pytest.approx(95.0, abs=1.0)


def test_percentile_single_value():
    assert percentile([42.0], 50) == pytest.approx(42.0)


def test_percentile_with_none():
    result = percentile([None, 10.0, None, 20.0, 30.0], 50)
    assert result == pytest.approx(20.0)


def test_percentile_all_none():
    assert percentile([None, None], 50) is None


def test_percentile_p0_returns_min():
    assert percentile([5.0, 1.0, 3.0], 0) == pytest.approx(1.0)


def test_percentile_p100_returns_max():
    assert percentile([5.0, 1.0, 3.0], 100) == pytest.approx(5.0)
