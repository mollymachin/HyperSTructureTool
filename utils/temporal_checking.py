import datetime


def _parse_iso_or_none(value: str | None) -> datetime.datetime | None:
    """
    Return datetime from ISO 8601 string, otherwise None.
    If the input is a non-ISO descriptive string (e.g., "start of the wedding"), return None.
    """
    if not value or not isinstance(value, str):
        return None
    try:
        # datetime.fromisoformat supports YYYY-MM-DD and with time; fails on descriptors
        return datetime.datetime.fromisoformat(value)
    except Exception:
        return None

def get_temporal_intervals_from_hyperedge(hyperedge):
    """
    Given a hyperedge dict (frotnend format), extract temporal intervals as list of dicts.
    Returns list of dicts with start_time and end_time, or empty list if none.
    """
    return hyperedge.get('temporal_intervals', [])

def extract_time_range_from_interval(interval):
    """
    Given a temporal interval dict, extract start and end times as datetime objects when ISO.
    If a bound is a descriptive string, return None for that bound (treated as unknown for querying).
    Returns (start_time, end_time) as datetime.datetime or None.
    """
    start = interval.get('start_time')
    end = interval.get('end_time')
    start_dt = _parse_iso_or_none(start)
    end_dt = _parse_iso_or_none(end)
    return start_dt, end_dt

def is_time_within_range(current_time, start_time, end_time):
    """
    Takes times as datetime objects.
    Returns True if current_time (datetime) is within [start_time, end_time] (inclusive).
    If start_time is None, valid until end_time. If end_time is None, valid from start_time onwards.
    """
    if start_time and end_time:
        return start_time <= current_time <= end_time
    elif start_time:
        return current_time >= start_time
    elif end_time:
        return current_time <= end_time
    else:
        return True  # No bounds = always valid

def is_hyperedge_valid_at_time(hyperedge, current_time):
    """
    Given a hyperedge dict (frontend format) and a current time, check if the hyperedge is valid at that time.
    Returns True if the hyperedge is valid at the current time, False otherwise.
    """
    intervals = get_temporal_intervals_from_hyperedge(hyperedge)
    for interval in intervals:
        start, end = extract_time_range_from_interval(interval)
        if is_time_within_range(current_time, start, end):
            return True
    return False

# ---- TESTS ----
def test_temporal_functions():
    # Test get_temporal_intervals_from_hyperedge
    hyperedge = {
        "entities": ['Will', 'Molly', 'cats'],
        "subjects": ["John Smith"],
        "objects": ["Google"],
        "relation_type": "worked_at",
        "temporal_intervals": [
            {"start_time": "2020-01-01T00:00:00", "end_time": "2021-12-31T23:59:59"}
        ]
    }
    intervals = get_temporal_intervals_from_hyperedge(hyperedge)
    assert intervals[0]['start_time'] == '2020-01-01T00:00:00'
    assert intervals[0]['end_time'] == '2021-12-31T23:59:59'
    

    # Test extract_time_range_from_interval and is_time_within_range
    interval = {'start_time': '2023-01-01T00:00:00', 'end_time': '2023-12-31T23:59:59'}
    start, end = extract_time_range_from_interval(interval)
    assert is_time_within_range(datetime.datetime(2023, 6, 1), start, end) == True
    assert is_time_within_range(datetime.datetime(2022, 12, 31), start, end) == False
    assert is_time_within_range(datetime.datetime(2024, 1, 1), start, end) == False
    
    # Check w open-ended intervals
    interval2 = {'start_time': '2023-01-01T00:00:00'}
    start2, end2 = extract_time_range_from_interval(interval2)
    assert is_time_within_range(datetime.datetime(2024, 1, 1), start2, end2) == True
    
    interval3 = {'end_time': '2023-12-31T23:59:59'}
    start3, end3 = extract_time_range_from_interval(interval3)
    assert is_time_within_range(datetime.datetime(2022, 1, 1), start3, end3) == True
    assert is_time_within_range(datetime.datetime(2024, 1, 1), start3, end3) == False
    
    
    print("All temporal tests passed!")

if __name__ == '__main__':
    test_temporal_functions()
