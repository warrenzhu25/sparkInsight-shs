import gzip
import shutil
from pathlib import Path

from spark_insight.core.cache import CacheManager


def test_cache_parses_gzip_eventlog(tmp_path):
    source = Path("examples/eventlogs/local-0001.json")
    gz_path = tmp_path / "local-0001.json.gz"
    with source.open("rb") as input_file, gzip.open(gz_path, "wb") as output_file:
        shutil.copyfileobj(input_file, output_file)

    cache = CacheManager(tmp_path / "cache")
    parsed = cache.get_or_parse(gz_path)

    assert parsed.app_info.id == "local-0001"
    assert cache.is_cached(gz_path)
    assert cache.load_cached(gz_path).jobs[0].status == "SUCCEEDED"
