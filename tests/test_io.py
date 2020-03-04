from pathlib import Path

from ploomber import io


def test_csv_write(tmp_directory):
    csv = io.CSVIO('file.csv', chunked=False)
    csv.write([[0, 1, 2]], headers=['a', 'b', 'c'])

    assert Path('file.csv').is_file()


def test_csv_write_chunks(tmp_directory):
    csv = io.CSVIO('files', chunked=True)
    csv.write([[0, 1, 2]], headers=['a', 'b', 'c'])
    csv.write([[0, 1, 2]], headers=['a', 'b', 'c'])

    assert Path('files/0.csv').is_file()
    assert Path('files/1.csv').is_file()


def test_parquet_write(tmp_directory):
    parquet = io.ParquetIO('file.parquet', chunked=False)
    parquet.write([[0, 1, 2]], headers=['a', 'b', 'c'])

    assert Path('file.parquet').is_file()


def test_parquet_write_chunks(tmp_directory):
    parquet = io.ParquetIO('files', chunked=True)
    parquet.write([[0, 1, 2]], headers=['a', 'b', 'c'])
    parquet.write([[0, 1, 2]], headers=['a', 'b', 'c'])

    assert Path('files/0.parquet').is_file()
    assert Path('files/1.parquet').is_file()
