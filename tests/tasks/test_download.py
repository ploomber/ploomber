from ploomber import DAG
from ploomber.products import File
from ploomber.tasks import DownloadFromURL


def test_can_download_file(tmp_directory):
    dag = DAG()

    url = """
    https://google.com
    """
    DownloadFromURL(url, File("file"), dag=dag, name="download")

    assert dag.build()
