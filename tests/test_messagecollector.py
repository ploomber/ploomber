import ploomber.messagecollector as mc
from ploomber.products import File
from ploomber.tasks import DownloadFromURL
from ploomber import DAG


def test_task_build_exception_works_for_valid_args():
    dag = DAG()

    url = """
    https://google.com
    """
    task = DownloadFromURL(url, File('file'), dag=dag, name='download')

    exceptionText = mc.task_build_exception(task=task,
                                            message="hello",
                                            exception=Exception("hello)"))

    expected = """
============================== Task build failed ===============================
------------------ DownloadFromURL: download -> File('file') -------------------
hello
============================== Task build failed ===============================
"""  # noqa: E501

    assert exceptionText == expected
