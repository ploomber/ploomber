from pathlib import Path

from ploomber import DAG
from ploomber.tasks import ShellScript, PythonCallable
from ploomber.products import File


def touch_root(product):
    Path(str(product)).touch()


def touch(product, upstream):
    Path(str(product)).touch()


def test_non_existent_file(tmp_directory):
    dag = DAG()
    f = File("file.txt")
    ta = ShellScript("echo hi > {{product}}", f, dag, "ta")
    ta.render()

    assert not f.exists()
    assert f._is_outdated()
    assert f._outdated_code_dependency()
    assert not f._outdated_data_dependencies()


def test_outdated_data_simple_dependency(tmp_directory):
    """A -> B"""
    dag = DAG()

    fa = Path("a.txt")
    fb = Path("b.txt")

    ta = PythonCallable(touch_root, File(fa), dag, "ta")
    tb = PythonCallable(touch, File(fb), dag, "tb")

    ta >> tb

    ta.render()
    tb.render()

    assert not ta.product.exists()
    assert not tb.product.exists()
    assert ta.product._is_outdated()
    assert tb.product._is_outdated()

    dag.build()

    dag._clear_metadata()

    # they both exist now
    assert ta.product.exists()
    assert tb.product.exists()

    # and arent outdated...
    assert not ta.product._is_outdated()
    assert not tb.product._is_outdated()

    # let's make b outdated
    ta.build(force=True)

    dag._clear_metadata()

    assert not ta.product._is_outdated()
    assert tb.product._is_outdated()


def test_many_upstream(tmp_directory):
    """{A, B} -> C"""
    dag = DAG()

    fa = Path("a.txt")
    fb = Path("b.txt")
    fc = Path("c.txt")

    ta = PythonCallable(touch_root, File(fa), dag, "ta")
    tb = PythonCallable(touch_root, File(fb), dag, "tb")
    tc = PythonCallable(touch, File(fc), dag, "tc")

    (ta + tb) >> tc

    dag.build()

    assert ta.product.exists()
    assert tb.product.exists()
    assert tc.product.exists()

    assert not ta.product._is_outdated()
    assert not tb.product._is_outdated()
    assert not tc.product._is_outdated()

    ta.build(force=True)
    dag._clear_metadata()

    assert not ta.product._is_outdated()
    assert not tb.product._is_outdated()
    assert tc.product._is_outdated()

    dag.build()
    tb.build(force=True)
    dag._clear_metadata()

    assert not ta.product._is_outdated()
    assert not tb.product._is_outdated()
    assert tc.product._is_outdated()


def test_many_downstream():
    """A -> {B, C}"""
    pass


def test_chained_dependency():
    """A -> B -> C"""
    pass


def test_can_instantiate_task_with_many_products():
    dag = DAG()
    fa1 = File("a1.txt")
    fa2 = File("a2.txt")
    ta = ShellScript("echo {{product}}", [fa1, fa2], dag, "ta")
    ta.render()

    assert not ta.product.exists()
    assert ta.product._is_outdated()
    assert ta.product._outdated_code_dependency()
    assert not ta.product._outdated_data_dependencies()


def test_overloaded_operators():
    dag = DAG()

    fa = Path("a.txt")
    fb = Path("b.txt")
    fc = Path("c.txt")

    ta = ShellScript("touch {{product}}", File(fa), dag, "ta")
    tb = ShellScript("touch {{product}}", File(fb), dag, "tb")
    tc = ShellScript("touch {{product}}", File(fc), dag, "tc")

    ta >> tb >> tc

    assert not ta.upstream
    assert tb in tc.upstream.values()
    assert ta in tb.upstream.values()


def test_adding_tasks():
    dag = DAG()

    fa = Path("a.txt")
    fb = Path("b.txt")
    fc = Path("c.txt")

    ta = ShellScript("touch {{product}}", File(fa), dag, "ta")
    tb = ShellScript("touch {{product}}", File(fb), dag, "tb")
    tc = ShellScript("touch {{product}}", File(fc), dag, "tc")

    assert list((ta + tb).tasks) == [ta, tb]
    assert list((tb + ta).tasks) == [tb, ta]
    assert list((ta + tb + tc).tasks) == [ta, tb, tc]
    assert list(((ta + tb) + tc).tasks) == [ta, tb, tc]
    assert list((ta + (tb + tc)).tasks) == [ta, tb, tc]


def test_adding_tasks_left():
    dag = DAG()

    fa = Path("a.txt")
    fb = Path("b.txt")
    fc = Path("c.txt")

    ta = ShellScript("touch {{product}}", File(fa), dag, "ta")
    tb = ShellScript("touch {{product}}", File(fb), dag, "tb")
    tc = ShellScript("touch {{product}}", File(fc), dag, "tc")

    (ta + tb) >> tc

    assert not ta.upstream
    assert not tb.upstream
    assert set(tc.upstream.values()) == {ta, tb}


def test_adding_tasks_right():
    dag = DAG()

    fa = Path("a.txt")
    fb = Path("b.txt")
    fc = Path("c.txt")

    ta = ShellScript("touch {{product}}", File(fa), dag, "ta")
    tb = ShellScript("touch {{product}}", File(fb), dag, "tb")
    tc = ShellScript("touch {{product}}", File(fc), dag, "tc")

    ta >> (tb + tc)

    assert not ta.upstream
    assert list(tb.upstream.values()) == [ta]
    assert list(tc.upstream.values()) == [ta]
