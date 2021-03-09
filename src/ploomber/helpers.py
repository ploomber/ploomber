from pathlib import Path

from jinja2 import Template

from ploomber.tasks import PythonCallable
from ploomber.tasks.tasks import _Gather, _Partition
from ploomber.products import File


class PartitionedFile(File):
    @property
    def _path_to_stored_source_code(self):
        # point to the parent folder
        name = '.' + self._path_to_file.name.parent
        return Path(str(name) + '.metadata')

    def save_metadata(self):
        # do not save metadata
        pass

    # I think i have to remove this
    def _outdated_code_dependency(self):
        return False


def partitioned_execution(upstream_partitioned,
                          downstream_callable,
                          downstream_prefix,
                          downstream_path,
                          partition_ids,
                          partition_template='partition={{id}}',
                          upstream_other=None):
    # make sure output is a File
    assert isinstance(upstream_partitioned.product, File)

    dag = upstream_partitioned.dag
    partition_template_w_suffix = Template(downstream_prefix + '_' +
                                           partition_template)
    partition_template_t = Template(partition_template)

    # instantiate null tasks
    nulls = [
        _Partition(product=PartitionedFile(
            Path(str(upstream_partitioned.product),
                 partition_template_t.render(id=id_))),
                   dag=dag,
                   name=Template('null_' + partition_template).render(id=id_))
        for id_ in partition_ids
    ]

    def make_file(id_):
        f = PartitionedFile(
            Path(downstream_path, partition_template_t.render(id=id_)))
        return f

    # TODO: validate downstream product is File
    tasks = [
        PythonCallable(downstream_callable,
                       product=make_file(id_),
                       dag=dag,
                       name=(partition_template_w_suffix.render(id=id_)),
                       params={
                           'upstream_key':
                           (Template('null_' +
                                     partition_template).render(id=id_))
                       }) for id_ in partition_ids
    ]

    gather = _Gather(source=downstream_callable,
                     product=File(downstream_path),
                     dag=dag,
                     name=downstream_prefix)

    # TODO: "fuse" operator that merges task chains and shows it like a single
    # task - maybe this should be like this and the dag.plot function take
    # care of simplifications for viz purposes?
    for null, task in zip(nulls, tasks):
        upstream_partitioned >> null >> task >> gather

        if upstream_other:
            upstream_other >> task

    return gather
