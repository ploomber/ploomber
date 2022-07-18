import sys
import time
import asyncio
import typing as t
from queue import Empty

from nbclient.exceptions import CellExecutionError, CellTimeoutError
from papermill.clientwrap import PapermillNotebookClient
from nbclient import NotebookClient
from nbclient.util import ensure_async, run_hook, run_sync
from nbclient.exceptions import CellControlSignal, DeadKernelError
from nbformat import NotebookNode
from jupyter_client.client import KernelClient


class PloomberNotebookClient(NotebookClient):
    """
    A subclass of nbclient.NotebookClient with stdin support
    """

    async def async_start_new_kernel_client(self) -> KernelClient:
        """Creates a new kernel client.

        Returns
        -------
        kc : KernelClient
            Kernel client as created by the kernel manager ``km``.
        """
        assert self.km is not None
        try:
            self.kc = self.km.client()
            await ensure_async(self.kc.start_channels()
                               )  # type:ignore[func-returns-value]
            await ensure_async(
                self.kc.wait_for_ready(timeout=self.startup_timeout)
            )  # type:ignore
        except Exception as e:
            self.log.error("Error occurred while starting new kernel client "
                           "for kernel {}: {}".format(self.km.kernel_id,
                                                      str(e)))
            await self._async_cleanup_kernel()
            raise

        # allow stdin
        self.kc.allow_stdin = True

        await run_hook(self.on_notebook_start, notebook=self.nb)
        return self.kc

    start_new_kernel_client = run_sync(async_start_new_kernel_client)

    async def _async_poll_stdin_msg(self, parent_msg_id: str,
                                    cell: NotebookNode,
                                    cell_index: int) -> None:
        assert self.kc is not None

        # why does it break if I remove this?
        time.sleep(0.5)

        # looks like this sometimes returns false
        msg_received = await self.kc.stdin_channel.msg_ready()

        if msg_received:
            while True:
                try:
                    msg = await ensure_async(
                        self.kc.stdin_channel.get_msg(timeout=0.1))
                except Empty:
                    break
                else:
                    # display all pending messages (e.g. traceback)
                    flush_io(self.kc)

                    self.kc.input(input(msg['content']['prompt']))

                    try:
                        msg = await self.kc.get_iopub_msg(timeout=1)
                    except Empty:
                        pass
                    else:
                        if msg.get('content', {}).get('text'):
                            print(msg['content']['text'])

    async def async_execute_cell(
        self,
        cell: NotebookNode,
        cell_index: int,
        execution_count: t.Optional[int] = None,
        store_history: bool = True,
    ) -> NotebookNode:
        """
        Executes a single code cell.

        To execute all cells see :meth:`execute`.

        Parameters
        ----------
        cell : nbformat.NotebookNode
            The cell which is currently being processed.
        cell_index : int
            The position of the cell within the notebook object.
        execution_count : int
            The execution count to be assigned to the cell (default: Use
            kernel response)
        store_history : bool
            Determines if history should be stored in the kernel
            (default: False). Specific to ipython kernels, which can store
            command histories.

        Returns
        -------
        output : dict
            The execution output payload (or None for no output).

        Raises
        ------
        CellExecutionError
            If execution failed and should raise an exception, this will be
            raised with defaults about the failure.

        Returns
        -------
        cell : NotebookNode
            The cell which was just processed.
        """
        assert self.kc is not None

        await run_hook(self.on_cell_start, cell=cell, cell_index=cell_index)

        if cell.cell_type != 'code' or not cell.source.strip():
            self.log.debug("Skipping non-executing cell %s", cell_index)
            return cell

        if self.skip_cells_with_tag in cell.metadata.get("tags", []):
            self.log.debug("Skipping tagged cell %s", cell_index)
            return cell

        if self.record_timing:  # clear execution metadata prior to execution
            cell['metadata']['execution'] = {}

        self.log.debug("Executing cell:\n%s", cell.source)

        cell_allows_errors = (not self.force_raise_errors) and (
            self.allow_errors
            or "raises-exception" in cell.metadata.get("tags", []))

        await run_hook(self.on_cell_execute, cell=cell, cell_index=cell_index)

        # execute cell
        parent_msg_id = await ensure_async(
            self.kc.execute(cell.source,
                            store_history=store_history,
                            stop_on_error=not cell_allows_errors))

        await run_hook(self.on_cell_complete, cell=cell, cell_index=cell_index)

        # check for input
        await self._async_poll_stdin_msg(parent_msg_id, cell, cell_index)

        # We launched a code cell to execute
        self.code_cells_executed += 1
        exec_timeout = self._get_timeout(cell)

        cell.outputs = []
        self.clear_before_next_output = False

        task_poll_kernel_alive = asyncio.ensure_future(
            self._async_poll_kernel_alive())

        task_poll_output_msg = asyncio.ensure_future(
            self._async_poll_output_msg(parent_msg_id, cell, cell_index))
        self.task_poll_for_reply = asyncio.ensure_future(
            self._async_poll_for_reply(parent_msg_id, cell, exec_timeout,
                                       task_poll_output_msg,
                                       task_poll_kernel_alive))

        try:
            exec_reply = await self.task_poll_for_reply
        except asyncio.CancelledError:
            # can only be cancelled by task_poll_kernel_alive when the kernel
            # is dead
            task_poll_output_msg.cancel()
            raise DeadKernelError("Kernel died")
        except Exception as e:
            # Best effort to cancel request if it hasn't been resolved
            try:
                # Check if the task_poll_output is doing the raising for us
                if not isinstance(e, CellControlSignal):
                    task_poll_output_msg.cancel()
            finally:
                raise

        if execution_count:
            cell['execution_count'] = execution_count
        await run_hook(self.on_cell_executed,
                       cell=cell,
                       cell_index=cell_index,
                       execute_reply=exec_reply)

        await self._check_raise_for_error(cell, cell_index, exec_reply)

        self.nb['cells'][cell_index] = cell

        return cell

    execute_cell = run_sync(async_execute_cell)


# NOTE: this still needs some work, I adapted it from jupyter-console, but
# there are cases that I'm unsure if they are handled properly (e.g. rich
# data display)
def flush_io(client):
    """Flush messages from the iopub channel

    Notes
    -----
    Adapted from:
    https://github.com/jupyter/jupyter_console/blob/bcf17a3953844d75262e3ce23b784832d9044877/jupyter_console/ptshell.py#L846 # noqa
    """
    while run_sync(client.iopub_channel.msg_ready)():
        sub_msg = run_sync(client.iopub_channel.get_msg)()
        msg_type = sub_msg['header']['msg_type']

        _pending_clearoutput = True

        # do we need to handle this?
        if msg_type == 'status':
            pass
        elif msg_type == 'stream':
            if sub_msg["content"]["name"] == "stdout":
                if _pending_clearoutput:
                    print("\r", end="")
                    _pending_clearoutput = False
                print(sub_msg["content"]["text"], end="")
                sys.stdout.flush()
            elif sub_msg["content"]["name"] == "stderr":
                if _pending_clearoutput:
                    print("\r", file=sys.stderr, end="")
                    _pending_clearoutput = False
                print(sub_msg["content"]["text"], file=sys.stderr, end="")
                sys.stderr.flush()

        elif msg_type == 'execute_result':
            if _pending_clearoutput:
                print("\r", end="")
                _pending_clearoutput = False

            format_dict = sub_msg["content"]["data"]

            if 'text/plain' not in format_dict:
                continue

            # prompt_toolkit writes the prompt at a slightly lower level,
            # so flush streams first to ensure correct ordering.
            sys.stdout.flush()
            sys.stderr.flush()

            text_repr = format_dict['text/plain']
            if '\n' in text_repr:
                # For multi-line results, start a new line after prompt
                print()
            print(text_repr)

        elif msg_type == 'display_data':
            pass

        # If execute input: print it
        elif msg_type == 'execute_input':
            content = sub_msg['content']

            # New line
            sys.stdout.write('\n')
            sys.stdout.flush()

            # With `Remote In [3]: `
            # self.print_remote_prompt(ec=ec)

            # And the code
            sys.stdout.write(content['code'] + '\n')

        elif msg_type == 'clear_output':
            if sub_msg["content"]["wait"]:
                _pending_clearoutput = True
            else:
                print("\r", end="")

        elif msg_type == 'error':
            for frame in sub_msg["content"]["traceback"]:
                print(frame, file=sys.stderr)


class PapermillPloomberNotebookClient(PapermillNotebookClient,
                                      PloomberNotebookClient):
    """A papermill client that uses our custom notebook client
    """

    # NOTE: adapted from papermill's source code
    def papermill_execute_cells(self):
        """
        This function replaces cell execution with it's own wrapper.
        We are doing this for the following reasons:
        1. Notebooks will stop executing when they encounter a failure but not
           raise a `CellException`. This allows us to save the notebook with
           the traceback even though a `CellExecutionError` was encountered.
        2. We want to write the notebook as cells are executed. We inject our
           logic for that here.
        3. We want to include timing and execution status information with the
           metadata of each cell.
        """
        # Execute each cell and update the output in real time.
        for index, cell in enumerate(self.nb.cells):
            try:
                self.nb_man.cell_start(cell, index)
                self.execute_cell(cell, index)
            except CellExecutionError as ex:
                self.nb_man.cell_exception(self.nb.cells[index],
                                           cell_index=index,
                                           exception=ex)
                break
            except CellTimeoutError as e:
                # this will exit execution gracefully upon debugging, but
                # it will also cause the notebook to abort execution after
                # an "input" cell
                print(e)
                sys.exit(1)
            finally:
                self.nb_man.cell_complete(self.nb.cells[index],
                                          cell_index=index)
