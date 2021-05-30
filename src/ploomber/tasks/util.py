from concurrent.futures import ThreadPoolExecutor, as_completed


def download_products_in_parallel(tasks):
    """Call Task.product.download in parallel
    """
    with ThreadPoolExecutor(max_workers=64) as executor:
        future2task = {executor.submit(t.product.download): t for t in tasks}

        for future in as_completed(future2task):
            exception = future.exception()

            if exception:
                task = future2task[future]
                raise RuntimeError(
                    'An error occurred when downloading product from '
                    f'task: {task!r}') from exception
