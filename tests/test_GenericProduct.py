from ploomber.products import GenericProduct


def test_exists_sqlite_backend(sqlite_client_and_tmp_dir):
    client, tmp_dir = sqlite_client_and_tmp_dir
    product = GenericProduct('some_identifier', client=client)
    assert not product.exists()


def test_save_metadata_sqlite_backend(sqlite_client_and_tmp_dir):
    client, tmp_dir = sqlite_client_and_tmp_dir
    product = GenericProduct('some_identifier', client=client)
    m = {'metadata': 'value'}
    product.save_metadata(m)

    assert product.exists()
    assert product.fetch_metadata() == m
