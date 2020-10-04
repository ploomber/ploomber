"""
Tests for the abstract class
"""
from ploomber.products import Product


class FakeProduct(Product):
    """Fakes a Product concrete class by implementing abstract methods
    """
    def _init_identifier(self, identifier):
        return identifier

    def fetch_metadata(self):
        pass

    def save_metadata(self, source_code):
        pass

    def delete(self):
        pass

    def exists(self):
        pass


def test_save_metadata_resets_cached_status():
    """
    WHen saving metadata, we must reset cached status because they are outdated
    when the metadata changes
    """
    p = FakeProduct(True)

    p._outdated_data_dependencies_status = True
    p._outdated_code_dependency_status = True

    p._save_metadata('some new code')

    assert p._outdated_data_dependencies_status is None
    assert p._outdated_code_dependency_status is None
