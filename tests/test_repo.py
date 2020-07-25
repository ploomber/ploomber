from ploomber.repo import get_git_info


def test_get_git_info():
    git_info = get_git_info('.')
    assert set(git_info) == {
        'git_summary', 'git_hash', 'git_diff', 'git_timestamp', 'git_branch',
        'git_location'
    }
