from slurm_analytics.sacctmgr import add_hierarchy


def test_add_hierarchy_uses_parent_chain_and_path():
    rows = [
        ("dept", None, None),
        ("pi", "dept", None),
        ("project", "pi", None),
    ]
    assert add_hierarchy(rows) == [
        ("dept", None, None, 0, "dept"),
        ("pi", "dept", None, 1, "dept/pi"),
        ("project", "pi", None, 2, "dept/pi/project"),
    ]


def test_parse_association_tree_parsable_output():
    from slurm_analytics.sacctmgr import _parse_association_tree

    output = '''tkurc-group|asinhasa||\ntkurc-group|jsteier||\ntma-group||clinwulf|\n  yazdi-group||clinwulf|\n'''
    assert _parse_association_tree(output) == [
        ("tkurc-group", "asinhasa", None),
        ("tkurc-group", "jsteier", None),
        ("tma-group", None, "clinwulf"),
        ("yazdi-group", None, "clinwulf"),
    ]


def test_parse_association_tree_does_not_leak_adjacent_columns():
    from slurm_analytics.sacctmgr import _parse_association_tree

    output = "akumar-group|c|parent-group|\n"
    assert _parse_association_tree(output) == [
        ("akumar-group", "c", "parent-group"),
    ]
