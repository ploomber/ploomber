from docutils import nodes
from docutils.statemachine import ViewList
from sphinx_tabs.tabs import TabsDirective, GroupTabDirective


class PloomberExampleCliDirective(GroupTabDirective):

    has_content = True

    def run(self):

        self.assert_has_content()

        new_content = ViewList([
            'CLI',
            '',
        ])

        new_content.append(self.content)

        self.content = new_content

        node = super().run()

        return node


class PloomberExamplePythonDirective(GroupTabDirective):

    has_content = True

    def run(self):

        self.assert_has_content()

        # ploomber example config
        # found with python -m sphinx.ext.intersphinx _build/html/objects.inv
        ploomber_example_prepare_info_path = '../user-guide'\
            '/using-python-api.html'

        ref_node = nodes.reference(
            '',
            '',
            internal=True,
            # refname=app.config["ploomber_example_prepare_info_path"])
            refuri=ploomber_example_prepare_info_path)

        inner_node = nodes.emphasis("here.", "here.")

        ref_node.append(inner_node)

        note_node = nodes.note()

        warning_text_node = nodes.paragraph(
            text='Unlike the command line interface, the Ploomber Python '
            'API requires preparation before each command. Find more '
            'information ')

        warning_text_node += ref_node

        note_node.append(warning_text_node)

        new_content = ViewList([
            'Python',
            '',
        ])

        new_content.append(self.content)

        self.content = new_content

        tab_node = super().run()[0]

        # prepend
        tab_node.insert(0, note_node)

        return [tab_node]


def setup(app):
    app.add_config_value('ploomber_example_prepare_info_path',
                         'unconfigured prepare guide path', 'html')

    app.add_directive("ploomber-example", TabsDirective)
    app.add_directive("example-python", PloomberExamplePythonDirective)
    app.add_directive("example-cli", PloomberExampleCliDirective)

    return {
        'version': '0.1',
        'parallel_read_safe': True,
        'parallel_write_safe': True,
    }
