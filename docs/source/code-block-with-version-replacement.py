from docutils.parsers.rst import Directive
from sphinx.directives import CodeBlock


def setup(app):
    app.add_directive('code-block-with-version-replacement',
                      CodeBlockWithVersion)
    return {'version': '0.1'}


class CodeBlockWithVersion(CodeBlock):
    """ This directive exactly Sphinx's CodeBlock directive, except that
    we do an initial transformation of the content, replacing |version| with
    the version in conf.py.
    """

    def _get_version_from_conf_py(self):
        env = self.state.document.settings.env
        config = env.config
        return config['version']

    def _transformed_content(self):
        version = self._get_version_from_conf_py()
        return map(lambda x: x.replace('|version|', version),
                   self.content)

    def run(self):
        self.content = self._transformed_content()
        return super(CodeBlockWithVersion, self).run()
