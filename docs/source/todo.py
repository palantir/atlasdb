from docutils.parsers.rst import Directive
from sphinx.directives import CodeBlock


def setup(app):
    app.add_directive('code-block-with-version-replacement',
                      CodeBlockWithVersion)
    return {'version': '0.1'}


class CodeBlockWithVersion(Directive):
    """ This directive is a copy of Sphinx's CodeBlock directive, except that
    we do an initial transformation of the content,
    Does a string replacement on the given content, replacing
        |version| with the version in conf.py, and then passes through to
        CodeBlock.run
    """

    has_content = CodeBlock.has_content
    required_arguments = CodeBlock.required_arguments
    optional_arguments = CodeBlock.optional_arguments
    final_argument_whitespace = CodeBlock.final_argument_whitespace
    option_spec = CodeBlock.option_spec

    def __init__(self, *args, **kwargs):
        self.code_block = CodeBlock(*args, **kwargs)
        super(CodeBlockWithVersion, self).__init__(*args, **kwargs)

    def _get_version_from_conf_py(self):
        env = self.state.document.settings.env
        config = env.config
        return config['version']

    def _transformed_content(self):
        version = self._get_version_from_conf_py()
        return map(lambda x: x.replace('|version|', version),
                   self.content)

    def run(self):
        transformed_content = self._transformed_content()
        self.code_block.content = transformed_content
        return self.code_block.run()
