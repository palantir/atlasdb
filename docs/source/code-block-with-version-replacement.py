from sphinx.directives.code import CodeBlock


def setup(app):
    app.add_directive('code-block-with-version-replacement',
                      CodeBlockWithVersion)
    app.add_config_value('latest', '0.0.0', 'env')
    return {'version': '0.1'}


class CodeBlockWithVersion(CodeBlock):
    """ Exactly Sphinx's CodeBlock directive, except that we do an initial
    transformation of the content, replacing |version| with the version in
    conf.py.
    """

    def _get_version_from_conf_py(self):
        env = self.state.document.settings.env
        config = env.config
        return config['latest']

    def _transformed_content(self):
        version = self._get_version_from_conf_py()
        return map(lambda x: x.replace('|latest|', version),
                   self.content)

    def run(self):
        self.content = self._transformed_content()
        return super(CodeBlockWithVersion, self).run()
