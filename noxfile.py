import nox

TEST_DEPENDENCIES = ["pytest", "mock"]

@nox.session(python=['3.7', '3.8'])
def tests(session):
    session.install(".")
    session.install(*TEST_DEPENDENCIES)
    session.run('pytest')
