import nox

TEST_DEPENDENCIES = ["pytest", "mock"]

@nox.session(python=['3.12'])
def tests(session):
    session.install(".")
    session.install(*TEST_DEPENDENCIES)
    session.run('pytest')
