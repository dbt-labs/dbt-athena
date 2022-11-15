include dev.env
export

check:
	pre-commit run

install_deps:
	pip install -r dev-requirements.txt
	pip install -r requirements.txt
	pip install -e .

run_tests:
	pytest test/integration/athena.dbtspec
