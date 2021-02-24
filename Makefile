include dev.env
export

install_deps:
	pip install -r dev_requirements.txt
	pip install -r requirements.txt
	pip install -e .

run_tests:
	pytest test/integration/athena.dbtspec