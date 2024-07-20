####################################################################################################################
# Setup containers to run Spark

build:
	docker compose up --build -d
down:
	docker compose down
####################################################################################################################
# Testing, auto formatting, type checks, & Lint checks

pytest:
	docker exec spark_master bash -c 'python3 -m pytest --log-cli-level info -p no:warnings -v /Tests'

format:
	docker exec spark_master black -S --line-length 79 --preview .
	docker exec spark_master isort .

type:
	docker exec spark_master mypy --no-implicit-reexport --ignore-missing-imports --no-namespace-packages .

lint:
	docker exec spark_master flake8 ./Code_ETL

ci: format type pytest
