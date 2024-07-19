####################################################################################################################
# Setup containers to run Spark

build:
	docker compose up --build -d
down:
	docker compose down
####################################################################################################################
# Testing, auto formatting, type checks, & Lint checks

pytest:
	docker exec spark-master bash -c 'python3 -m pytest --log-cli-level info -p no:warnings -v ./Tests'

format:
	docker exec spark-master black -S --line-length 79 --preview .
	docker exec spark-master isort .

type:
	docker exec spark-master mypy --no-implicit-reexport --ignore-missing-imports --no-namespace-packages .

lint:
	docker exec spark-master flake8 .

ci: format type lint pytest
