####################################################################################################################
# Setup containers to run Spark

build:
	docker compose up --build -d
down:
	docker compose down
####################################################################################################################
# Testing, auto formatting, type checks, & Lint checks

pytest:
	docker exec spark bash -c 'python3 -m pytest --log-cli-level info -p no:warnings -v ./Tests'

format:
	docker exec spark black -S --line-length 79 --preview .
	docker exec spark isort .

type:
	docker exec spark mypy --no-implicit-reexport --ignore-missing-imports --no-namespace-packages .

lint:
	docker exec spark flake8 .

ci: format type lint pytest
