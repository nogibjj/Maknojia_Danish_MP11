install:
	pip install --upgrade pip && pip install -r requirement.txt

format:
	black *.py

lint:
	pylint --disable=R,C --ignore-patterns=test_.*?py *.py

test:
	python -m pytest -cov test.py

all: install format lint test