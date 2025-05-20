default: build

.venv: requirements.txt requirements_dev.txt
	python3 -m venv .venv
	. .venv/bin/activate
	pip3 install -U -r requirements.txt -r requirements_dev.txt
	@rm -f .venv/.updated
	@touch .venv/.updated

build: clean .venv
	. .venv/bin/activate
	@python3 setup.py clean
	@python3 setup.py bdist_wheel

clean:
	@rm -rf build dist

test: .venv
	. .venv/bin/activate
	pytest -s tests

coverage: .venv
	. .venv/bin/activate
	coverage run -m pytest -s tests
	coverage report -m

.PHONY: build
