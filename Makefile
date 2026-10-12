.PHONY: init test example clean_coverage test_coverage lint distclean distcheck dist install

init36:
	# packages are deprecating support, so this uses exact versions
	pip install -r requirements.txt

init:
	pip install --use-feature=in-tree-build .

unit:
	PYTHONPATH=. py.test --doctest-modules cdx_toolkit tests/unit -v -v

test:
	PYTHONPATH=. py.test --doctest-modules cdx_toolkit tests -v -v

# runs against the live Common Crawl index (not part of `test`: connections from CI runners time out)
example:
	PYTHONPATH=. examples/iter-and-warc.py

clean_coverage:
	rm -f .coverage

test_coverage: clean_coverage
	#	PYTHONPATH=. coverage run -a --source=cdx_toolkit,examples examples/iter-and-warc.py
	# -rA to see all output, pass or fail
	# CDXT_LOGLEVEL=DEBUG
	# -vvvv -s
	PYTHONPATH=. py.test -rA -s --doctest-modules --cov-report=xml --cov-append --cov cdx_toolkit tests -v -v
	coverage report

lint:
	flake8 cdx_toolkit tests

distclean:
	rm -rf dist/

distcheck: distclean
	python ./setup.py sdist
	twine check dist/*

dist: distclean
	echo "  Finishe CHANGELOG.md and commit it."
	echo "  git tag --list"
	echo "  git tag 0.x.x  # no v"
	echo "  git push --tags"
	python ./setup.py sdist
	twine check dist/*
	twine upload dist/* -r pypi

install:
	pip install -e ".[dev,test]"
	pre-commit install
