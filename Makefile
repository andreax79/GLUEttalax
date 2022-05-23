
help:
	@echo - make clean
	@echo - make tests
	@echo - make release
	@echo - make venv

release:
	rm -rf dist
	python3 setup.py sdist bdist_wheel
	python3 setup.py bdist_wheel
	twine upload -r pypi dist/*

clean:
	-rm -rf build dist
	-rm -rf *.egg-info
	-rm -rf bin lib share

tests:
	python3 setup.py test

venv:
	python3 -m virtualenv .
	. bin/activate; pip install -Ur requirements.txt
	. bin/activate; pip install -Ur requirements-dev.txt
