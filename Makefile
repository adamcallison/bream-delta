unit_test_verbose:
	uv run pytest -vv

unit_test_coverage:
	uv run coverage run -m pytest
	uv run coverage html

unit_test_coverage_display:
	make unit_test_coverage
	xdg-open htmlcov/index.html
