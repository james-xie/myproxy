.PHONY: test

test:
	./gradlew test

release-major:
	SKIP=test,format bumpversion major --config-file version/release.cfg
	git push --tags

release-minor:
	SKIP=test,format bumpversion minor --config-file version/release.cfg
	git push --tags

release-patch:
	SKIP=test,format bumpversion patch --config-file version/release.cfg
	git push --tags

