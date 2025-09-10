MOD_DIRS := internal/api internal/compression internal/etl internal/extractor internal/job internal/sink internal/transformer internal/worker tests/cluster_test tests/secrets_test tests/sink_test tests/worker_test

.PHONY: update-deps get-deps test all

clean-testcache:
	go clean -testcache

clean-coverage:
	@rm -f cover.out cover.html tmp.out

test: clean-coverage
	@echo "Running tests with coverage for modules: $(MOD_DIRS)"
	@echo "mode: set" > cover.out
	@exit_code=0; \
	for d in $(MOD_DIRS); do \
		echo "\nTesting $$d..."; \
		if ! go test -coverprofile=tmp.out ./$$d; then \
			exit_code=1; \
		fi; \
		tail -n +2 tmp.out >> cover.out; \
	done; \
	rm -f tmp.out; \
	go tool cover -html=cover.out -o cover.html; \
	printf "\nTotal coverage: %s\n\n" "$$(go tool cover -func=cover.out | grep total | awk '{print $$3}')"; \
	exit $$exit_code

all: test