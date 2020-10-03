NAME=udacity-postgres-student

default: image

.built: Dockerfile
	docker build -t ${NAME} .
	touch .built

lint:
	docker run --rm -i hadolint/hadolint < Dockerfile

image: .built

clean:
	rm -rf .built

.PHONY: default image clean