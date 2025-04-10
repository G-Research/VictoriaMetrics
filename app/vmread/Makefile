# All these commands must run from repository root.

run-vmread:
	mkdir -p vmread-data
	DOCKER_OPTS='-v $(shell pwd)/vmstorage-data:/vmstorage-data' \
	APP_NAME=vmread \
	$(MAKE) run-via-docker

vmread:
	APP_NAME=vmread $(MAKE) app-local

vmread-race:
	APP_NAME=vmread RACE=-race $(MAKE) app-local

vmread-prod:
	APP_NAME=vmread $(MAKE) app-via-docker

vmread-pure-prod:
	APP_NAME=vmread $(MAKE) app-via-docker-pure

vmread-linux-amd64-prod:
	APP_NAME=vmread $(MAKE) app-via-docker-linux-amd64

vmread-linux-arm-prod:
	APP_NAME=vmread $(MAKE) app-via-docker-linux-arm

vmread-linux-arm64-prod:
	APP_NAME=vmread $(MAKE) app-via-docker-linux-arm64

vmread-linux-ppc64le-prod:
	APP_NAME=vmread $(MAKE) app-via-docker-linux-ppc64le

vmread-linux-386-prod:
	APP_NAME=vmread $(MAKE) app-via-docker-linux-386

vmread-freebsd-amd64-prod:
	APP_NAME=vmread $(MAKE) app-via-docker-freebsd-amd64

vmread-openbsd-amd64-prod:
	APP_NAME=vmread $(MAKE) app-via-docker-openbsd-amd64

vmread-windows-amd64-prod:
	APP_NAME=vmread $(MAKE) app-via-docker-windows-amd64

vmread-darwin-amd64-prod:
	APP_NAME=vmread $(MAKE) app-via-docker-darwin-amd64

vmread-darwin-arm64-prod:
	APP_NAME=vmread $(MAKE) app-via-docker-darwin-arm64

vmread-prod-race:
	APP_NAME=vmread RACE=-race $(MAKE) app-via-docker

package-vmread:
	APP_NAME=vmread $(MAKE) package-via-docker

package-vmread-race:
	APP_NAME=vmread RACE=-race $(MAKE) package-via-docker

package-vmread-amd64:
	APP_NAME=vmread $(MAKE) package-via-docker-amd64

package-vmread-arm:
	APP_NAME=vmread $(MAKE) package-via-docker-arm

package-vmread-arm64:
	APP_NAME=vmread $(MAKE) package-via-docker-arm64

package-vmread-ppc64le:
	APP_NAME=vmread $(MAKE) package-via-docker-ppc64le

package-vmread-386:
	APP_NAME=vmread $(MAKE) package-via-docker-386

publish-vmread:
	APP_NAME=vmread $(MAKE) publish-via-docker

publish-vmread-race:
	APP_NAME=vmread RACE=-race $(MAKE) publish-via-docker

vmread-linux-amd64:
	APP_NAME=vmread CGO_ENABLED=1 GOOS=linux GOARCH=amd64 $(MAKE) app-local-goos-goarch

vmread-linux-arm:
	APP_NAME=vmread CGO_ENABLED=0 GOOS=linux GOARCH=arm $(MAKE) app-local-goos-goarch

vmread-linux-arm64:
	APP_NAME=vmread CGO_ENABLED=0 GOOS=linux GOARCH=arm64 $(MAKE) app-local-goos-goarch

vmread-linux-ppc64le:
	APP_NAME=vmread CGO_ENABLED=0 GOOS=linux GOARCH=ppc64le $(MAKE) app-local-goos-goarch

vmread-linux-s390x:
	APP_NAME=vmread CGO_ENABLED=0 GOOS=linux GOARCH=s390x $(MAKE) app-local-goos-goarch

vmread-linux-loong64:
	APP_NAME=vmread CGO_ENABLED=0 GOOS=linux GOARCH=loong64 $(MAKE) app-local-goos-goarch

vmread-linux-386:
	APP_NAME=vmread CGO_ENABLED=0 GOOS=linux GOARCH=386 $(MAKE) app-local-goos-goarch

vmread-freebsd-amd64:
	APP_NAME=vmread CGO_ENABLED=0 GOOS=freebsd GOARCH=amd64 $(MAKE) app-local-goos-goarch

vmread-openbsd-amd64:
	APP_NAME=vmread CGO_ENABLED=0 GOOS=openbsd GOARCH=amd64 $(MAKE) app-local-goos-goarch

vmread-windows-amd64:
	GOARCH=amd64 APP_NAME=vmread $(MAKE) app-local-windows-goarch

vmread-darwin-amd64:
	APP_NAME=vmread CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 $(MAKE) app-local-goos-goarch

vmread-darwin-arm64:
	APP_NAME=vmread CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 $(MAKE) app-local-goos-goarch

vmread-pure:
	APP_NAME=vmread $(MAKE) app-local-pure
