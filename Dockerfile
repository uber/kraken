## -*- docker-image-name: "uber-usi/kraken" -*-
FROM 192.168.65.1:15055/uber-uai/go-1.7:sjc1-produ-0000000008

LABEL com.uber.base-image-name="uber-uai/go-1.7:sjc1-produ-0000000008" com.uber.supported_app_id="kraken"

### registry, peer, annonuce, redis
EXPOSE 5051 5081 8001 6379

RUN /ucontainer/prepare.sh && /ucontainer/fix-git.sh
### TODO: temp fix to make langley location same as what is on the box. need to remove this line after udeploy fix mount
RUN mkdir -p /langley/udocker/ && mkdir -p /langley/current/ && ln -sf /langley/udocker/kraken/current  /langley/current/kraken

### Make service sercrets available to the build
RUN mkdir -p /langley/udocker/kraken && ln -sf /ucontainer/service-secrets /langley/udocker/kraken/current

### Environment variables
ENV BASE_PATH="/home/udocker/kraken" BUILD_TYPE="development" FUNC_PATH="/ucontainer/functions.sh" GIT_URI="" IS_CLEAN="true" NODE_ENV="development-local-container" PYTHON_PACKAGE_CONFLICTS_OUT="/ucontainer/_python_package_conflicts.out" PYTHON_PACKAGE_CONFLICTS_SCRIPT="/usr/local/bin/python_package_check.py" SETUPTOOLS_SYS_PATH_TECHNIQUE="rewrite" SVC_ID="kraken" UBER_ENVIRONMENT="development-local-container" UBER_PORT_HTTP="5051" USE_PIP7="1" USE_UWSGI="1"

### Extra Debian packages
RUN /ucontainer/apt-get-install.sh \
    sudo

### Add Repo
ADD git-repo /home/udocker/kraken
WORKDIR /home/udocker/kraken

### Environment variables that might invalidate build cache
ENV GIT_DESCRIBE="" GIT_REF=""

### Configure
# TODO This is expensive in the CoW FS - it will store all files touched in the new layer!
RUN chown -R udocker:udocker /home/udocker/kraken

RUN mkdir -p /var/log/udocker/kraken && chown -R udocker:udocker /var/log/udocker/kraken && chmod 0755 /var/log/udocker/kraken && \
    mkdir -p /etc/udocker && chown -R udocker:udocker /etc/udocker && chmod 0755 /etc/udocker && \
    mkdir -p /var/run/udocker && chown -R udocker:udocker /var/run/udocker && chmod 0755 /var/run/udocker

### Remove service secrets directory and link langley directory in its place
RUN rm -rf /home/udocker/kraken/config/secrets.yaml \
    && mkdir -p $(dirname /home/udocker/kraken/config/secrets.yaml) \
    && ln -sf /langley/current/kraken/service.kraken.yaml /home/udocker/kraken/config/secrets.yaml

### Build
RUN /ucontainer/build_go_target.sh && /ucontainer/clean_docker_go_target.sh

### Cleanup
RUN /ucontainer/cleanup.sh

USER udocker

### Metadata
ENTRYPOINT ["/bin/bash", "-c", "./start-origin.sh"]
